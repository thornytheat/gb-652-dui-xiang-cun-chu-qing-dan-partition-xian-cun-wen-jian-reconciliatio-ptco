"""分区合并服务：封账归属保留、未封账转账、撤销退回与读模型。

关键保证：
- 合并先做乐观版本校验，过期版本直接拒绝，失败保留旧版（不写任何数据）。
- 逐条转账各带独立事务与幂等键，中断后同 merge_id 重试只续做未完成项，不重复转账。
- 封账项保留原小时归属，仅登记流水证据；撤销时同样保留。
- 后补对象登记到自身分区，沿有效归属存在，不被历史合并领取。
- 完成时校验总量守恒；不守恒则标记 FAILED 并保留旧版。

连接纪律：不在一个长事务里做逐项 commit；每个工作单元使用短连接，
读取永远拿到已落盘的最新状态，避免 SQLite 同连接快照过期。
"""
from __future__ import annotations

import json

from app.errors import DomainError
from . import core
from .store import MergeStore


class MergeService:
    def __init__(self, store: MergeStore):
        self.store = store

    # ---- 接入现存对象（后补对象沿有效归属进入） ------------------------
    def register_objects(self, payload: dict) -> dict:
        objects = payload.get("objects") or []
        expected = payload.get("expected_version")
        registered, conflicts, ignored = [], [], []
        with self.store.connect() as db:
            if expected is not None:
                self.store.assert_version(db, expected)
            for row in objects:
                key, etag = row.get("key"), row.get("etag")
                if not key or not etag:
                    ignored.append({"key": key, "reason": "INVALID_VALUE"})
                    continue
                partition = core.partition_of(key)
                if not partition:
                    ignored.append({"key": key, "reason": "INVALID_VALUE"})
                    continue
                existing = self.store.get_attribution(db, key)
                if existing is None:
                    self.store.insert_attribution(db, key, etag, partition)
                    registered.append({"key": key, "partition": partition, "etag": etag})
                elif existing["etag"] == etag and existing["partition"] == partition:
                    ignored.append({"key": key, "reason": "ALREADY_REGISTERED"})
                else:
                    conflicts.append(
                        {
                            "key": key,
                            "reason": "DUPLICATE_CONFLICT",
                            "ledger": {
                                "etag": existing["etag"],
                                "partition": existing["partition"],
                            },
                            "incoming": {"etag": etag, "partition": partition},
                        }
                    )
            version = (
                self.store.bump_version(db) if registered else self.store.version(db)
            )
        return {
            "status": "READY",
            "reason": "DUPLICATE_CONFLICT" if conflicts else "OK",
            "version": version,
            "input_count": len(objects),
            "registered_count": len(registered),
            "conflict_count": len(conflicts),
            "ignored_count": len(ignored),
            "registered": registered,
            "conflicts": conflicts,
            "ignored": ignored,
            "total": self.store.active_total(),
        }

    # ---- 封账（责任数量固定在原小时分区） ------------------------------
    def seal_partition(self, payload: dict) -> dict:
        partition = payload.get("partition")
        expected = payload.get("expected_version")
        if not partition:
            raise DomainError("INVALID_VALUE", "partition 不能为空")
        if expected is None:
            raise DomainError("INVALID_VALUE", "expected_version 不能为空")
        with self.store.connect() as db:
            self.store.assert_version(db, expected)  # 过期版本拒绝，旧版保留
            if self.store.is_sealed(db, partition):
                raise DomainError("PARTITION_SEALED", f"分区已封账：{partition}")
            total = db.execute(
                "SELECT COUNT(*) FROM m7_attributions WHERE partition=?",
                (partition,),
            ).fetchone()[0]
            if total == 0:
                raise DomainError(
                    "PARTITION_EMPTY", f"分区无现存对象，不能封账：{partition}"
                )
            sealed_count = self.store.mark_sealed(db, partition)
            version = self.store.bump_version(db)
        return {
            "status": "READY",
            "reason": "OK",
            "version": version,
            "partition": partition,
            "sealed_count": sealed_count,
            "total": self.store.active_total(),
        }

    # ---- 合并 ----------------------------------------------------------
    def merge(self, payload: dict) -> dict:
        sources = payload.get("sources") or []
        target = payload.get("target")
        expected = payload.get("expected_version")
        merge_id = payload.get("merge_id")
        batch_size = payload.get("batch_size")
        manifest = payload.get("manifest")
        objects = payload.get("objects")
        if batch_size is not None and (
            not isinstance(batch_size, int) or batch_size <= 0
        ):
            raise DomainError("INVALID_VALUE", "batch_size 必须为正整数")

        existing = self.store.get_merge(merge_id) if merge_id else None
        if existing is None:
            # 新建合并单才校验输入与版本；恢复只凭 merge_id
            if not sources or not target:
                raise DomainError("INVALID_VALUE", "sources 与 target 不能为空")
            if any(core.day_of(p) != target for p in sources):
                raise DomainError(
                    "INVALID_VALUE", "源小时分区必须与目标日分区属于同一 dt"
                )
            if target in sources:
                raise DomainError("INVALID_VALUE", "目标分区不能同时是源分区")
        if existing is not None:
            if existing["status"] == "COMMITTED":
                result = self.progress({"merge_id": existing["merge_id"]})
                result["resumed"] = True
                return result
            if existing["status"] == "REVERTED":
                raise DomainError(
                    "MERGE_REVERTED",
                    f"合并单已撤销，不能继续转账：{existing['merge_id']}",
                )
            # RUNNING / FAILED：同 merge_id 恢复，不重复做版本校验（重试不重复转账）
            mid = existing["merge_id"]
            target = existing["target"]
            resume = True
        else:
            if expected is None:
                raise DomainError("INVALID_VALUE", "expected_version 不能为空")
            with self.store.connect() as db:
                self.store.assert_version(db, expected)  # 过期版本拒绝，零写入
                mid = merge_id or core.default_merge_id(
                    sources, target, self.store.version(db)
                )
                self.store.create_merge(
                    db, mid, sources, target, self.store.active_total()
                )
                self._plan(db, mid, sources, target, manifest, objects)
            resume = False

        snapshot_total = self.store.active_total()
        done, failed, planned, skipped = self._transfer_batch(mid, target, batch_size)
        if failed:
            self._update_merge(mid, "RUNNING", "TRANSFER_FAILED", failed)
            result = self.progress({"merge_id": mid})
            result["note"] = failed
        elif planned - skipped - done > 0:
            self._update_merge(mid, "RUNNING", "OK")
            result = self.progress({"merge_id": mid})
            result["note"] = "BATCH_LIMIT"
        else:
            result = self._commit(mid, snapshot_total)
        result["resumed"] = resume
        return result

    def _plan(
        self, db, merge_id: str, sources: list[str], target: str,
        manifest: list[dict] | None, objects: list[dict] | None,
    ) -> None:
        """把本次合并计划固化为 merge_items，恢复时无需重放输入。"""
        joined = core.join_manifest_objects(manifest or [], objects or [])
        sealed = self.store.sealed_partitions(db)
        plan = core.classify_items(joined, sources, target, sealed)
        for item in plan["transfer"]:
            self.store.add_item(
                db, merge_id, item["key"], item["etag"],
                item["partition"], target, "transfer",
            )
        for item in plan["kept_sealed"]:
            self.store.add_item(
                db, merge_id, item["key"], item["etag"],
                item["partition"], item["partition"], "kept_sealed",
            )

    def _transfer_batch(
        self, merge_id: str, target: str, batch_size: int | None
    ) -> tuple[int, str, int, int]:
        """逐条转账，每项独立短事务；已存在流水则跳过（重试不重复转账）。

        返回 (本批新处理数, 失败信息, 计划项总数, 此前已完成跳过数)。
        """
        with self.store.connect() as db:
            items = db.execute(
                "SELECT * FROM m7_merge_items WHERE merge_id=? ORDER BY key",
                (merge_id,),
            ).fetchall()
        planned = len(items)
        done, skipped, failed = 0, 0, ""
        for item in items:
            tid = core.transfer_id(merge_id, item["key"])
            with self.store.connect() as db:
                if self.store.transfer_exists(db, tid):
                    skipped += 1  # 幂等：中断恢复时跳过已完成项
                    continue
            if batch_size is not None and done >= batch_size:
                break
            try:
                with self.store.connect() as db:
                    if item["item_kind"] == "kept_sealed":
                        # 封账归属留在原小时，仅留证据，不改归属
                        self.store.record_transfer(
                            db, tid, merge_id, item["key"], item["etag"],
                            item["source_partition"], item["source_partition"],
                            "forward", kept_sealed=True,
                        )
                    elif self.store.is_sealed(db, item["source_partition"]):
                        # 合并与封账并发：转账瞬间源分区已封账，保留原小时归属
                        self.store.record_transfer(
                            db, tid, merge_id, item["key"], item["etag"],
                            item["source_partition"], item["source_partition"],
                            "forward", kept_sealed=True,
                        )
                    else:
                        self.store.move_attribution(db, item["key"], target)
                        self.store.record_transfer(
                            db, tid, merge_id, item["key"], item["etag"],
                            item["source_partition"], target,
                            "forward", kept_sealed=False,
                        )
                    # with 退出即提交：跨分区写入逐项落盘，中断后可从此项之后恢复
            except Exception as exc:  # 单项失败不影响已落盘项
                failed = f"{item['key']}: {exc}"
                break
            done += 1
        return done, failed, planned, skipped

    def _update_merge(self, merge_id: str, status: str, reason: str, detail: str = "") -> None:
        with self.store.connect() as db:
            self.store.update_merge(db, merge_id, status, reason, detail)

    def _commit(self, merge_id: str, snapshot_total: int) -> dict:
        after = self.store.active_total()
        if not core.conservation_holds(snapshot_total, after):
            # 守住总量：不守恒则不提交、不升版，失败保留旧版
            detail = f"snapshot={snapshot_total} current={after}"
            self._update_merge(merge_id, "FAILED", "TOTAL_MISMATCH", detail)
            result = self.progress({"merge_id": merge_id})
            result["note"] = f"TOTAL_MISMATCH {detail}"
            return result
        with self.store.connect() as db:
            self.store.update_merge(db, merge_id, "COMMITTED", "OK")
            version = self.store.bump_version(db)
        result = self.progress({"merge_id": merge_id})
        result["version"] = version
        return result

    # ---- 撤销合并：按对象退回原分区 ------------------------------------
    def revert(self, payload: dict) -> dict:
        merge_id = payload.get("merge_id")
        expected = payload.get("expected_version")
        if not merge_id:
            raise DomainError("INVALID_VALUE", "merge_id 不能为空")
        if expected is None:
            raise DomainError("INVALID_VALUE", "expected_version 不能为空")
        merge_row = self.store.get_merge(merge_id)
        if merge_row is None:
            raise DomainError("MERGE_NOT_FOUND", f"合并单不存在：{merge_id}")
        with self.store.connect() as db:
            # 过期版本优先拒绝：即使是已撤销单的重放，也先校验版本（失败保留旧版）
            self.store.assert_version(db, expected)
        if merge_row["status"] == "REVERTED":
            result = self.progress({"merge_id": merge_id})
            result["note"] = "ALREADY_REVERTED"
            result["resumed"] = True
            return result
        if merge_row["status"] != "COMMITTED":
            raise DomainError(
                "MERGE_NOT_COMMITTED",
                f"合并单状态为 {merge_row['status']}，不能撤销",
            )
        snapshot_total = self.store.active_total()
        transfers = self.store.list_transfers(merge_id)
        moved_back, kept = 0, 0
        for tr in transfers:
            if tr["direction"] != "forward":
                continue
            reverse_tid = "rev-" + tr["transfer_id"]
            with self.store.connect() as db:
                if self.store.transfer_exists(db, reverse_tid):
                    continue  # 撤销本身也幂等
            try:
                with self.store.connect() as db:
                    if tr["kept_sealed"]:
                        # 封账责任仍属原小时，撤销不移动
                        self.store.record_transfer(
                            db, reverse_tid, merge_id, tr["key"], tr["etag"],
                            tr["from_partition"], tr["from_partition"],
                            "revert", kept_sealed=True,
                        )
                        kept += 1
                        continue
                    current = self.store.get_attribution(db, tr["key"])
                    # 后补对象不在流水内：沿有效归属进入，不被本次撤销触碰
                    if current is None or current["partition"] != tr["to_partition"]:
                        continue
                    self.store.move_attribution(db, tr["key"], tr["from_partition"])
                    self.store.record_transfer(
                        db, reverse_tid, merge_id, tr["key"], tr["etag"],
                        tr["to_partition"], tr["from_partition"],
                        "revert", kept_sealed=False,
                    )
                    moved_back += 1
                    # with 退出即提交：中断后重试从下一对象继续，不重复退回
            except Exception as exc:
                # 单项失败：已退回项保留落盘，状态维持 COMMITTED 并记录中断详情，等待重试
                self._update_merge(
                    merge_id, "COMMITTED", "TRANSFER_FAILED", f"{tr['key']}: {exc}"
                )
                result = self.progress({"merge_id": merge_id})
                result["note"] = f"{tr['key']}: {exc}"
                return result
        after = self.store.active_total()
        if not core.conservation_holds(snapshot_total, after):
            detail = f"snapshot={snapshot_total} current={after}"
            self._update_merge(merge_id, "FAILED", "TOTAL_MISMATCH", detail)
            result = self.progress({"merge_id": merge_id})
            result["note"] = f"TOTAL_MISMATCH {detail}"
            return result
        with self.store.connect() as db:
            self.store.update_merge(db, merge_id, "REVERTED", "OK")
            version = self.store.bump_version(db)
        result = self.progress({"merge_id": merge_id})
        result.update(
            {
                "version": version,
                "moved_back_count": moved_back,
                "kept_sealed_count": kept,
                "reason": "OK",
            }
        )
        return result

    # ---- 读模型：合并进度 ----------------------------------------------
    def _progress(self, db, merge_row: dict, note: str = "") -> dict:
        merge_id = merge_row["merge_id"]
        items = self.store.list_items(db, merge_id)
        planned = len(items)
        transfer_planned = sum(1 for i in items if i["item_kind"] == "transfer")
        sealed_planned = sum(1 for i in items if i["item_kind"] == "kept_sealed")
        transfers = db.execute(
            "SELECT direction, COUNT(*) AS n FROM m7_transfers"
            " WHERE merge_id=? GROUP BY direction",
            (merge_id,),
        ).fetchall()
        forward_done = next(
            (r["n"] for r in transfers if r["direction"] == "forward"), 0
        )
        revert_done = next(
            (r["n"] for r in transfers if r["direction"] == "revert"), 0
        )
        current_total = self.store.active_total(db)
        return {
            "status": "READY",
            "merge_id": merge_id,
            "merge_status": merge_row["status"],
            "reason": merge_row["reason"] if merge_row["reason"] != "OK" or not note else "OK",
            "note": note,
            "sources": json.loads(merge_row["sources"]),
            "target": merge_row["target"],
            "base_version": merge_row["base_version"],
            "version": self.store.version(db),
            "planned_count": planned,
            "transfer_planned": transfer_planned,
            "kept_sealed_planned": sealed_planned,
            "forward_done": forward_done,
            "revert_done": revert_done,
            "remaining": max(planned - forward_done, 0),
            "baseline_total": merge_row["baseline_total"],
            "current_total": current_total,
            # 合并/撤销只迁移归属、不消灭归属：当前总量不少于合并建立时即守住下限
            "conservation": current_total >= merge_row["baseline_total"],
        }

    def progress(self, payload: dict) -> dict:
        merge_id = payload.get("merge_id")
        if not merge_id:
            raise DomainError("INVALID_VALUE", "merge_id 不能为空")
        with self.store.connect() as db:
            row = db.execute(
                "SELECT * FROM m7_merges WHERE merge_id=?", (merge_id,)
            ).fetchone()
            if row is None:
                raise DomainError("MERGE_NOT_FOUND", f"合并单不存在：{merge_id}")
            return self._progress(db, dict(row))

    def list_merges(self) -> dict:
        return {"status": "READY", "merges": self.store.list_merges()}

    # ---- 读模型：当前与封账差异，守住总量 ------------------------------
    def diff(self) -> dict:
        partitions = self.store.partition_counts()
        current_total = sum(p["current"] for p in partitions.values())
        sealed_total = sum(p["sealed"] for p in partitions.values())
        return {
            "status": "READY",
            "reason": "DIFF" if sealed_total else "OK",
            "version": self.store.version(),
            "partitions": [
                {"partition": name, **values}
                for name, values in sorted(partitions.items())
            ],
            "current_total": current_total,
            "sealed_total": sealed_total,
            "unsealed_total": current_total - sealed_total,
            "seals": self.store.list_seals(),
        }

    def attributions(self) -> dict:
        rows = self.store.list_attributions()
        return {
            "status": "READY",
            "total": len(rows),
            "sealed_total": sum(1 for r in rows if r["sealed"]),
            "attributions": rows,
        }
