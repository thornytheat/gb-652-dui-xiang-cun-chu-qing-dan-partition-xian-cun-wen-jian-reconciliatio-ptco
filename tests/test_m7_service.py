import pytest

from app.errors import DomainError
from app.modules.m7_merges.service import MergeService
from app.modules.m7_merges.store import MergeStore

H00 = "dt=2026-11-15/hour=00"
H01 = "dt=2026-11-15/hour=01"
H02 = "dt=2026-11-15/hour=02"
DAY = "dt=2026-11-15"


def key(hour, name):
    return f"dt=2026-11-15/hour={hour}/{name}.parquet"


def row(hour, name, etag=None, size=10):
    k = key(hour, name)
    return {"key": k, "size": size, "etag": etag or f"e-{name}"}


@pytest.fixture
def service(tmp_path):
    return MergeService(MergeStore(str(tmp_path / "m7.db")))


def seed(service, *rows_):
    out = service.register_objects({"objects": list(rows_)})
    return out["version"]


def merge_payload(version, manifest, objects_=None, **extra):
    payload = {
        "sources": [H00, H01],
        "target": DAY,
        "expected_version": version,
        "manifest": manifest,
        "objects": objects_ if objects_ is not None else manifest,
    }
    payload.update(extra)
    return payload


# ---- 接入现存对象与封账 ----------------------------------------------
def test_register_assigns_natural_partition(service):
    out = service.register_objects({"objects": [row("00", "a"), row("01", "b")]})
    assert out["registered_count"] == 2 and out["version"] == 1
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert attrs[key("00", "a")]["partition"] == H00


def test_register_idempotent_and_conflict(service):
    seed(service, row("00", "a"))
    again = service.register_objects({"objects": [row("00", "a")]})
    assert again["ignored_count"] == 1 and again["version"] == 1  # 幂等不升版
    conflict = service.register_objects(
        {"objects": [{"key": key("00", "a"), "size": 10, "etag": "changed"}]}
    )
    assert conflict["conflict_count"] == 1
    assert conflict["conflicts"][0]["reason"] == "DUPLICATE_CONFLICT"
    # 冲突不覆盖原账本
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert attrs[key("00", "a")]["etag"] == "e-a"


def test_register_skips_keys_without_partition(service):
    out = service.register_objects(
        {"objects": [{"key": "bad", "size": 1, "etag": "x"}]}
    )
    assert out["ignored"][0]["reason"] == "INVALID_VALUE"
    assert service.attributions()["total"] == 0


def test_seal_marks_responsibility_in_original_hour(service):
    v = seed(service, row("00", "a"), row("00", "b"))
    out = service.seal_partition({"partition": H00, "expected_version": v})
    assert out["sealed_count"] == 2 and out["version"] == v + 1
    diff = service.diff()
    part = next(p for p in diff["partitions"] if p["partition"] == H00)
    assert part["sealed"] == 2 and part["unsealed"] == 0


def test_seal_empty_partition_rejected(service):
    with pytest.raises(DomainError) as exc:
        service.seal_partition({"partition": H02, "expected_version": 0})
    assert exc.value.code == "PARTITION_EMPTY"
    assert service.diff()["version"] == 0  # 失败保留旧版


def test_seal_stale_version_rejected_before_any_write(service):
    v = seed(service, row("00", "a"))
    with pytest.raises(DomainError) as exc:
        service.seal_partition({"partition": H00, "expected_version": v - 1})
    assert exc.value.code == "STALE_VERSION"
    diff = service.diff()
    assert diff["version"] == v  # 旧版保留
    assert all(a["sealed"] == 0 for a in service.attributions()["attributions"])


# ---- 合并主流程 -------------------------------------------------------
def test_merge_moves_unsealed_and_keeps_sealed(service):
    seed(service, row("00", "a"), row("01", "b"), row("01", "c"))
    v = service.seal_partition({"partition": H01, "expected_version": 1})["version"]
    manifest = [row("00", "a"), row("01", "b"), row("01", "c")]
    out = service.merge(merge_payload(v, manifest))
    assert out["merge_status"] == "COMMITTED"
    assert out["transfer_planned"] == 1
    assert out["kept_sealed_planned"] == 2
    assert out["forward_done"] == 3 and out["remaining"] == 0
    assert out["conservation"] is True
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert attrs[key("00", "a")]["partition"] == DAY       # 未封账转入日分区
    assert attrs[key("01", "b")]["partition"] == H01       # 封账责任仍属原小时
    assert attrs[key("01", "c")]["partition"] == H01
    # key 与摘要原样保留
    assert attrs[key("00", "a")]["etag"] == "e-a"
    assert service.diff()["current_total"] == 3


def test_merge_uses_manifest_and_objects_and_reports_diffs(service):
    v = seed(service, row("00", "a"), row("01", "b"), row("02", "orphan"))
    manifest = [
        row("00", "a"),
        row("01", "b"),
        {"key": key("01", "missing"), "size": 1, "etag": "e-m"},  # 清单有现存无
        {"key": key("00", "a"), "size": 99, "etag": "other"},     # 同 key 摘要冲突
    ]
    objects_ = [
        row("00", "a"),
        row("01", "b"),
        row("02", "orphan"),  # 清单外现存对象：不做补数领取
    ]
    out = service.merge(merge_payload(v, manifest, objects_))
    assert out["merge_status"] == "COMMITTED"
    # 冲突项与缺失项不在计划内，只有 b 完成转账
    assert out["transfer_planned"] == 1
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert key("02", "orphan") in attrs  # 仍沿自身归属存在
    assert attrs[key("02", "orphan")]["partition"] == H02
    assert attrs[key("00", "a")]["partition"] == H00  # 冲突项不转账
    assert attrs[key("01", "b")]["partition"] == DAY
    assert service.diff()["current_total"] == 3  # 守恒：不新增不消灭


def test_merge_stale_version_rejected_with_zero_writes(service):
    v = seed(service, row("00", "a"))
    payload = merge_payload(v - 1, [row("00", "a")])
    with pytest.raises(DomainError) as exc:
        service.merge(payload)
    assert exc.value.code == "STALE_VERSION"
    assert service.list_merges()["merges"] == []  # 没有合并单、没有转账
    assert service.diff()["version"] == v
    attrs = service.attributions()["attributions"]
    assert attrs[0]["partition"] == H00


def test_merge_validates_partition_family(service):
    v = seed(service, row("00", "a"))
    payload = merge_payload(v, [row("00", "a")], target="dt=2026-11-16")
    with pytest.raises(DomainError) as exc:
        service.merge(payload)
    assert exc.value.code == "INVALID_VALUE"


# ---- 中断恢复与重试幂等 -----------------------------------------------
def test_merge_resumes_after_batch_interrupt_without_double_transfer(service):
    v = seed(service, row("00", "a"), row("00", "b"), row("01", "c"))
    manifest = [row("00", "a"), row("00", "b"), row("01", "c")]
    first = service.merge(merge_payload(v, manifest, batch_size=1, merge_id="m-resume"))
    assert first["merge_status"] == "RUNNING" and first["note"] == "BATCH_LIMIT"
    assert first["forward_done"] == 1 and first["remaining"] == 2
    second = service.merge(
        {"merge_id": "m-resume", "batch_size": 1}
    )  # 恢复：无需版本号与输入，继续限量一批
    assert second["merge_status"] == "RUNNING" and second["forward_done"] == 2
    third = service.merge({"merge_id": "m-resume"})  # 不带限量，收尾提交
    assert third["merge_status"] == "COMMITTED" and third["forward_done"] == 3
    # 流水唯一，重试不重复转账
    transfers = service.store.list_transfers("m-resume")
    assert len([t for t in transfers if t["direction"] == "forward"]) == 3
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert all(a["partition"] == DAY for a in attrs.values())


def test_merge_retry_is_idempotent_after_transfer_failure(service, monkeypatch):
    v = seed(service, row("00", "a"), row("00", "b"))
    manifest = [row("00", "a"), row("00", "b")]
    calls = {"n": 0}
    real = MergeStore.move_attribution

    def flaky(self, db, key, target):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated cross-partition write interrupt")
        return real(self, db, key, target)

    monkeypatch.setattr(MergeStore, "move_attribution", flaky)
    first = service.merge(merge_payload(v, manifest, merge_id="m-flaky"))
    assert first["merge_status"] == "RUNNING"
    assert first["reason"] == "TRANSFER_FAILED"
    monkeypatch.setattr(MergeStore, "move_attribution", real)  # 恢复后故障消失
    second = service.merge({"merge_id": "m-flaky"})
    assert second["merge_status"] == "COMMITTED"
    forward = [t for t in service.store.list_transfers("m-flaky")
               if t["direction"] == "forward"]
    assert len(forward) == 2  # 首项未留下流水，恢复后只做未完成项
    assert service.diff()["current_total"] == 2


def test_seal_landing_between_retries_keeps_object_in_hour(service):
    v = seed(service, row("00", "a"), row("01", "b"))
    manifest = [row("00", "a"), row("01", "b")]
    first = service.merge(merge_payload(v, manifest, batch_size=1, merge_id="m-seal"))
    assert first["forward_done"] == 1  # 按 key 排序先处理 hour=00/a
    # 合并进行中，hour=01 封账（版本已因登记升过，当前版本为 v）
    service.seal_partition({"partition": H01, "expected_version": v})
    second = service.merge({"merge_id": "m-seal"})
    assert second["merge_status"] == "COMMITTED"
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert attrs[key("00", "a")]["partition"] == DAY
    assert attrs[key("01", "b")]["partition"] == H01  # 封账归属保留
    transfers = {t["key"]: t for t in service.store.list_transfers("m-seal")
                 if t["direction"] == "forward"}
    assert transfers[key("01", "b")]["kept_sealed"] == 1


def test_committed_merge_retry_returns_same_progress(service):
    v = seed(service, row("00", "a"))
    out = service.merge(merge_payload(v, [row("00", "a")], merge_id="m-1"))
    again = service.merge({"merge_id": "m-1"})
    assert again["merge_status"] == "COMMITTED"
    assert again["forward_done"] == out["forward_done"] == 1
    assert len(service.store.list_transfers("m-1")) == 1  # 无新增流水


# ---- 撤销合并 ---------------------------------------------------------
def test_revert_returns_objects_to_original_partitions(service):
    v = seed(service, row("00", "a"), row("01", "b"))
    merge_out = service.merge(merge_payload(v, [row("00", "a"), row("01", "b")]))
    out = service.revert(
        {"merge_id": merge_out["merge_id"], "expected_version": merge_out["version"]}
    )
    assert out["merge_status"] == "REVERTED"
    assert out["moved_back_count"] == 2
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert attrs[key("00", "a")]["partition"] == H00
    assert attrs[key("01", "b")]["partition"] == H01
    assert service.diff()["current_total"] == 2


def test_revert_keeps_sealed_and_backfilled_objects(service):
    seed(service, row("00", "a"), row("01", "b"))
    v = service.seal_partition({"partition": H01, "expected_version": 1})["version"]
    merge_out = service.merge(merge_payload(v, [row("00", "a"), row("01", "b")]))
    # 合并后补对象：沿有效归属进入日分区，但不在合并流水内
    service.register_objects(
        {"objects": [{"key": f"{DAY}/late.parquet", "size": 1, "etag": "late"}]}
    )
    before = {a["key"]: a for a in service.attributions()["attributions"]}
    out = service.revert(
        {"merge_id": merge_out["merge_id"],
         "expected_version": service.diff()["version"]}
    )
    assert out["moved_back_count"] == 1 and out["kept_sealed_count"] == 1
    after = {a["key"]: a for a in service.attributions()["attributions"]}
    assert after[key("01", "b")]["partition"] == H01          # 封账不动
    assert after[f"{DAY}/late.parquet"]["partition"] == DAY   # 后补对象不被退回、不被领取
    assert after[key("00", "a")]["partition"] == H00
    assert set(before) == set(after)                          # 总量与对象集合守恒


def test_revert_is_idempotent_and_rejects_stale_version(service):
    v = seed(service, row("00", "a"))
    merge_out = service.merge(merge_payload(v, [row("00", "a")]))
    first = service.revert(
        {"merge_id": merge_out["merge_id"],
         "expected_version": merge_out["version"]}
    )
    # 幂等重放需带撤销后的最新版本
    second = service.revert(
        {"merge_id": merge_out["merge_id"], "expected_version": first["version"]}
    )
    assert first["merge_status"] == second["merge_status"] == "REVERTED"
    reverses = [t for t in service.store.list_transfers(merge_out["merge_id"])
                if t["direction"] == "revert"]
    assert len(reverses) == 1  # 重跑不重复退回
    # 带旧 expected_version 必须被拒绝（并发封账/合并的过期版本）
    with pytest.raises(DomainError) as exc:
        service.revert({"merge_id": merge_out["merge_id"],
                        "expected_version": merge_out["version"]})
    assert exc.value.code == "STALE_VERSION"


def test_revert_unknown_and_uncommitted_merge(service):
    with pytest.raises(DomainError) as exc:
        service.revert({"merge_id": "nope", "expected_version": 0})
    assert exc.value.code == "MERGE_NOT_FOUND"
    v = seed(service, row("00", "a"), row("00", "z"))
    service.merge(
        merge_payload(v, [row("00", "a"), row("00", "z")],
                      batch_size=1, merge_id="m-run2")
    )  # 首批一项后中断，合并单仍为 RUNNING
    with pytest.raises(DomainError) as exc:
        service.revert({"merge_id": "m-run2",
                        "expected_version": service.diff()["version"]})
    assert exc.value.code == "MERGE_NOT_COMMITTED"


# ---- 读模型与守恒 -----------------------------------------------------
def test_diff_reports_current_vs_sealed_and_holds_total(service):
    seed(service, row("00", "a"), row("01", "b"))
    service.seal_partition({"partition": H00, "expected_version": 1})
    diff = service.diff()
    assert diff["current_total"] == 2
    assert diff["sealed_total"] == 1
    assert diff["unsealed_total"] == 1
    assert diff["current_total"] == diff["sealed_total"] + diff["unsealed_total"]


def test_progress_readback(service):
    v = seed(service, row("00", "a"), row("01", "b"))
    created = service.merge(merge_payload(v, [row("00", "a"), row("01", "b")],
                                          merge_id="m-prog"))
    readback = service.progress({"merge_id": "m-prog"})
    assert readback["merge_id"] == created["merge_id"]
    assert readback["planned_count"] == 2 and readback["forward_done"] == 2


def test_unknown_progress(service):
    with pytest.raises(DomainError) as exc:
        service.progress({"merge_id": "ghost"})
    assert exc.value.code == "MERGE_NOT_FOUND"


def test_failed_merge_can_retry_and_commit_after_issue_clears(service, monkeypatch):
    v = seed(service, row("00", "a"), row("00", "b"))
    manifest = [row("00", "a"), row("00", "b")]
    real = MergeStore.move_attribution
    calls = {"n": 0}

    def flaky(self, db, key, target):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("interrupt")
        return real(self, db, key, target)

    monkeypatch.setattr(MergeStore, "move_attribution", flaky)
    first = service.merge(merge_payload(v, manifest, merge_id="m-recover"))
    assert first["merge_status"] == "RUNNING"
    assert first["reason"] == "TRANSFER_FAILED"
    monkeypatch.setattr(MergeStore, "move_attribution", real)
    second = service.merge({"merge_id": "m-recover"})  # 失败保留旧版，重试恢复
    assert second["merge_status"] == "COMMITTED"
    forward = [t for t in service.store.list_transfers("m-recover")
               if t["direction"] == "forward"]
    assert len(forward) == 2  # 不重复转账
    assert service.diff()["current_total"] == 2


def test_revert_interrupt_resumes_without_double_return(service, monkeypatch):
    v = seed(service, row("00", "a"), row("00", "b"))
    merge_out = service.merge(
        merge_payload(v, [row("00", "a"), row("00", "b")], merge_id="m-ri")
    )
    real = MergeStore.move_attribution
    calls = {"n": 0}

    def flaky(self, db, key, target):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("revert interrupt")
        return real(self, db, key, target)

    monkeypatch.setattr(MergeStore, "move_attribution", flaky)
    first = service.revert(
        {"merge_id": "m-ri", "expected_version": merge_out["version"]}
    )
    assert first["note"].endswith("revert interrupt")
    monkeypatch.setattr(MergeStore, "move_attribution", real)
    second = service.revert(
        {"merge_id": "m-ri", "expected_version": first["version"]}
    )
    assert second["merge_status"] == "REVERTED"
    reverses = [t for t in service.store.list_transfers("m-ri")
                if t["direction"] == "revert"]
    assert len(reverses) == 2  # 只补做未退回项
    attrs = {a["key"]: a for a in service.attributions()["attributions"]}
    assert all(a["partition"].startswith("dt=2026-11-15/hour=")
               for a in attrs.values())


def test_failed_merge_keeps_old_version_when_conservation_breaks(service, monkeypatch):
    v = seed(service, row("00", "a"), row("01", "b"))
    manifest = [row("00", "a"), row("01", "b")]
    phase = {"committing": False}
    real = MergeStore.active_total

    def shrinking_total(self, db=None):
        total = real(self, db)
        # 提交阶段：操作开始快照读真实值，提交读少 1，模拟守恒被破坏
        return total - 1 if phase["committing"] else total

    monkeypatch.setattr(MergeStore, "active_total", shrinking_total)
    real_commit = MergeService._commit

    def commit_then_shrink(self, merge_id, snapshot_total):
        phase["committing"] = True  # 快照已在进入 _commit 前读取
        try:
            return real_commit(self, merge_id, snapshot_total)
        finally:
            phase["committing"] = False

    monkeypatch.setattr(MergeService, "_commit", commit_then_shrink)
    out = service.merge(merge_payload(v, manifest, merge_id="m-fail"))
    assert out["merge_status"] == "FAILED"
    assert out["note"].startswith("TOTAL_MISMATCH")
    # 失败保留旧版：版本号没有因合并而推进
    assert service.diff()["version"] == v
    row_ = service.store.get_merge("m-fail")
    assert row_["status"] == "FAILED" and row_["reason"] == "TOTAL_MISMATCH"
