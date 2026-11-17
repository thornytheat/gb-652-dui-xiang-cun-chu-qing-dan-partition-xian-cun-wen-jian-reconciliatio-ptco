"""分区合并领域纯函数：分区解析、清单/现存对接、合并计划与守恒校验。

设计约束（来自需求）：
- 不改 key 与摘要（etag），路径仅用于推导分区。
- 已封账归属留在原小时分区；未封账归属转入目标日分区并注销原归属。
- 后补对象沿其自身有效归属进入，不被历史合并自动领取、不做补数领取。
"""
from __future__ import annotations

from app.manifest import canonical_digest

SAMPLE = {
    "sources": ["dt=2026-11-15/hour=00", "dt=2026-11-15/hour=01"],
    "target": "dt=2026-11-15",
    "expected_version": 0,
    "manifest": [
        {"key": "dt=2026-11-15/hour=00/a.parquet", "size": 120, "etag": "aaa"},
        {"key": "dt=2026-11-15/hour=01/b.parquet", "size": 80, "etag": "bbb"},
    ],
    "objects": [
        {"key": "dt=2026-11-15/hour=00/a.parquet", "size": 120, "etag": "aaa"},
        {"key": "dt=2026-11-15/hour=01/b.parquet", "size": 80, "etag": "bbb"},
        {"key": "dt=2026-11-15/hour=03/orphan.parquet", "size": 20, "etag": "zzz"},
    ],
}


def partition_of(key: str) -> str | None:
    """从 key 推导分区。

    含 hour 段返回小时分区（``dt=.../hour=00``）；
    只有 dt 段的日级 key（合并后新写入）返回日分区（``dt=...``）；
    两者都没有则返回 None。
    """
    parts = [p for p in key.split("/") if "=" in p]
    dt = next((p for p in parts if p.startswith("dt=")), None)
    hour = next((p for p in parts if p.startswith("hour=")), None)
    if not dt:
        return None
    return f"{dt}/{hour}" if hour else dt


def day_of(partition: str | None) -> str | None:
    """小时分区上卷到日分区，如 ``dt=2026-11-15/hour=00`` -> ``dt=2026-11-15``。"""
    if not partition:
        return None
    dt = next((p for p in partition.split("/") if p.startswith("dt=")), None)
    return dt


def index_by_key(rows: list[dict]) -> dict[str, dict]:
    """以 key 建索引；重复 key 后写覆盖（M1 已将冲突重复行隔离）。"""
    return {row["key"]: row for row in rows}


def join_manifest_objects(manifest: list[dict], objects: list[dict]) -> dict:
    """接入实际清单与现存对象，按 key 对接并保留逐项证据。

    - matched：两侧 key、size、etag 完全一致，可参与合并转账；
    - conflict：key 相同但 size 或 etag 不一致，保留两侧原值，不转账；
    - missing：清单有、现存无，不转账；
    - orphan：现存有、清单无，沿其自身有效归属，不做补数领取。
    """
    left = index_by_key(manifest)
    right = index_by_key(objects)
    matched, conflict, missing, orphan = [], [], [], []
    for key in sorted(left):
        if key not in right:
            missing.append({"key": key, "manifest": left[key]})
            continue
        if (left[key].get("size"), left[key].get("etag")) != (
            right[key].get("size"),
            right[key].get("etag"),
        ):
            conflict.append({"key": key, "manifest": left[key], "object": right[key]})
        else:
            matched.append({"key": key, "size": right[key]["size"], "etag": right[key]["etag"]})
    for key in sorted(right):
        if key not in left:
            orphan.append({"key": key, "object": right[key]})
    return {"matched": matched, "conflict": conflict, "missing": missing, "orphan": orphan}


def classify_items(
    joined: dict,
    sources: list[str],
    target: str,
    sealed_partitions: set[str] | None = None,
) -> dict:
    """把对接结果按封账状态与分区归属分类。

    - transfer：未封账且属于源小时分区的匹配项，转入目标分区；
    - kept_sealed：封账项保留原小时归属（封账数量仍属原小时）；
    - outside_source：分区不在合并范围内的匹配项，不参与本次合并；
    - conflict/missing/orphan：透传对接证据，均不转账。
    """
    sealed_partitions = sealed_partitions or set()
    transfer, kept_sealed, outside = [], [], []
    for row in joined["matched"]:
        partition = partition_of(row["key"])
        item = {"key": row["key"], "partition": partition, "size": row["size"], "etag": row["etag"]}
        if partition not in sources:
            outside.append(item)
        elif partition in sealed_partitions:
            kept_sealed.append(item)
        else:
            transfer.append(item)
    return {
        "transfer": transfer,
        "kept_sealed": kept_sealed,
        "outside_source": outside,
        "conflict": joined["conflict"],
        "missing": joined["missing"],
        "orphan": joined["orphan"],
    }


def default_merge_id(sources: list[str], target: str, base_version: int) -> str:
    raw = {"sources": sorted(sources), "target": target, "base_version": base_version}
    return "merge-" + canonical_digest(raw)[:12]


def transfer_id(merge_id: str, key: str) -> str:
    return canonical_digest({"merge": merge_id, "key": key})[:16]


def conservation_holds(before_total: int, after_total: int) -> bool:
    """总量守恒：合并/撤销只迁移归属，不新增也不消灭有效归属。"""
    return before_total == after_total
