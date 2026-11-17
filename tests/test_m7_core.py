from app.modules.m7_merges.core import (
    classify_items,
    conservation_holds,
    day_of,
    default_merge_id,
    join_manifest_objects,
    partition_of,
    transfer_id,
)

H00 = "dt=2026-11-15/hour=00"
H01 = "dt=2026-11-15/hour=01"
DAY = "dt=2026-11-15"


def test_partition_of_extracts_hour_partition():
    assert partition_of("dt=2026-11-15/hour=00/a.parquet") == H00


def test_partition_of_rejects_key_without_dt():
    assert partition_of("hour=00/a.parquet") is None
    assert partition_of("bad") is None


def test_partition_of_accepts_day_level_key():
    # 合并后写入日分区的对象只有 dt 段
    assert partition_of("dt=2026-11-15/late.parquet") == DAY


def test_day_of_rolls_up_to_day():
    assert day_of(H00) == DAY
    assert day_of(None) is None


def test_join_matched_conflict_missing_orphan():
    joined = join_manifest_objects(
        manifest=[
            {"key": "a", "size": 1, "etag": "x"},
            {"key": "b", "size": 2, "etag": "y"},
            {"key": "m", "size": 3, "etag": "z"},
        ],
        objects=[
            {"key": "a", "size": 1, "etag": "x"},
            {"key": "b", "size": 9, "etag": "y"},
            {"key": "o", "size": 4, "etag": "w"},
        ],
    )
    assert [r["key"] for r in joined["matched"]] == ["a"]
    assert [r["key"] for r in joined["conflict"]] == ["b"]
    assert [r["key"] for r in joined["missing"]] == ["m"]
    assert [r["key"] for r in joined["orphan"]] == ["o"]


def test_join_keeps_conflict_evidence():
    joined = join_manifest_objects(
        [{"key": "b", "size": 2, "etag": "y"}],
        [{"key": "b", "size": 9, "etag": "y"}],
    )
    row = joined["conflict"][0]
    assert row["manifest"]["size"] == 2 and row["object"]["size"] == 9


def test_classify_sealed_stays_in_hour_unsealed_transfers():
    joined = join_manifest_objects(
        manifest=[
            {"key": f"{H00}/a", "size": 1, "etag": "ea"},
            {"key": f"{H01}/b", "size": 2, "etag": "eb"},
            {"key": f"{H01}/c", "size": 3, "etag": "ec"},
        ],
        objects=[
            {"key": f"{H00}/a", "size": 1, "etag": "ea"},
            {"key": f"{H01}/b", "size": 2, "etag": "eb"},
            {"key": f"{H01}/c", "size": 3, "etag": "ec"},
        ],
    )
    plan = classify_items(joined, [H00, H01], DAY, sealed_partitions={H01})
    assert [i["key"] for i in plan["transfer"]] == [f"{H00}/a"]
    assert sorted(i["key"] for i in plan["kept_sealed"]) == [f"{H01}/b", f"{H01}/c"]


def test_classify_outside_source_is_not_claimed():
    joined = join_manifest_objects(
        [{"key": "dt=2026-11-15/hour=09/z", "size": 1, "etag": "z"}],
        [{"key": "dt=2026-11-15/hour=09/z", "size": 1, "etag": "z"}],
    )
    plan = classify_items(joined, [H00], DAY)
    assert plan["transfer"] == []
    assert plan["outside_source"][0]["partition"] == "dt=2026-11-15/hour=09"


def test_default_merge_id_stable_and_distinct():
    a = default_merge_id([H00, H01], DAY, 0)
    b = default_merge_id([H01, H00], DAY, 0)  # 顺序无关
    c = default_merge_id([H00], DAY, 0)
    assert a == b and a != c and a.startswith("merge-")


def test_transfer_id_deterministic():
    assert transfer_id("m1", "k") == transfer_id("m1", "k")
    assert transfer_id("m1", "k") != transfer_id("m2", "k")


def test_conservation():
    assert conservation_holds(3, 3) is True
    assert conservation_holds(3, 2) is False
