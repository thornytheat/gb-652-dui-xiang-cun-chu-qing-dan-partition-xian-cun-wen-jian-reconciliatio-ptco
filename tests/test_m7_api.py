import pytest

H00 = "dt=2026-11-15/hour=00"
H01 = "dt=2026-11-15/hour=01"
DAY = "dt=2026-11-15"


def row(hour, name):
    return {"key": f"dt=2026-11-15/hour={hour}/{name}.parquet",
            "size": 10, "etag": f"e-{name}"}


@pytest.fixture
def ledger(client):
    rows = [row("00", "a"), row("01", "b")]
    out = client.post("/api/merges/objects", json={"objects": rows}).json()
    return client, rows, out["version"]


def test_merges_page_renders(client):
    response = client.get("/merges")
    assert response.status_code == 200
    assert "panel" in response.text


def test_full_merge_and_revert_flow_over_http(ledger):
    client, rows, version = ledger
    seal = client.post("/api/merges/seal",
                       json={"partition": H01, "expected_version": version})
    assert seal.status_code == 200 and seal.json()["sealed_count"] == 1
    merge = client.post("/api/merges", json={
        "sources": [H00, H01], "target": DAY,
        "expected_version": seal.json()["version"],
        "manifest": rows, "objects": rows, "merge_id": "http-1",
    })
    assert merge.status_code == 200
    body = merge.json()
    assert body["merge_status"] == "COMMITTED"
    assert body["transfer_planned"] == 1 and body["kept_sealed_planned"] == 1

    progress = client.get("/api/merges/progress", params={"merge_id": "http-1"})
    assert progress.json()["forward_done"] == 2

    diff = client.get("/api/merges/diff").json()
    assert diff["current_total"] == 2
    sealed_part = next(p for p in diff["partitions"] if p["partition"] == H01)
    assert sealed_part["sealed"] == 1  # 封账数量仍属原小时
    target_part = next(p for p in diff["partitions"] if p["partition"] == DAY)
    assert target_part["current"] == 1  # 当前差异归新分区

    revert = client.post("/api/merges/revert", json={
        "merge_id": "http-1", "expected_version": body["version"],
    })
    assert revert.status_code == 200
    assert revert.json()["merge_status"] == "REVERTED"
    attrs = client.get("/api/merges/attributions").json()["attributions"]
    assert {a["partition"] for a in attrs} == {H00, H01}


def test_stale_version_returns_422_and_keeps_old_version(ledger):
    client, rows, version = ledger
    response = client.post("/api/merges", json={
        "sources": [H00, H01], "target": DAY,
        "expected_version": version - 1,
        "manifest": rows, "objects": rows,
    })
    assert response.status_code == 422
    assert response.json()["error"] == "STALE_VERSION"
    assert client.get("/api/merges/diff").json()["version"] == version
    assert client.get("/api/merges/list").json()["merges"] == []


def test_resume_retry_does_not_duplicate_transfers(ledger):
    client, rows, version = ledger
    first = client.post("/api/merges", json={
        "sources": [H00, H01], "target": DAY, "expected_version": version,
        "manifest": rows, "objects": rows, "merge_id": "batch-1", "batch_size": 1,
    }).json()
    assert first["merge_status"] == "RUNNING"
    second = client.post("/api/merges", json={"merge_id": "batch-1"}).json()
    assert second["merge_status"] == "COMMITTED" and second["forward_done"] == 2
    # 再试一次仍是同一份进度
    third = client.post("/api/merges", json={"merge_id": "batch-1"}).json()
    assert third["forward_done"] == 2


def test_backfilled_object_follows_own_attribution(ledger):
    client, rows, version = ledger
    merge = client.post("/api/merges", json={
        "sources": [H00, H01], "target": DAY, "expected_version": version,
        "manifest": rows, "objects": rows, "merge_id": "bf-1",
    }).json()
    late = {"key": f"{DAY}/late.parquet", "size": 1, "etag": "late"}
    client.post("/api/merges/objects", json={"objects": [late]})
    client.post("/api/merges/revert", json={
        "merge_id": "bf-1", "expected_version": merge["version"],
    })
    attrs = {a["key"]: a for a in client.get("/api/merges/attributions").json()["attributions"]}
    assert attrs[late["key"]]["partition"] == DAY  # 后补对象沿有效归属，未被撤销触碰
    assert client.get("/api/merges/diff").json()["current_total"] == 3


def test_seal_empty_partition_422(client):
    response = client.post("/api/merges/seal",
                           json={"partition": H00, "expected_version": 0})
    assert response.status_code == 422
    assert response.json()["error"] == "PARTITION_EMPTY"
