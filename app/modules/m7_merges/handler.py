from fastapi import APIRouter, Body, Request

from .service import MergeService

router = APIRouter(tags=["M7-分区合并"])


def service(request: Request) -> MergeService:
    return request.app.state.merge_service


@router.get("/merges")
def page(request: Request):
    data = service(request).diff()
    data["sample_merge"] = {
        "sources": [
            "dt=2026-11-15/hour=00",
            "dt=2026-11-15/hour=01",
        ],
        "target": "dt=2026-11-15",
        "expected_version": data["version"],
    }
    return request.app.state.templates.TemplateResponse(
        request, "m7_merges.html", {"title": "分区合并", "data": data}
    )


@router.post("/api/merges/objects")
def register(request: Request, payload: dict = Body(default={})):
    """接入现存对象；后补对象沿自身有效归属登记，不做补数领取。"""
    return service(request).register_objects(payload)


@router.post("/api/merges/seal")
def seal(request: Request, payload: dict = Body(...)):
    """封账：责任数量固定在原小时分区，后续合并只能保留不能搬走。"""
    return service(request).seal_partition(payload)


@router.post("/api/merges")
def merge(request: Request, payload: dict = Body(...)):
    """小时分区合并为日分区；同 merge_id 重试走恢复，不重复转账。"""
    return service(request).merge(payload)


@router.post("/api/merges/revert")
def revert(request: Request, payload: dict = Body(...)):
    """撤销合并：按对象退回原分区；封账项与后补对象不动。"""
    return service(request).revert(payload)


@router.get("/api/merges/progress")
def progress(request: Request, merge_id: str):
    """回读合并进度。"""
    return service(request).progress({"merge_id": merge_id})


@router.get("/api/merges/diff")
def diff(request: Request):
    """回读当前归属与封账差异，附总量守恒口径。"""
    return service(request).diff()


@router.get("/api/merges/attributions")
def attributions(request: Request):
    """回读现存对象归属账本。"""
    return service(request).attributions()


@router.get("/api/merges/list")
def list_merges(request: Request):
    """回读全部合并单。"""
    return service(request).list_merges()
