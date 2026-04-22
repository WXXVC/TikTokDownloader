import asyncio
from datetime import datetime

from fastapi import APIRouter, Query, Response, status

from ..schemas import (
    CreatorCreate,
    CreatorPage,
    CreatorQuickAddBatchResponse,
    CreatorQuickAddRequest,
    CreatorRead,
    CreatorScriptHistoryRequest,
    CreatorScriptHistoryResponse,
    CreatorScriptUpsertRequest,
    CreatorScriptUpsertResponse,
    CreatorUpdate,
)
from ..services import creators as service
from ..services import tasks as task_service
from ..services.auto_download_runtime import request_manual_creator_run


router = APIRouter(prefix="/creators", tags=["creators"])


@router.get("", response_model=CreatorPage)
def list_creators(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    keyword: str = Query(default=""),
    platform: str = Query(default=""),
    profile_id: int | None = Query(default=None),
    enabled: str = Query(default=""),
    auto_enabled: str = Query(default=""),
    download_status: str = Query(default=""),
):
    return service.list_creator_page(
        page=page,
        page_size=page_size,
        keyword=keyword,
        platform=platform,
        profile_id=profile_id,
        enabled=enabled,
        auto_enabled=auto_enabled,
        download_status=download_status,
    )


@router.get("/options")
def list_creator_options():
    return service.list_creator_options()


@router.get("/{creator_id}", response_model=CreatorRead)
def get_creator(creator_id: int):
    return service.get_creator(creator_id)


@router.post("", response_model=CreatorRead, status_code=status.HTTP_201_CREATED)
def create_creator(payload: CreatorCreate):
    return service.create_creator(payload)


@router.post(
    "/quick-add",
    response_model=CreatorQuickAddBatchResponse,
    status_code=status.HTTP_201_CREATED,
)
def quick_add_creator(payload: CreatorQuickAddRequest):
    return service.create_creator_via_quick_add(payload)


@router.post("/script-upsert", response_model=CreatorScriptUpsertResponse)
def script_upsert_creator(payload: CreatorScriptUpsertRequest):
    return service.upsert_creator_from_script(payload.model_dump(exclude_unset=True))


@router.post("/script-history", response_model=CreatorScriptHistoryResponse)
def script_creator_history(payload: CreatorScriptHistoryRequest):
    return service.list_creator_history_for_script(payload.password)


@router.put("/{creator_id}", response_model=CreatorRead)
def update_creator(creator_id: int, payload: CreatorUpdate):
    return service.update_creator(creator_id, payload)


@router.post("/{creator_id}/auto-run", response_model=CreatorRead)
async def auto_run_creator(creator_id: int):
    creator = service.get_creator(creator_id)
    service.update_auto_download_result(
        creator_id,
        status="queued",
        message="已收到手动执行请求，正在加入后台顺序队列。",
        next_run_at=datetime.now().isoformat(timespec="seconds"),
        mark_run=False,
        record_history=False,
    )
    asyncio.create_task(request_manual_creator_run(creator))
    return service.get_creator(creator_id)


@router.post("/{creator_id}/reset-schedule", response_model=CreatorRead)
def reset_creator_schedule(creator_id: int):
    return service.reset_auto_download_schedule(creator_id)


@router.post("/{creator_id}/clear-task-records")
def clear_creator_task_records(
    creator_id: int, purge_download_history: bool = Query(default=False)
):
    return task_service.clear_creator_task_records(
        creator_id,
        purge_download_history=purge_download_history,
    )


@router.post("/{creator_id}/stop-workflow")
def stop_creator_workflow(creator_id: int):
    return task_service.stop_creator_workflow(creator_id)


@router.post("/{creator_id}/delete-with-history")
def delete_creator_with_history(creator_id: int):
    return service.delete_creator_with_download_history(creator_id)


@router.delete("/{creator_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_creator(creator_id: int):
    service.delete_creator(creator_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
