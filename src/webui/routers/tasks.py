from fastapi import APIRouter, Query, status

from ..schemas import DownloadTaskCreate, DownloadTaskPage, DownloadTaskRead, DownloadWorksTaskCreate, TaskCenterSummaryPage
from ..services import tasks as service


router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("", response_model=DownloadTaskPage)
def list_tasks(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=200),
    keyword: str = Query(default=""),
    status: str = Query(default=""),
    mode: str = Query(default=""),
    kind: str = Query(default=""),
):
    return service.list_task_page(
        page=page,
        page_size=page_size,
        keyword=keyword,
        status=status,
        mode=mode,
        kind=kind,
    )


@router.get("/summary", response_model=TaskCenterSummaryPage)
def list_task_center_summary(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=200),
    keyword: str = Query(default=""),
):
    return service.list_task_center_summary_page(
        page=page,
        page_size=page_size,
        keyword=keyword,
    )


@router.get("/{task_id}", response_model=DownloadTaskRead)
def get_task(task_id: int):
    return service.get_task(task_id)


@router.post("", response_model=DownloadTaskRead, status_code=status.HTTP_201_CREATED)
def create_task(payload: DownloadTaskCreate):
    return service.create_download_task(payload)


@router.post("/works", response_model=DownloadTaskRead, status_code=status.HTTP_201_CREATED)
def create_works_task(payload: DownloadWorksTaskCreate):
    return service.create_works_download_task(payload)


@router.post("/{task_id}/stop", response_model=DownloadTaskRead)
def stop_task(task_id: int):
    return service.stop_task(task_id)
