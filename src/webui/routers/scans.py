from fastapi import APIRouter, Query

from ..schemas import ScanPageResponse
from ..services import scans as service


router = APIRouter(prefix="/scans", tags=["scans"])


@router.post("/creator/{creator_id}", response_model=ScanPageResponse)
def scan_creator(
    creator_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=6, ge=1, le=200),
    keyword: str = Query(default=""),
    item_type: str = Query(default="", alias="type"),
    show_downloaded: bool = Query(default=False),
):
    return service.scan_creator_works(
        creator_id,
        page=page,
        page_size=page_size,
        keyword=keyword,
        item_type=item_type,
        show_downloaded=show_downloaded,
    )


@router.get("/creator/{creator_id}/latest", response_model=ScanPageResponse)
def latest_scan(
    creator_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=6, ge=1, le=200),
    keyword: str = Query(default=""),
    item_type: str = Query(default="", alias="type"),
    show_downloaded: bool = Query(default=False),
):
    return service.latest_scan(
        creator_id,
        page=page,
        page_size=page_size,
        keyword=keyword,
        item_type=item_type,
        show_downloaded=show_downloaded,
    )
