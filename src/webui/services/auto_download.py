import asyncio
from datetime import datetime, timedelta

from ..config import AUTO_DOWNLOAD_MAX_CONCURRENCY
from ..schemas import DownloadWorksTaskCreate
from . import scans
from .creators import get_creator, list_creators, update_auto_download_result
from .engine import fetch_account_items
from .tasks import create_auto_works_download_task, has_running_task_for_creator


CHECK_INTERVAL_SECONDS = 45
RUNNING_CREATORS: set[int] = set()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _next_run_after(creator: dict, base: datetime | None = None) -> datetime | None:
    interval = max(0, int(creator.get("auto_download_interval_minutes") or 0))
    if interval <= 0 or not creator.get("auto_download_enabled"):
        return None
    start_at = _parse_dt(creator.get("auto_download_start_at"))
    anchor = base or datetime.now()
    if start_at and start_at > anchor:
        return start_at
    return anchor + timedelta(minutes=interval)


def _scan_creator_once(creator_id: int) -> tuple[dict, list[str]]:
    creator = get_creator(creator_id)
    engine_items = scans.map_engine_items(fetch_account_items(creator))
    payload = scans.build_scan_payload(creator, engine_items, "engine_api_auto")
    work_ids = [item.id for item in payload["items"] if not item.is_downloaded]
    return payload, work_ids


async def run_once_for_creator(creator: dict) -> None:
    creator_id = creator["id"]
    if creator_id in RUNNING_CREATORS:
        return
    RUNNING_CREATORS.add(creator_id)
    try:
        if has_running_task_for_creator(creator_id):
            update_auto_download_result(
                creator_id,
                status="skipped",
                message="已有运行中的任务，跳过本轮自动下载。",
                next_run_at=_next_run_after(creator),
                mark_run=False,
            )
            return

        creator = get_creator(creator_id)
        engine_items = scans.map_engine_items(fetch_account_items(creator))
        payload = scans.build_scan_payload(creator, engine_items, "engine_api_auto")
        work_ids = [item.id for item in payload["items"] if not item.is_downloaded]
        if not work_ids:
            update_auto_download_result(
                creator_id,
                status="idle",
                message=f"扫描完成，未发现新作品。总数 {payload['all_count']}，已下载 {payload['downloaded_count']}。",
                next_run_at=_next_run_after(creator),
            )
            return

        create_auto_works_download_task(
            DownloadWorksTaskCreate(creator_id=creator_id, work_ids=work_ids)
        )
        update_auto_download_result(
            creator_id,
            status="scheduled",
            message=f"扫描完成，新增待下载作品 {len(work_ids)} 个，已加入任务队列。",
            next_run_at=_next_run_after(creator),
        )
    except Exception as error:
        update_auto_download_result(
            creator_id,
            status="failed",
            message=f"自动下载失败：{error}",
            next_run_at=_next_run_after(creator),
        )
    finally:
        RUNNING_CREATORS.discard(creator_id)


async def scheduler_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        now = datetime.now()
        for creator in list_creators():
            if not creator.get("enabled") or not creator.get("auto_download_enabled"):
                continue
            next_run_at = _parse_dt(creator.get("auto_download_next_run_at"))
            if next_run_at and next_run_at > now:
                continue
            await run_once_for_creator(creator)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
