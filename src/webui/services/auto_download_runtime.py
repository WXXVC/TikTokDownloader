import asyncio
import json
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx

from ..config import AUTO_DOWNLOAD_WORK_BATCH_SIZE, DATA_DIR
from ..schemas import DownloadWorksTaskCreate
from . import scans
from .auto_download_throttle import is_auto_download_paused
from .creators import get_creator, has_creator_completed_initial_scan, list_due_auto_download_creators, set_creator_initial_scan_completed, update_auto_download_result
from .engine import fetch_account_items
from .panel_config import (
    is_auto_download_scheduler_enabled,
    is_auto_download_split_batches_enabled,
    read_panel_config,
)
from .risk_guard import is_risk_guard_active
from .tasks import (
    create_auto_batch_source_task,
    create_auto_works_download_task,
    has_active_auto_task_workload,
    has_running_task_for_creator,
)


CHECK_INTERVAL_SECONDS = 45
MANUAL_QUEUE_PATH = DATA_DIR / "manual_auto_queue.json"
RUNNING_CREATORS: set[int] = set()
AUTO_DOWNLOAD_WAKE_EVENT: asyncio.Event | None = None
MANUAL_QUEUE: deque[int] = deque()
MANUAL_QUEUE_SET: set[int] = set()
MANUAL_QUEUE_LOADED = False
STOP_REQUESTED_CREATORS: set[int] = set()


def _save_manual_queue() -> None:
    payload = {
        "creator_ids": [creator_id for creator_id in MANUAL_QUEUE if int(creator_id or 0) > 0],
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    MANUAL_QUEUE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_manual_queue() -> None:
    global MANUAL_QUEUE_LOADED
    if MANUAL_QUEUE_LOADED:
        return
    MANUAL_QUEUE.clear()
    MANUAL_QUEUE_SET.clear()
    if MANUAL_QUEUE_PATH.exists():
        try:
            data = json.loads(MANUAL_QUEUE_PATH.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        for creator_id in data.get("creator_ids") or []:
            value = int(creator_id or 0)
            if value > 0 and value not in MANUAL_QUEUE_SET:
                MANUAL_QUEUE.append(value)
                MANUAL_QUEUE_SET.add(value)
    MANUAL_QUEUE_LOADED = True


def bind_auto_download_wake_event(event: asyncio.Event) -> None:
    global AUTO_DOWNLOAD_WAKE_EVENT
    AUTO_DOWNLOAD_WAKE_EVENT = event
    _load_manual_queue()


def request_auto_download_wakeup() -> None:
    if AUTO_DOWNLOAD_WAKE_EVENT:
        AUTO_DOWNLOAD_WAKE_EVENT.set()


def enqueue_manual_creator_run(creator_id: int) -> bool:
    _load_manual_queue()
    creator_id = int(creator_id or 0)
    if creator_id <= 0:
        return False
    if creator_id in MANUAL_QUEUE_SET:
        request_auto_download_wakeup()
        return False
    MANUAL_QUEUE.append(creator_id)
    MANUAL_QUEUE_SET.add(creator_id)
    _save_manual_queue()
    request_auto_download_wakeup()
    return True


def remove_manual_creator_run(creator_id: int) -> bool:
    _load_manual_queue()
    creator_id = int(creator_id or 0)
    if creator_id <= 0 or creator_id not in MANUAL_QUEUE_SET:
        return False
    remaining_ids = [int(item) for item in MANUAL_QUEUE if int(item or 0) != creator_id]
    MANUAL_QUEUE.clear()
    MANUAL_QUEUE_SET.clear()
    for item in remaining_ids:
        if item > 0 and item not in MANUAL_QUEUE_SET:
            MANUAL_QUEUE.append(item)
            MANUAL_QUEUE_SET.add(item)
    _save_manual_queue()
    request_auto_download_wakeup()
    return True


def request_stop_creator_workflow(creator_id: int) -> dict:
    creator_id = int(creator_id or 0)
    removed_from_manual_queue = remove_manual_creator_run(creator_id)
    if creator_id > 0:
        STOP_REQUESTED_CREATORS.add(creator_id)
    request_auto_download_wakeup()
    return {
        "creator_id": creator_id,
        "removed_from_manual_queue": removed_from_manual_queue,
        "was_running": creator_id in RUNNING_CREATORS,
    }


def consume_stop_request(creator_id: int) -> bool:
    creator_id = int(creator_id or 0)
    if creator_id <= 0 or creator_id not in STOP_REQUESTED_CREATORS:
        return False
    STOP_REQUESTED_CREATORS.discard(creator_id)
    return True


def clear_stop_request(creator_id: int) -> None:
    STOP_REQUESTED_CREATORS.discard(int(creator_id or 0))


def get_manual_queue_state() -> dict:
    _load_manual_queue()
    creator_ids = [int(creator_id) for creator_id in MANUAL_QUEUE if int(creator_id or 0) > 0]
    next_creator = None
    if creator_ids:
        try:
            creator = get_creator(creator_ids[0])
            next_creator = {
                "id": int(creator["id"]),
                "name": creator.get("name") or "",
                "mark": creator.get("mark") or "",
                "platform": creator.get("platform") or "",
            }
        except Exception:
            next_creator = {
                "id": int(creator_ids[0]),
                "name": "",
                "mark": "",
                "platform": "",
            }
    return {
        "count": len(creator_ids),
        "creator_ids": creator_ids,
        "next_creator": next_creator,
        "path": str(MANUAL_QUEUE_PATH),
    }


def _pop_manual_creator_id() -> int | None:
    _load_manual_queue()
    while MANUAL_QUEUE:
        creator_id = int(MANUAL_QUEUE.popleft() or 0)
        MANUAL_QUEUE_SET.discard(creator_id)
        _save_manual_queue()
        if creator_id > 0:
            return creator_id
    return None


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


def _source_item_id(item: dict) -> str:
    return str(item.get("aweme_id") or item.get("id") or "")


def _scan_creator_once(creator_id: int) -> tuple[dict, list[str], list[dict]]:
    creator = get_creator(creator_id)
    source_items = fetch_account_items(creator)
    engine_items = scans.map_engine_items(source_items)
    payload = scans.build_scan_payload(creator, engine_items, "engine_api_auto")
    work_ids = [item.id for item in payload["items"] if not item.is_downloaded]
    work_id_set = {str(item) for item in work_ids}
    filtered_source_items = [
        item for item in source_items
        if _source_item_id(item) in work_id_set
    ]
    return payload, work_ids, filtered_source_items


def _split_work_ids(work_ids: list[str]) -> list[list[str]]:
    if not is_auto_download_split_batches_enabled():
        return [list(work_ids)] if work_ids else []
    config = read_panel_config()
    size = max(
        1,
        int(config.get("auto_download_work_batch_size") or AUTO_DOWNLOAD_WORK_BATCH_SIZE),
    )
    return [work_ids[index:index + size] for index in range(0, len(work_ids), size)]


async def request_manual_creator_run(creator: dict) -> None:
    creator_id = int(creator["id"])
    clear_stop_request(creator_id)
    if creator_id in RUNNING_CREATORS:
        await asyncio.to_thread(
            update_auto_download_result,
            creator_id,
            status="queued",
            message="该账号正在处理中，请等待当前这一轮完成。",
            next_run_at=datetime.now().isoformat(timespec="seconds"),
            mark_run=False,
            record_history=False,
        )
        return
    if await asyncio.to_thread(has_running_task_for_creator, creator_id):
        await asyncio.to_thread(
            update_auto_download_result,
            creator_id,
            status="running",
            message="当前账号已经有自动下载任务在执行，正在等待这一轮完成。",
            next_run_at=_next_run_after(creator),
            mark_run=False,
            record_history=False,
        )
        return

    queued_now = await asyncio.to_thread(enqueue_manual_creator_run, creator_id)
    has_other_active_workload = await asyncio.to_thread(has_active_auto_task_workload)
    if queued_now:
        message = (
            "已收到手动执行请求，已加入手动顺序队列。当前任务完成后会自动开始扫描和下载。"
            if has_other_active_workload or len(RUNNING_CREATORS) > 0
            else "已收到手动执行请求，正在等待调度器启动扫描和下载。"
        )
    else:
        message = "该账号已在手动执行队列中，正在等待后台继续处理。"

    await asyncio.to_thread(
        update_auto_download_result,
        creator_id,
        status="queued",
        message=message,
        next_run_at=datetime.now().isoformat(timespec="seconds"),
        mark_run=False,
        record_history=False,
    )


async def run_once_for_creator(creator: dict, *, force: bool = False) -> None:
    creator_id = int(creator["id"])
    if creator_id in RUNNING_CREATORS:
        return
    if not force and not await asyncio.to_thread(is_auto_download_scheduler_enabled):
        return
    if await asyncio.to_thread(is_auto_download_paused) or await asyncio.to_thread(is_risk_guard_active):
        return
    RUNNING_CREATORS.add(creator_id)
    try:
        if consume_stop_request(creator_id):
            await asyncio.to_thread(
                update_auto_download_result,
                creator_id,
                status="stopped",
                message="已停止当前账号任务，本轮不会继续扫描或入队下载。",
                next_run_at=_next_run_after(creator),
                mark_run=False,
                record_history=False,
            )
            return
        if await asyncio.to_thread(has_running_task_for_creator, creator_id):
            await asyncio.to_thread(
                update_auto_download_result,
                creator_id,
                status="skipped",
                message="当前账号已有正在运行的自动下载任务，本轮跳过。",
                next_run_at=_next_run_after(creator),
                mark_run=False,
            )
            return
        if await asyncio.to_thread(has_active_auto_task_workload, creator_id):
            if force:
                await asyncio.to_thread(enqueue_manual_creator_run, creator_id)
                await asyncio.to_thread(
                    update_auto_download_result,
                    creator_id,
                    status="queued",
                    message="当前有其他账号正在自动下载，已加入手动顺序队列，上一账号完成后会自动开始。",
                    next_run_at=datetime.now().isoformat(timespec="seconds"),
                    mark_run=False,
                    record_history=False,
                )
            return

        await asyncio.to_thread(
            update_auto_download_result,
            creator_id,
            status="scanning",
            message="正在扫描账号作品并比对数据库中的已下载记录，请稍候。",
            next_run_at=_next_run_after(creator),
            mark_run=False,
            record_history=False,
        )
        payload, work_ids, filtered_source_items = await asyncio.to_thread(_scan_creator_once, creator_id)
        creator = await asyncio.to_thread(get_creator, creator_id)
        if consume_stop_request(creator_id):
            await asyncio.to_thread(
                update_auto_download_result,
                creator_id,
                status="stopped",
                message="已收到结束任务请求；当前扫描结果已丢弃，不会继续为该账号入队下载任务。",
                next_run_at=_next_run_after(creator),
                mark_run=False,
                record_history=False,
            )
            return
        if not work_ids:
            if not has_creator_completed_initial_scan(creator):
                await asyncio.to_thread(set_creator_initial_scan_completed, creator_id, True)
            await asyncio.to_thread(
                update_auto_download_result,
                creator_id,
                status="idle",
                message=f"扫描完成，未发现新作品。总数 {payload['all_count']}，已下载 {payload['downloaded_count']}。",
                next_run_at=_next_run_after(creator),
            )
            return

        split_batches_enabled = is_auto_download_split_batches_enabled()
        batches = _split_work_ids(work_ids)
        session_id = uuid4().hex
        queue_message = (
            f"扫描完成，发现 {len(work_ids)} 个待下载作品，正在拆分为 {len(batches)} 个顺序批次并加入下载队列。"
            if split_batches_enabled
            else f"扫描完成，发现 {len(work_ids)} 个待下载作品，正在按单批作品 ID 下载模式加入下载队列。"
        )
        await asyncio.to_thread(
            update_auto_download_result,
            creator_id,
            status="queueing",
            message=queue_message,
            next_run_at=_next_run_after(creator),
            mark_run=False,
            record_history=False,
        )

        for batch_work_ids in batches:
            if split_batches_enabled:
                await asyncio.to_thread(
                    create_auto_works_download_task,
                    DownloadWorksTaskCreate(creator_id=creator_id, work_ids=batch_work_ids),
                    session_id=session_id,
                    allow_when_scheduler_disabled=force,
                )
            else:
                await asyncio.to_thread(
                    create_auto_batch_source_task,
                    creator_id=creator_id,
                    source_items=list(filtered_source_items),
                    work_ids=list(batch_work_ids),
                    session_id=session_id,
                    allow_when_scheduler_disabled=force,
                )

        if split_batches_enabled:
            finish_message = (
                f"当前账号扫描完成，新增待下载作品 {len(work_ids)} 个，"
                f"已拆分为 {len(batches)} 个顺序批次，当前账号完成前不会切换到下一个账号。"
            )
            finish_status = "running" if len(batches) > 1 else "scheduled"
        else:
            finish_message = (
                f"当前账号扫描完成，新增待下载作品 {len(work_ids)} 个，"
                "已按单批作品 ID 下载模式入队，不再重复整号扫描，当前账号完成前不会切换到下一个账号。"
            )
            finish_status = "scheduled"

        await asyncio.to_thread(
            update_auto_download_result,
            creator_id,
            status=finish_status,
            message=finish_message,
            next_run_at=_next_run_after(creator),
        )
    except Exception as error:
        error_message = (
            "自动下载失败：扫描账号作品时请求引擎超时，请稍后重试或继续增大账号扫描超时时间。"
            if isinstance(error, httpx.TimeoutException)
            else f"自动下载失败：{error}"
        )
        await asyncio.to_thread(
            update_auto_download_result,
            creator_id,
            status="failed",
            message=error_message,
            next_run_at=_next_run_after(creator),
        )
    finally:
        RUNNING_CREATORS.discard(creator_id)


async def _wait_for_next_cycle(stop_event: asyncio.Event) -> None:
    wake_event = AUTO_DOWNLOAD_WAKE_EVENT
    if wake_event is None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
        return
    stop_task = asyncio.create_task(stop_event.wait())
    wake_task = asyncio.create_task(wake_event.wait())
    try:
        _done, pending = await asyncio.wait(
            {stop_task, wake_task},
            timeout=CHECK_INTERVAL_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        if wake_event.is_set():
            wake_event.clear()


async def scheduler_loop(stop_event: asyncio.Event) -> None:
    _load_manual_queue()
    while not stop_event.is_set():
        if await asyncio.to_thread(has_active_auto_task_workload):
            await _wait_for_next_cycle(stop_event)
            continue

        if MANUAL_QUEUE:
            manual_creator_id = _pop_manual_creator_id()
            if manual_creator_id:
                try:
                    creator = await asyncio.to_thread(get_creator, manual_creator_id)
                except Exception:
                    creator = None
                if creator:
                    await run_once_for_creator(creator, force=True)
                    continue

        if not await asyncio.to_thread(is_auto_download_scheduler_enabled):
            await _wait_for_next_cycle(stop_event)
            continue
        if await asyncio.to_thread(is_auto_download_paused) or await asyncio.to_thread(is_risk_guard_active):
            await _wait_for_next_cycle(stop_event)
            continue

        now = datetime.now()
        due_creators = [
            creator
            for creator in await asyncio.to_thread(
                list_due_auto_download_creators,
                now.isoformat(timespec="seconds"),
                1,
            )
            if int(creator["id"]) not in RUNNING_CREATORS
        ]

        if due_creators:
            creator = due_creators[0]
            if not await asyncio.to_thread(is_auto_download_paused) and not await asyncio.to_thread(is_risk_guard_active):
                await run_once_for_creator(creator)
                continue
        await _wait_for_next_cycle(stop_event)
