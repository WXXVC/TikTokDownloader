import asyncio
import contextlib
import os
import subprocess
import sys
import threading
import json
import shutil
import sqlite3
from pathlib import Path
from shutil import copy2
from datetime import datetime, timedelta

from fastapi import HTTPException

from ..config import (
    ENGINE_PROJECT_ROOT,
    ENGINE_DB_PATH,
    ENGINE_VOLUME_PATH,
    TASK_DISPATCH_INTERVAL_SECONDS,
    TASK_LOG_DIR,
    TASK_RUNTIME_DIR,
    TASK_RUNTIME_CLEANUP_INTERVAL_SECONDS,
    TASK_RUNTIME_RETENTION_SECONDS,
    WORKER_ISOLATED_DETAIL_PATH,
    WORKER_ISOLATED_BATCH_PATH,
    WORKER_ISOLATED_MAIN_PATH,
)
from ..db import get_sqlite_task, list_sqlite_scan_cache, list_sqlite_tasks, next_sqlite_task_id, now_iso, save_sqlite_task
from ..db import count_sqlite_tasks, delete_sqlite_scan_cache_for_creator, delete_sqlite_tasks_by_creator, list_sqlite_task_summaries, list_sqlite_task_summaries_paginated, list_sqlite_tasks_by_statuses
from ..schemas import DownloadTaskCreate, DownloadWorksTaskCreate
from .auto_download_throttle import is_auto_download_paused, record_auto_download_progress
from .creators import clear_auto_download_runtime_state, collect_creator_work_ids_for_purge, get_creator, list_creators, set_creator_initial_scan_completed, update_auto_download_result
from .engine import (
    build_run_command,
    build_settings_payload,
    delete_downloaded_ids,
    fetch_account_items,
    normalize_download_root,
    read_downloaded_ids,
    read_engine_settings,
)
from .panel_config import is_auto_download_scheduler_enabled
from .panel_config import (
    get_detail_fetch_concurrency,
    get_file_download_max_workers,
)
from .profiles import get_profile
from .risk_guard import (
    assess_low_quality_items,
    detect_empty_download,
    extract_status_codes,
    is_risk_guard_active,
    record_empty_download_signal,
    record_http_error_signal,
    record_low_quality_signal,
)
from .scans import build_scan_payload, find_scan_items_by_work_ids, infer_creator_folder_name, map_engine_items


PROCESS_REGISTRY: dict[int, dict] = {}
TASK_LAUNCH_LOCK = threading.Lock()
TERMINAL_TASK_STATUSES = {"success", "failed", "stopped"}
ACTIVE_AUTO_TASK_STATUSES = {"queued", "running"}
AUTO_TASK_PARALLELISM = 1
AUTO_TASK_MODES = {"auto_detail_download", "auto_creator_batch_download"}


def has_running_task_for_creator(creator_id: int) -> bool:
    running_count = count_sqlite_tasks(creator_id=creator_id, status="running")
    if running_count:
        for task in list_sqlite_tasks_by_statuses(
            creator_id=creator_id,
            statuses=("running",),
        ):
            if _task_status(task)["status"] == "running":
                return True
    return count_sqlite_tasks(creator_id=creator_id, status="queued") > 0


def refresh_active_task_statuses() -> None:
    for task in list_sqlite_tasks_by_statuses(
        statuses=tuple(ACTIVE_AUTO_TASK_STATUSES),
    ):
        _task_status(task)


def count_running_auto_tasks() -> int:
    for task in list_sqlite_tasks_by_statuses(statuses=("running",)):
        if task.get("mode") not in AUTO_TASK_MODES:
            continue
        _task_status(task)
    return sum(
        1
        for task in list_sqlite_tasks_by_statuses(statuses=("running",))
        if _task_status(task).get("status") == "running" and task.get("mode") in AUTO_TASK_MODES
    )


def has_active_auto_task_workload(exclude_creator_id: int | None = None) -> bool:
    for task in list_sqlite_tasks_by_statuses(statuses=tuple(ACTIVE_AUTO_TASK_STATUSES)):
        if task.get("mode") not in AUTO_TASK_MODES:
            continue
        current = _task_status(task)
        if current.get("status") not in ACTIVE_AUTO_TASK_STATUSES:
            continue
        creator_id = int(current.get("creator_id") or 0)
        if exclude_creator_id is not None and creator_id == exclude_creator_id:
            continue
        return True
    return False


def _list_auto_session_tasks(creator_id: int, session_id: str) -> list[dict]:
    return [
        task
        for task in list_sqlite_tasks_by_statuses(creator_id=creator_id)
        if task.get("mode") in AUTO_TASK_MODES
        if str(task.get("auto_session_id") or "").strip() == session_id
    ]


def _summarize_auto_session(task: dict) -> dict | None:
    creator_id = int(task.get("creator_id") or 0)
    session_id = str(task.get("auto_session_id") or "").strip()
    if not creator_id or not session_id:
        return None

    session_tasks = _list_auto_session_tasks(creator_id, session_id)
    task_id = int(task.get("id") or 0)
    if task_id:
        session_tasks = [
            task if int(item.get("id") or 0) == task_id else item
            for item in session_tasks
        ]
    if not session_tasks:
        return None

    downloaded_ids = read_downloaded_ids(force_refresh=True)
    pending_tasks = [
        item for item in session_tasks
        if str(item.get("status") or "") in ACTIVE_AUTO_TASK_STATUSES
    ]
    completed_tasks = [
        item for item in session_tasks
        if str(item.get("status") or "") in TERMINAL_TASK_STATUSES
    ]
    failed_tasks = [
        item for item in completed_tasks
        if str(item.get("status") or "") != "success"
    ]
    all_work_ids: set[str] = set()
    downloaded_work_ids: set[str] = set()
    for item in session_tasks:
        work_ids = {
            str(work_id)
            for work_id in (item.get("work_ids") or [])
            if str(work_id or "").strip()
        }
        all_work_ids.update(work_ids)
        if str(item.get("status") or "") == "success":
            downloaded_work_ids.update(work_id for work_id in work_ids if work_id in downloaded_ids)

    return {
        "total_batches": len(session_tasks),
        "completed_batches": len(completed_tasks),
        "pending_batches": len(pending_tasks),
        "failed_batches": len(failed_tasks),
        "total_works": len(all_work_ids),
        "downloaded_works": len(downloaded_work_ids),
    }


def _task_status(task: dict) -> dict:
    entry = PROCESS_REGISTRY.get(task["id"])
    process = entry["process"] if entry else None
    if process and task["status"] == "running":
        code = process.poll()
        if code is None:
            return task
        task["status"] = "success" if code == 0 else "failed"
        task["message"] = (
            "Downloader process finished successfully"
            if code == 0
            else f"Downloader process exited with code {code}"
        )
        task["exit_code"] = code
        _evaluate_completed_task_risk(task)
        _record_completed_auto_download_progress(task)
        _mark_creator_initial_scan_completed_for_task(task)
        _sync_creator_auto_download_status(task)
        task["updated_at"] = now_iso()
        save_sqlite_task(task)
        _finalize_registry_entry(task["id"])
    return task


def list_tasks():
    refresh_active_task_statuses()
    return [_task_status(task) for task in list_sqlite_tasks()]


def list_task_summaries():
    return list_sqlite_task_summaries()


def list_task_page(
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    status: str = "",
    mode: str = "",
    kind: str = "",
):
    refresh_active_task_statuses()
    return list_sqlite_task_summaries_paginated(
        page=page,
        page_size=page_size,
        keyword=keyword,
        status=status,
        mode=mode,
        kind=kind,
    )


def list_task_center_summary_page(
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    status: str = "",
    mode: str = "",
    kind: str = "",
):
    refresh_active_task_statuses()
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 10)))
    creators = list_creators()
    tasks = list_sqlite_tasks()
    downloaded_ids = read_downloaded_ids(force_refresh=True)

    def classify_work_type(value: str) -> str:
        text = str(value or "").strip().lower()
        if "live" in text or "实况" in text:
            return "live"
        if "collection" in text or "图集" in text:
            return "collection"
        return "video"

    work_type_by_creator: dict[int, dict[str, str]] = {}
    for creator in creators:
        creator_id = int(creator["id"])
        work_type_map: dict[str, str] = {}
        for row in list_sqlite_scan_cache(creator_id):
            for item in row.get("payload") or []:
                work_id = str(item.get("id") or "").strip()
                if not work_id:
                    continue
                work_type_map[work_id] = classify_work_type(item.get("type") or "")
        work_type_by_creator[creator_id] = work_type_map

    summaries: dict[int, dict] = {
        int(creator["id"]): {
            "creator_id": int(creator["id"]),
            "creator_name": creator.get("name") or "",
            "platform": creator.get("platform") or "",
            "mark": creator.get("mark") or "",
            "video_ids": set(),
            "collection_ids": set(),
            "live_ids": set(),
            "failed_count": 0,
            "last_download_at": None,
        }
        for creator in creators
    }

    status_text = str(status or "").strip().lower()
    mode_text = str(mode or "").strip()
    kind_text = str(kind or "").strip().lower()

    for task in tasks:
        creator_id = int(task.get("creator_id") or 0)
        if not creator_id:
            continue
        task_status = str(task.get("status") or "").strip().lower()
        task_mode = str(task.get("mode") or "").strip()
        is_auto_task = task_mode in AUTO_TASK_MODES
        if status_text and task_status != status_text:
            continue
        if mode_text and task_mode != mode_text:
            continue
        if kind_text == "auto" and not is_auto_task:
            continue
        if kind_text == "manual" and is_auto_task:
            continue
        summary = summaries.setdefault(
            creator_id,
            {
                "creator_id": creator_id,
                "creator_name": task.get("creator_name") or f"账号 {creator_id}",
                "platform": task.get("platform") or "",
                "mark": "",
                "video_ids": set(),
                "collection_ids": set(),
                "live_ids": set(),
                "failed_count": 0,
                "last_download_at": None,
            },
        )

        if task.get("status") == "failed":
            summary["failed_count"] += 1

        work_ids = [str(item) for item in (task.get("work_ids") or []) if item]
        if not work_ids:
            continue

        success_work_ids = [work_id for work_id in work_ids if work_id in downloaded_ids]
        if not success_work_ids:
            continue

        updated_at = task.get("updated_at") or task.get("created_at")
        work_type_map = work_type_by_creator.setdefault(creator_id, {})
        matched_items = find_scan_items_by_work_ids(creator_id, success_work_ids)
        for item in matched_items:
            if item.get("id"):
                work_type_map[str(item["id"])] = classify_work_type(item.get("type") or "")
        for work_id in success_work_ids:
            item_type = classify_work_type(work_type_map.get(work_id, ""))
            if item_type == "collection":
                summary["collection_ids"].add(work_id)
            elif item_type == "live":
                summary["live_ids"].add(work_id)
            else:
                summary["video_ids"].add(work_id)
        if updated_at and (summary["last_download_at"] is None or str(updated_at) > str(summary["last_download_at"])):
            summary["last_download_at"] = updated_at

    items = []
    keyword_text = str(keyword or "").strip().lower()
    for summary in summaries.values():
        creator_name = summary["creator_name"]
        mark = summary["mark"]
        if keyword_text and keyword_text not in f"{creator_name} {mark} {summary['platform']}".lower():
            continue
        items.append(
            {
                "creator_id": summary["creator_id"],
                "creator_name": creator_name,
                "platform": summary["platform"],
                "mark": mark,
                "video_count": len(summary["video_ids"]),
                "collection_count": len(summary["collection_ids"]),
                "live_count": len(summary["live_ids"]),
                "failed_count": int(summary["failed_count"] or 0),
                "last_download_at": summary["last_download_at"],
            }
        )

    items.sort(key=lambda item: ((item["last_download_at"] or ""), item["creator_id"]), reverse=True)
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": items[start:end],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


def _list_pending_work_ids_for_creator(creator: dict) -> list[str]:
    try:
        engine_items = map_engine_items(fetch_account_items(creator))
        payload = build_scan_payload(creator, engine_items, "engine_api_batch_preview")
        return [item.id for item in payload["items"] if not item.is_downloaded]
    except Exception:
        return []


def list_running_task_cards() -> list[dict]:
    downloaded_ids = read_downloaded_ids(force_refresh=True)
    items: list[dict] = []
    auto_groups: dict[tuple[int, str], dict] = {}
    for task in list_sqlite_tasks_by_statuses(statuses=("queued", "running"), order_asc=False):
        current = _task_status(task)
        status = str(current.get("status") or "")
        if status not in {"queued", "running"}:
            continue
        if current.get("mode") in AUTO_TASK_MODES:
            session_id = str(current.get("auto_session_id") or "").strip() or f"creator:{current.get('creator_id')}"
            group_key = (int(current.get("creator_id") or 0), session_id)
            group = auto_groups.setdefault(
                group_key,
                {
                    "task_id": int(current["id"]),
                    "creator_id": int(current["creator_id"]),
                    "creator_name": current.get("creator_name") or f"璐﹀彿 {current['creator_id']}",
                    "platform": current.get("platform") or "",
                    "mode": current.get("mode") or "",
                    "work_ids": set(),
                    "target_folder_name": current.get("target_folder_name") or "",
                    "message": current.get("message") or "",
                },
            )
            group["task_id"] = max(int(group["task_id"]), int(current["id"]))
            group["work_ids"].update(
                str(work_id) for work_id in (current.get("work_ids") or []) if str(work_id or "").strip()
            )
            if status == "running":
                group["message"] = current.get("message") or group["message"]
                group["target_folder_name"] = current.get("target_folder_name") or group["target_folder_name"]
            continue
        work_ids = [str(item) for item in (current.get("work_ids") or []) if item]
        total_count = max(0, int(current.get("item_count") or 0))
        if work_ids:
            total_count = max(total_count, len(work_ids))
            completed_count = sum(1 for work_id in work_ids if work_id in downloaded_ids)
        else:
            completed_count = 0
        progress_percent = min(100, int((completed_count / total_count) * 100)) if total_count > 0 else 0
        items.append(
            {
                "task_id": int(current["id"]),
                "creator_id": int(current["creator_id"]),
                "creator_name": current.get("creator_name") or f"账号 {current['creator_id']}",
                "platform": current.get("platform") or "",
                "mode": current.get("mode") or "",
                "total_count": total_count,
                "completed_count": completed_count,
                "progress_percent": progress_percent,
                "target_folder_name": current.get("target_folder_name") or "",
                "message": current.get("message") or "",
            }
        )
    for group in auto_groups.values():
        work_ids = list(group["work_ids"])
        total_count = len(work_ids)
        completed_count = sum(1 for work_id in work_ids if work_id in downloaded_ids)
        progress_percent = min(100, int((completed_count / total_count) * 100)) if total_count > 0 else 0
        items.append(
            {
                "task_id": int(group["task_id"]),
                "creator_id": int(group["creator_id"]),
                "creator_name": group["creator_name"],
                "platform": group["platform"],
                "mode": group["mode"],
                "total_count": total_count,
                "completed_count": completed_count,
                "progress_percent": progress_percent,
                "target_folder_name": group["target_folder_name"],
                "message": group["message"] or "Auto download queued",
            }
        )
    items.sort(key=lambda item: int(item.get("task_id") or 0), reverse=True)
    return items


def get_task(task_id: int):
    task = get_sqlite_task(task_id)
    if task:
        current = _task_status(task)
        stdout_path = str(current.get("stdout_log") or "")
        stderr_path = str(current.get("stderr_log") or "")
        current["stdout_log_path"] = stdout_path
        current["stderr_log_path"] = stderr_path
        current["stdout_log"] = _read_log_text(stdout_path)
        current["stderr_log"] = _read_log_text(stderr_path)
        return current
    raise HTTPException(status_code=404, detail="Task not found")


def _finalize_registry_entry(task_id: int) -> None:
    entry = PROCESS_REGISTRY.pop(task_id, None)
    if not entry:
        return
    for handle_name in ("stdout", "stderr"):
        handle = entry.get(handle_name)
        if handle and not handle.closed:
            handle.close()


def _read_log_text(path_value: str) -> str:
    if not path_value:
        return ""
    path = Path(path_value)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def _evaluate_completed_task_risk(task: dict) -> None:
    stdout_text = _read_log_text(task.get("stdout_log", ""))
    stderr_text = _read_log_text(task.get("stderr_log", ""))
    raw_text = f"{stdout_text}\n{stderr_text}"
    status_codes = extract_status_codes(raw_text)
    record_http_error_signal(status_codes, raw_text)
    record_empty_download_signal(detect_empty_download(raw_text))


def _record_completed_auto_download_progress(task: dict) -> None:
    if task.get("mode") not in AUTO_TASK_MODES or task.get("status") != "success":
        return
    work_ids = [str(item) for item in (task.get("work_ids") or []) if item]
    if not work_ids:
        return
    downloaded_ids = read_downloaded_ids(force_refresh=True)
    success_count = sum(1 for work_id in work_ids if work_id in downloaded_ids)
    if success_count <= 0:
        return
    throttle_state = record_auto_download_progress(
        creators_count=1,
        works_count=success_count,
    )
    if throttle_state.get("last_reason"):
        task["message"] = f"{task.get('message') or 'Downloader process finished successfully'} {throttle_state['last_reason']}".strip()


def _mark_creator_initial_scan_completed_for_task(task: dict) -> None:
    if task.get("status") != "success":
        return
    if task.get("mode") not in {"creator_batch_download", "auto_creator_batch_download", "auto_detail_download"}:
        return
    creator_id = int(task.get("creator_id") or 0)
    if creator_id <= 0:
        return
    try:
        set_creator_initial_scan_completed(creator_id, True)
    except HTTPException:
        return


def _sync_creator_auto_download_status_legacy(task: dict) -> None:
    # Deprecated legacy stub kept only to avoid editing through historical mojibake content.
    return
    if task.get("mode") not in AUTO_TASK_MODES:
        return
    creator_id = int(task.get("creator_id") or 0)
    if not creator_id:
        return
    try:
        creator = get_creator(creator_id)
    except HTTPException:
        return

    next_run_at = creator.get("auto_download_next_run_at")
    session_summary = _summarize_auto_session(task)
    if session_summary and task.get("mode") == "auto_detail_download":
        if session_summary["pending_batches"] > 0:
            update_auto_download_result(
                creator_id,
                status="running",
                message=(
                    f"当前账号仍在顺序下载中，已完成批次 "
                    f"{session_summary['completed_batches']}/{session_summary['total_batches']}，"
                    f"作品 {session_summary['downloaded_works']}/{session_summary['total_works']}。"
                ),
                next_run_at=next_run_at,
                mark_run=False,
                record_history=False,
            )
            return

        final_status = "failed" if session_summary["failed_batches"] > 0 else "success"
        final_message = (
            f"当前账号自动下载结束，共 {session_summary['total_batches']} 个批次，"
            f"失败 {session_summary['failed_batches']} 个，成功下载作品 "
            f"{session_summary['downloaded_works']}/{session_summary['total_works']}。"
            if final_status == "failed"
            else (
                f"当前账号自动下载完成，共 {session_summary['total_batches']} 个批次，"
                f"成功下载作品 {session_summary['downloaded_works']}/{session_summary['total_works']}。"
            )
        )
        update_auto_download_result(
            creator_id,
            status=final_status,
            message=final_message,
            next_run_at=next_run_at,
            mark_run=False,
        )
        return
    work_ids = [str(item) for item in (task.get("work_ids") or []) if item]
    if task.get("status") == "success":
        downloaded_ids = read_downloaded_ids(force_refresh=True)
        success_count = sum(1 for work_id in work_ids if work_id in downloaded_ids)
        message = (
            f"自动下载完成，成功下载 {success_count}/{len(work_ids)} 个作品。"
            if work_ids
            else "自动下载完成。"
        )
        update_auto_download_result(
            creator_id,
            status="success",
            message=message,
            next_run_at=next_run_at,
            mark_run=False,
        )
        return

    update_auto_download_result(
        creator_id,
        status="failed",
        message=task.get("message") or "自动下载任务失败",
        next_run_at=next_run_at,
        mark_run=False,
    )


def _sync_creator_auto_download_status(task: dict) -> None:
    if task.get("mode") not in AUTO_TASK_MODES:
        return
    creator_id = int(task.get("creator_id") or 0)
    if not creator_id:
        return
    try:
        creator = get_creator(creator_id)
    except HTTPException:
        return

    next_run_at = creator.get("auto_download_next_run_at")
    session_summary = _summarize_auto_session(task)
    if session_summary and task.get("mode") == "auto_detail_download":
        if session_summary["pending_batches"] > 0:
            update_auto_download_result(
                creator_id,
                status="running",
                message=(
                    f"当前账号仍在顺序下载中，已完成批次 "
                    f"{session_summary['completed_batches']}/{session_summary['total_batches']}，"
                    f"作品 {session_summary['downloaded_works']}/{session_summary['total_works']}。"
                ),
                next_run_at=next_run_at,
                mark_run=False,
                record_history=False,
            )
            return

        final_status = "failed" if session_summary["failed_batches"] > 0 else "success"
        final_message = (
            f"当前账号自动下载结束，共 {session_summary['total_batches']} 个批次，"
            f"失败 {session_summary['failed_batches']} 个，成功下载作品 "
            f"{session_summary['downloaded_works']}/{session_summary['total_works']}。"
            if final_status == "failed"
            else (
                f"当前账号自动下载完成，共 {session_summary['total_batches']} 个批次，"
                f"成功下载作品 {session_summary['downloaded_works']}/{session_summary['total_works']}。"
            )
        )
        update_auto_download_result(
            creator_id,
            status=final_status,
            message=final_message,
            next_run_at=next_run_at,
            mark_run=False,
        )
        return

    work_ids = [str(item) for item in (task.get("work_ids") or []) if item]
    if task.get("status") == "success":
        downloaded_ids = read_downloaded_ids(force_refresh=True)
        success_count = sum(1 for work_id in work_ids if work_id in downloaded_ids)
        message = (
            f"自动整号下载完成，成功下载 {success_count}/{len(work_ids)} 个作品。"
            if task.get("mode") == "auto_creator_batch_download" and work_ids
            else (
                f"自动下载完成，成功下载 {success_count}/{len(work_ids)} 个作品。"
                if work_ids
                else "自动下载完成。"
            )
        )
        update_auto_download_result(
            creator_id,
            status="success",
            message=message,
            next_run_at=next_run_at,
            mark_run=False,
        )
        return

    update_auto_download_result(
        creator_id,
        status="failed",
        message=task.get("message") or (
            "自动整号下载任务失败" if task.get("mode") == "auto_creator_batch_download" else "自动下载任务失败"
        ),
        next_run_at=next_run_at,
        mark_run=False,
    )


def _ensure_shared_db_link(target_volume: Path) -> None:
    shared_db = ENGINE_DB_PATH
    target_db = target_volume / "DouK-Downloader.db"
    if target_db.exists():
        return
    if shared_db.exists():
        try:
            target_db.hardlink_to(shared_db)
            return
        except Exception:
            try:
                target_db.symlink_to(shared_db)
                return
            except Exception:
                copy2(shared_db, target_db)
                return
    target_db.touch(exist_ok=True)


def _prepare_runtime_engine_db(target_volume: Path) -> None:
    target_db = target_volume / "DouK-Downloader.db"
    if not target_db.exists():
        return
    try:
        with sqlite3.connect(target_db) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS config_data (
                NAME TEXT PRIMARY KEY,
                VALUE INTEGER NOT NULL CHECK(VALUE IN (0, 1))
                );"""
            )
            conn.execute(
                """INSERT OR IGNORE INTO config_data (NAME, VALUE)
                VALUES ('Record', 1), ('Logger', 1), ('Disclaimer', 1);"""
            )
            conn.execute("REPLACE INTO config_data (NAME, VALUE) VALUES ('Record', 1)")
            conn.execute("REPLACE INTO config_data (NAME, VALUE) VALUES ('Logger', 1)")
            conn.execute("REPLACE INTO config_data (NAME, VALUE) VALUES ('Disclaimer', 1)")
            conn.commit()
    except sqlite3.Error:
        # Shared engine databases can be readonly in some deployments.
        # Keep the task runnable even if we cannot flip logger switches here.
        return


def _prepare_task_volume(task_id: int, settings: dict) -> Path:
    volume_path = TASK_RUNTIME_DIR / f"task_{task_id}" / "Volume"
    volume_path.mkdir(parents=True, exist_ok=True)
    _ensure_shared_db_link(volume_path)
    _prepare_runtime_engine_db(volume_path)
    settings_path = volume_path / "settings.json"
    settings_path.write_text(
        json.dumps(settings, ensure_ascii=False, indent=2),
        encoding="utf-8-sig" if sys.platform.startswith("win") else "utf-8",
    )
    return volume_path


def _start_process_with_logs(
    task_id: int,
    command: list[str],
    cwd: str,
    *,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen, str, str, object, object]:
    stdout_path = TASK_LOG_DIR / f"task_{task_id}_stdout.log"
    stderr_path = TASK_LOG_DIR / f"task_{task_id}_stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=stdout_handle,
        stderr=stderr_handle,
        env=env,
    )
    return process, str(stdout_path), str(stderr_path), stdout_handle, stderr_handle


def _append_task(task: dict) -> dict:
    return save_sqlite_task(task)


def _resolve_download_root(profile: dict, settings: dict) -> str:
    profile_root = normalize_download_root(profile.get("root_path") or "")
    if profile_root:
        return profile_root
    engine_root = str(settings.get("root") or "").strip()
    if engine_root:
        return engine_root
    return str(ENGINE_VOLUME_PATH)


def _launch_creator_batch_task(
    task_id: int,
    creator: dict,
    profile: dict,
    work_ids: list[str] | None = None,
    *,
    mode: str = "creator_batch_download",
) -> dict:
    settings = read_engine_settings()
    settings.update(build_settings_payload(profile, [creator]))
    settings["root"] = _resolve_download_root(profile, settings)
    settings["run_command"] = build_run_command(creator["platform"])
    volume_path = _prepare_task_volume(task_id, settings)
    if not WORKER_ISOLATED_MAIN_PATH.exists():
        raise HTTPException(status_code=500, detail="Isolated main worker script not found")
    process_env = os.environ.copy()
    process_env["PYTHONUNBUFFERED"] = "1"
    process_env["DOUK_FILE_DOWNLOAD_MAX_WORKERS"] = str(get_file_download_max_workers())
    process, stdout_log, stderr_log, stdout_handle, stderr_handle = _start_process_with_logs(
        task_id,
        [sys.executable, str(WORKER_ISOLATED_MAIN_PATH), "--volume", str(volume_path)],
        str(ENGINE_PROJECT_ROOT),
        env=process_env,
    )
    task = {
        "id": task_id,
        "creator_id": creator["id"],
        "creator_name": creator["name"],
        "platform": creator["platform"],
        "profile_id": profile["id"],
        "status": "running",
        "mode": mode,
        "item_count": len(work_ids or []),
        "run_command": settings["run_command"],
        "pid": process.pid,
        "message": "Downloader process started",
        "stdout_log": stdout_log,
        "stderr_log": stderr_log,
        "exit_code": None,
        "runtime_volume_path": str(volume_path),
        "runtime_volume_cleaned_at": None,
        "work_ids": list(work_ids or []),
    }
    PROCESS_REGISTRY[task_id] = {
        "process": process,
        "stdout": stdout_handle,
        "stderr": stderr_handle,
    }
    return task


def _launch_detail_task(task_id: int, creator: dict, profile: dict, work_ids: list[str], mode: str) -> dict:
    settings = read_engine_settings()
    settings.update(build_settings_payload(profile, [creator]))
    settings["root"] = _resolve_download_root(profile, settings)
    target_folder_name = profile["folder_name"]
    if mode in {"detail_download", "auto_detail_download"}:
        target_folder_name = infer_creator_folder_name(creator)
        settings["folder_name"] = target_folder_name
    volume_path = _prepare_task_volume(task_id, settings)
    if not WORKER_ISOLATED_DETAIL_PATH.exists():
        raise HTTPException(status_code=500, detail="Isolated detail worker script not found")
    command = [
        sys.executable,
        str(WORKER_ISOLATED_DETAIL_PATH),
        "--volume",
        str(volume_path),
        "--platform",
        creator["platform"],
        "--ids",
        *work_ids,
    ]
    process_env = os.environ.copy()
    process_env["PYTHONUNBUFFERED"] = "1"
    process_env["DOUK_DETAIL_FETCH_CONCURRENCY"] = str(get_detail_fetch_concurrency())
    process_env["DOUK_FILE_DOWNLOAD_MAX_WORKERS"] = str(get_file_download_max_workers())
    process, stdout_log, stderr_log, stdout_handle, stderr_handle = _start_process_with_logs(
        task_id,
        command,
        str(ENGINE_PROJECT_ROOT),
        env=process_env,
    )
    task = {
        "id": task_id,
        "creator_id": creator["id"],
        "creator_name": creator["name"],
        "platform": creator["platform"],
        "profile_id": profile["id"],
        "status": "running",
        "mode": mode,
        "item_count": len(work_ids),
        "run_command": "detail-worker " + " ".join(work_ids),
        "pid": process.pid,
        "message": "Auto detail download worker started" if mode == "auto_detail_download" else "Detail download worker started",
        "stdout_log": stdout_log,
        "stderr_log": stderr_log,
        "exit_code": None,
        "runtime_volume_path": str(volume_path),
        "runtime_volume_cleaned_at": None,
        "work_ids": work_ids,
        "target_folder_name": target_folder_name,
    }
    PROCESS_REGISTRY[task_id] = {
        "process": process,
        "stdout": stdout_handle,
        "stderr": stderr_handle,
    }
    return task


def _launch_batch_source_task(
    task_id: int,
    creator: dict,
    profile: dict,
    source_items: list[dict],
    work_ids: list[str],
    mode: str,
) -> dict:
    settings = read_engine_settings()
    settings.update(build_settings_payload(profile, [creator]))
    settings["root"] = _resolve_download_root(profile, settings)
    target_folder_name = infer_creator_folder_name(creator)
    settings["folder_name"] = target_folder_name
    volume_path = _prepare_task_volume(task_id, settings)
    if not WORKER_ISOLATED_BATCH_PATH.exists():
        raise HTTPException(status_code=500, detail="Isolated batch worker script not found")
    source_path = volume_path / "webui_batch_source.json"
    source_path.write_text(
        json.dumps(source_items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    command = [
        sys.executable,
        str(WORKER_ISOLATED_BATCH_PATH),
        "--volume",
        str(volume_path),
        "--platform",
        creator["platform"],
        "--tab",
        str(creator.get("tab") or "post"),
        "--sec-user-id",
        str(creator.get("sec_user_id") or ""),
        "--mark",
        str(creator.get("mark") or creator.get("name") or ""),
        "--source-file",
        str(source_path),
    ]
    process_env = os.environ.copy()
    process_env["PYTHONUNBUFFERED"] = "1"
    process_env["DOUK_FILE_DOWNLOAD_MAX_WORKERS"] = str(get_file_download_max_workers())
    process, stdout_log, stderr_log, stdout_handle, stderr_handle = _start_process_with_logs(
        task_id,
        command,
        str(ENGINE_PROJECT_ROOT),
        env=process_env,
    )
    task = {
        "id": task_id,
        "creator_id": creator["id"],
        "creator_name": creator["name"],
        "platform": creator["platform"],
        "profile_id": profile["id"],
        "status": "running",
        "mode": mode,
        "item_count": len(work_ids),
        "run_command": f"batch-worker {creator.get('tab') or 'post'} {len(work_ids)}",
        "pid": process.pid,
        "message": "Auto batch download worker started",
        "stdout_log": stdout_log,
        "stderr_log": stderr_log,
        "exit_code": None,
        "runtime_volume_path": str(volume_path),
        "runtime_volume_cleaned_at": None,
        "work_ids": list(work_ids),
        "target_folder_name": target_folder_name,
    }
    PROCESS_REGISTRY[task_id] = {
        "process": process,
        "stdout": stdout_handle,
        "stderr": stderr_handle,
    }
    return task


def _build_queued_auto_task(task_id: int, creator: dict, profile: dict, work_ids: list[str], created_at: str) -> dict:
    return _build_queued_auto_task_with_mode(
        task_id,
        creator,
        profile,
        work_ids,
        created_at,
        mode="auto_detail_download",
    )


def _build_queued_auto_task_with_mode(
    task_id: int,
    creator: dict,
    profile: dict,
    work_ids: list[str],
    created_at: str,
    *,
    mode: str,
) -> dict:
    target_folder_name = infer_creator_folder_name(creator)
    return {
        "id": task_id,
        "creator_id": creator["id"],
        "creator_name": creator["name"],
        "platform": creator["platform"],
        "profile_id": profile["id"],
        "status": "queued",
        "mode": mode,
        "item_count": len(work_ids),
        "run_command": "detail-worker " + " ".join(work_ids),
        "pid": None,
        "message": "Auto task queued, waiting for available slot",
        "stdout_log": "",
        "stderr_log": "",
        "exit_code": None,
        "runtime_volume_path": "",
        "runtime_volume_cleaned_at": None,
        "work_ids": work_ids,
        "auto_session_id": "",
        "target_folder_name": target_folder_name,
        "created_at": created_at,
        "updated_at": created_at,
    }


def create_download_task(payload: DownloadTaskCreate):
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    creator = get_creator(payload.creator_id)
    profile = get_profile(payload.profile_id or creator.get("profile_id") or 1)
    work_ids = _list_pending_work_ids_for_creator(creator)
    created_at = now_iso()
    task_id = next_sqlite_task_id()
    with TASK_LAUNCH_LOCK:
        task = _launch_creator_batch_task(task_id, creator, profile, work_ids)
    task["created_at"] = created_at
    task["updated_at"] = created_at
    _append_task(task)
    return task


def create_works_download_task(payload: DownloadWorksTaskCreate):
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    creator = get_creator(payload.creator_id)
    profile = get_profile(creator.get("profile_id") or 1)
    record_low_quality_signal(
        assess_low_quality_items(find_scan_items_by_work_ids(payload.creator_id, payload.work_ids))
    )
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    created_at = now_iso()
    task_id = next_sqlite_task_id()
    with TASK_LAUNCH_LOCK:
        task = _launch_detail_task(task_id, creator, profile, payload.work_ids, "detail_download")
    task["created_at"] = created_at
    task["updated_at"] = created_at
    _append_task(task)
    return task


def create_auto_works_download_task(
    payload: DownloadWorksTaskCreate,
    *,
    session_id: str = "",
    allow_when_scheduler_disabled: bool = False,
):
    if not allow_when_scheduler_disabled and not is_auto_download_scheduler_enabled():
        raise HTTPException(status_code=409, detail="Auto download scheduler is disabled")
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    creator = get_creator(payload.creator_id)
    profile = get_profile(creator.get("profile_id") or 1)
    record_low_quality_signal(
        assess_low_quality_items(find_scan_items_by_work_ids(payload.creator_id, payload.work_ids))
    )
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    created_at = now_iso()
    task_id = next_sqlite_task_id()
    if count_running_auto_tasks() >= AUTO_TASK_PARALLELISM:
        task = _build_queued_auto_task(task_id, creator, profile, payload.work_ids, created_at)
        task["auto_session_id"] = session_id
        return save_sqlite_task(task)
    with TASK_LAUNCH_LOCK:
        task = _launch_detail_task(task_id, creator, profile, payload.work_ids, "auto_detail_download")
    task["auto_session_id"] = session_id
    task["created_at"] = created_at
    task["updated_at"] = created_at
    return save_sqlite_task(task)


def create_auto_creator_batch_task(
    payload: DownloadWorksTaskCreate,
    *,
    session_id: str = "",
    allow_when_scheduler_disabled: bool = False,
):
    if not allow_when_scheduler_disabled and not is_auto_download_scheduler_enabled():
        raise HTTPException(status_code=409, detail="Auto download scheduler is disabled")
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    creator = get_creator(payload.creator_id)
    profile = get_profile(creator.get("profile_id") or 1)
    record_low_quality_signal(
        assess_low_quality_items(find_scan_items_by_work_ids(payload.creator_id, payload.work_ids))
    )
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    created_at = now_iso()
    task_id = next_sqlite_task_id()
    if count_running_auto_tasks() >= AUTO_TASK_PARALLELISM:
        task = _build_queued_auto_task_with_mode(
            task_id,
            creator,
            profile,
            payload.work_ids,
            created_at,
            mode="auto_creator_batch_download",
        )
        task["auto_session_id"] = session_id
        return save_sqlite_task(task)
    with TASK_LAUNCH_LOCK:
        task = _launch_creator_batch_task(
            task_id,
            creator,
            profile,
            payload.work_ids,
            mode="auto_creator_batch_download",
        )
    task["auto_session_id"] = session_id
    task["created_at"] = created_at
    task["updated_at"] = created_at
    return save_sqlite_task(task)


def create_auto_batch_source_task(
    *,
    creator_id: int,
    source_items: list[dict],
    work_ids: list[str],
    session_id: str = "",
    allow_when_scheduler_disabled: bool = False,
):
    if not allow_when_scheduler_disabled and not is_auto_download_scheduler_enabled():
        raise HTTPException(status_code=409, detail="Auto download scheduler is disabled")
    if is_risk_guard_active():
        raise HTTPException(status_code=409, detail="Risk guard cooldown is active")
    creator = get_creator(creator_id)
    profile = get_profile(creator.get("profile_id") or 1)
    created_at = now_iso()
    task_id = next_sqlite_task_id()
    if count_running_auto_tasks() >= AUTO_TASK_PARALLELISM:
        task = _build_queued_auto_task_with_mode(
            task_id,
            creator,
            profile,
            work_ids,
            created_at,
            mode="auto_detail_download",
        )
        task["auto_session_id"] = session_id
        task["message"] = "Auto batch task queued, waiting for available slot"
        task["batch_source_items"] = source_items
        return save_sqlite_task(task)
    with TASK_LAUNCH_LOCK:
        task = _launch_batch_source_task(
            task_id,
            creator,
            profile,
            source_items,
            work_ids,
            "auto_detail_download",
        )
    task["auto_session_id"] = session_id
    task["created_at"] = created_at
    task["updated_at"] = created_at
    return save_sqlite_task(task)


def dispatch_queued_auto_tasks() -> int:
    started = 0
    if is_auto_download_paused() or is_risk_guard_active():
        return started
    available_slots = max(0, AUTO_TASK_PARALLELISM - count_running_auto_tasks())
    if available_slots <= 0:
        return started
    queued_tasks = [
        task
        for task in list_sqlite_tasks_by_statuses(
            statuses=("queued",),
            limit=max(available_slots * 4, 8),
            order_asc=True,
        )
        if task.get("status") == "queued" and task.get("mode") in AUTO_TASK_MODES
    ]
    for task in reversed(queued_tasks):
        if started >= available_slots:
            break
        creator = get_creator(task["creator_id"])
        profile = get_profile(task.get("profile_id") or creator.get("profile_id") or 1)
        with TASK_LAUNCH_LOCK:
            if task.get("mode") == "auto_creator_batch_download":
                launched = _launch_creator_batch_task(
                    task["id"],
                    creator,
                    profile,
                    list(task.get("work_ids") or []),
                    mode="auto_creator_batch_download",
                )
            elif task.get("batch_source_items"):
                launched = _launch_batch_source_task(
                    task["id"],
                    creator,
                    profile,
                    list(task.get("batch_source_items") or []),
                    list(task.get("work_ids") or []),
                    "auto_detail_download",
                )
            else:
                launched = _launch_detail_task(
                    task["id"],
                    creator,
                    profile,
                    list(task.get("work_ids") or []),
                    "auto_detail_download",
                )
        launched["auto_session_id"] = task.get("auto_session_id") or ""
        launched["created_at"] = task.get("created_at") or now_iso()
        launched["updated_at"] = now_iso()
        save_sqlite_task(launched)
        started += 1
    return started


async def task_dispatcher_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.to_thread(dispatch_queued_auto_tasks)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TASK_DISPATCH_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


def _parse_task_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(value)
    return None


def cleanup_task_runtime_dirs() -> int:
    cleaned = 0
    cutoff = datetime.now() - timedelta(seconds=TASK_RUNTIME_RETENTION_SECONDS)
    for task in list_tasks():
        runtime_volume_value = task.get("runtime_volume_path") or ""
        if not runtime_volume_value:
            continue
        runtime_volume_path = Path(runtime_volume_value)
        if not runtime_volume_path:
            continue
        if task.get("status") not in TERMINAL_TASK_STATUSES:
            continue
        if task.get("runtime_volume_cleaned_at"):
            continue
        updated_at = _parse_task_datetime(task.get("updated_at"))
        if updated_at and updated_at > cutoff:
            continue
        runtime_root = runtime_volume_path.parent
        if runtime_root.exists():
            shutil.rmtree(runtime_root, ignore_errors=True)
        task["runtime_volume_cleaned_at"] = now_iso()
        task["updated_at"] = now_iso()
        save_sqlite_task(task)
        cleaned += 1
    return cleaned


async def task_runtime_cleanup_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.to_thread(cleanup_task_runtime_dirs)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TASK_RUNTIME_CLEANUP_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


def stop_task(task_id: int):
    task = get_sqlite_task(task_id)
    if task:
        if task.get("status") == "queued":
            task["status"] = "stopped"
            task["message"] = "Queued auto task cancelled by user"
            task["updated_at"] = now_iso()
            save_sqlite_task(task)
            return task
        entry = PROCESS_REGISTRY.get(task_id)
        process = entry["process"] if entry else None
        if not process or process.poll() is not None:
            return _task_status(task)
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        task["status"] = "stopped"
        task["message"] = "Downloader process terminated by user"
        task["exit_code"] = process.returncode
        task["updated_at"] = now_iso()
        save_sqlite_task(task)
        _finalize_registry_entry(task_id)
        return task
    raise HTTPException(status_code=404, detail="Task not found")


def stop_creator_workflow(creator_id: int) -> dict:
    creator = get_creator(creator_id)
    from .auto_download_runtime import request_stop_creator_workflow as request_stop_workflow

    stop_request = request_stop_workflow(creator_id)
    stopped_task_count = 0
    stopped_task_ids: list[int] = []
    affected_tasks = list_sqlite_tasks_by_statuses(creator_id=creator_id)
    for task in affected_tasks:
        if task.get("status") in {"running", "queued"}:
            current = stop_task(int(task["id"]))
            if current.get("status") == "stopped":
                stopped_task_count += 1
                stopped_task_ids.append(int(current["id"]))

    if stop_request["was_running"]:
        message = "已发送结束任务请求；当前扫描会在本次请求返回后停止，且不会继续为该账号入队下载。"
    elif stopped_task_count or stop_request["removed_from_manual_queue"]:
        message = "已结束该账号当前排队或下载任务。"
    else:
        message = "当前账号没有正在执行中的扫描、排队或下载任务。"

    update_auto_download_result(
        int(creator["id"]),
        status="stopped",
        message=message,
        next_run_at=creator.get("auto_download_next_run_at"),
        mark_run=False,
        record_history=False,
    )
    return {
        "creator_id": int(creator["id"]),
        "stopped_task_count": stopped_task_count,
        "stopped_task_ids": stopped_task_ids,
        "removed_from_manual_queue": bool(stop_request["removed_from_manual_queue"]),
        "scan_stop_requested": bool(stop_request["was_running"]),
        "message": message,
    }


def clear_creator_task_records(creator_id: int, purge_download_history: bool = False) -> dict:
    creator = get_creator(creator_id)
    affected_tasks = list_sqlite_tasks_by_statuses(creator_id=creator_id)
    scan_cache_rows = list_sqlite_scan_cache(creator_id)
    stopped_count = 0
    for task in affected_tasks:
        if task.get("status") in {"running", "queued"}:
            current = stop_task(int(task["id"]))
            if current.get("status") == "stopped":
                stopped_count += 1
    deleted_count = delete_sqlite_tasks_by_creator(creator_id)
    deleted_download_records = 0
    resolved_work_ids = 0
    deleted_scan_cache_count = 0
    if purge_download_history:
        creator["_task_rows"] = affected_tasks
        creator["_scan_cache_rows"] = scan_cache_rows
        work_ids = collect_creator_work_ids_for_purge(creator)
        creator.pop("_task_rows", None)
        creator.pop("_scan_cache_rows", None)
        if work_ids:
            resolved_work_ids = len(work_ids)
            deleted_download_records = delete_downloaded_ids(work_ids)
        deleted_scan_cache_count = len(scan_cache_rows)
        delete_sqlite_scan_cache_for_creator(creator_id)
        set_creator_initial_scan_completed(creator_id, False)
    clear_auto_download_runtime_state(creator_id)
    return {
        "creator_id": creator_id,
        "stopped_task_count": stopped_count,
        "deleted_task_count": deleted_count,
        "purged_download_history": bool(purge_download_history),
        "resolved_work_ids": resolved_work_ids,
        "deleted_download_records": deleted_download_records,
        "deleted_scan_cache_count": deleted_scan_cache_count,
    }
