from datetime import datetime, timedelta

from fastapi import HTTPException

from ..db import (
    delete_sqlite_creator,
    get_sqlite_dashboard_summary,
    get_sqlite_creator,
    list_due_sqlite_creators,
    list_sqlite_creator_options,
    list_sqlite_scan_cache,
    list_sqlite_creator_summaries_paginated,
    list_sqlite_creator_summaries,
    list_sqlite_tasks,
    list_sqlite_creators,
    now_iso,
    save_sqlite_creator,
    delete_task_center_cache,
)
from ..schemas import CreatorCreate, CreatorQuickAddRequest, CreatorUpdate
from .engine import (
    delete_downloaded_ids,
    detect_platform_from_url,
    expand_share_url,
    extract_sec_user_id,
    fetch_account_items,
    inspect_creator_profile,
    normalize_creator_url,
)
from .panel_config import read_panel_config


def has_creator_completed_initial_scan(item: dict) -> bool:
    state = item.get("initial_scan_completed")
    if state is not None:
        return bool(state)
    if str(item.get("auto_download_last_status") or "").strip().lower() in {
        "success",
        "idle",
    }:
        return True
    for history_item in item.get("auto_download_history") or []:
        if str(history_item.get("status") or "").strip().lower() in {"success", "idle"}:
            return True
    creator_id = int(item.get("id") or 0)
    if creator_id <= 0:
        return False
    return bool(list_sqlite_scan_cache(creator_id))


def set_creator_initial_scan_completed(creator_id: int, completed: bool) -> dict:
    item = get_sqlite_creator(creator_id)
    if not item:
        raise HTTPException(status_code=404, detail="Creator not found")
    item["initial_scan_completed"] = bool(completed)
    item["updated_at"] = now_iso()
    return save_sqlite_creator(item)


def _normalize_schedule_fields(item: dict) -> None:
    interval = max(0, int(item.get("auto_download_interval_minutes") or 0))
    item["auto_download_interval_minutes"] = interval
    enabled = (
        bool(item.get("auto_download_enabled"))
        and interval > 0
        and bool(item.get("enabled", True))
    )
    item["auto_download_enabled"] = enabled

    start_at = item.get("auto_download_start_at")
    if hasattr(start_at, "isoformat"):
        start_at = start_at.isoformat(timespec="seconds")
    item["auto_download_start_at"] = start_at

    last_run_at = item.get("auto_download_last_run_at")
    if hasattr(last_run_at, "isoformat"):
        last_run_at = last_run_at.isoformat(timespec="seconds")
    item["auto_download_last_run_at"] = last_run_at
    item["auto_download_history"] = list(item.get("auto_download_history") or [])

    next_run_at = item.get("auto_download_next_run_at")
    if hasattr(next_run_at, "isoformat"):
        next_run_at = next_run_at.isoformat(timespec="seconds")

    if enabled:
        if next_run_at:
            item["auto_download_next_run_at"] = next_run_at
            return
        base = (
            datetime.fromisoformat(start_at)
            if start_at
            else (datetime.now() + timedelta(minutes=interval))
        )
        if base < datetime.now():
            base = datetime.now() + timedelta(minutes=interval)
        item["auto_download_next_run_at"] = base.isoformat(timespec="seconds")
        return

    item["auto_download_next_run_at"] = None


def _resolve_next_run_at(item: dict) -> str | None:
    interval = max(0, int(item.get("auto_download_interval_minutes") or 0))
    enabled = (
        bool(item.get("auto_download_enabled"))
        and interval > 0
        and bool(item.get("enabled", True))
    )
    if not enabled:
        return None

    start_at = item.get("auto_download_start_at")
    if start_at:
        try:
            base = datetime.fromisoformat(str(start_at))
        except ValueError:
            base = datetime.now()
    else:
        base = datetime.now() + timedelta(minutes=interval)

    if base < datetime.now():
        base = datetime.now() + timedelta(minutes=interval)
    return base.isoformat(timespec="seconds")


def update_auto_download_result(
    creator_id: int,
    *,
    status: str,
    message: str,
    next_run_at: datetime | str | None = None,
    mark_run: bool = True,
    record_history: bool = True,
):
    item = get_sqlite_creator(creator_id)
    if not item:
        raise HTTPException(status_code=404, detail="Creator not found")
    if mark_run:
        item["auto_download_last_run_at"] = now_iso()
    item["auto_download_last_status"] = status
    item["auto_download_last_message"] = message
    if record_history:
        history = list(item.get("auto_download_history") or [])
        history.insert(
            0,
            {
                "run_at": item["auto_download_last_run_at"] if mark_run else now_iso(),
                "status": status,
                "message": message,
                "next_run_at": next_run_at.isoformat(timespec="seconds")
                if hasattr(next_run_at, "isoformat")
                else (str(next_run_at) if next_run_at is not None else None),
            },
        )
        item["auto_download_history"] = history[:10]
    if next_run_at is None:
        item["auto_download_next_run_at"] = None
    elif hasattr(next_run_at, "isoformat"):
        item["auto_download_next_run_at"] = next_run_at.isoformat(timespec="seconds")
    else:
        item["auto_download_next_run_at"] = str(next_run_at)
    item["updated_at"] = now_iso()
    return save_sqlite_creator(item)


def reset_auto_download_schedule(creator_id: int):
    item = get_sqlite_creator(creator_id)
    if not item:
        raise HTTPException(status_code=404, detail="Creator not found")
    _normalize_schedule_fields(item)
    item["auto_download_next_run_at"] = _resolve_next_run_at(item)
    item["updated_at"] = now_iso()
    return save_sqlite_creator(item)


def clear_auto_download_runtime_state(creator_id: int):
    item = get_sqlite_creator(creator_id)
    if not item:
        raise HTTPException(status_code=404, detail="Creator not found")
    item["auto_download_last_run_at"] = None
    item["auto_download_last_status"] = ""
    item["auto_download_last_message"] = ""
    item["auto_download_history"] = []
    item["auto_download_next_run_at"] = _resolve_next_run_at(item)
    item["updated_at"] = now_iso()
    return save_sqlite_creator(item)


def list_creators():
    return list_sqlite_creators()


def list_creator_summaries():
    return list_sqlite_creator_summaries()


def list_creator_options():
    return list_sqlite_creator_options()


def list_creator_page(
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    platform: str = "",
    profile_id: int | None = None,
    enabled: str = "",
    auto_enabled: str = "",
    download_status: str = "",
):
    from .tasks import refresh_active_task_statuses, get_creator_download_status

    refresh_active_task_statuses()
    return list_sqlite_creator_summaries_paginated(
        page=page,
        page_size=page_size,
        keyword=keyword,
        platform=platform,
        profile_id=profile_id,
        enabled=enabled,
        auto_enabled=auto_enabled,
        download_status=download_status,
        get_creator_download_status=get_creator_download_status,
    )


def get_dashboard_summary():
    from .tasks import refresh_active_task_statuses

    refresh_active_task_statuses()
    return get_sqlite_dashboard_summary()


def list_due_auto_download_creators(now_value: str, limit: int | None = None):
    return list_due_sqlite_creators(now_value, limit=limit)


def get_creator(creator_id: int):
    item = get_sqlite_creator(creator_id)
    if item:
        return item
    raise HTTPException(status_code=404, detail="Creator not found")


def _normalize_creator_identity_fields(item: dict) -> None:
    item["platform"] = (
        str(item.get("platform") or detect_platform_from_url(item.get("url") or ""))
        .strip()
        .lower()
        or "douyin"
    )
    item["url"] = normalize_creator_url(item.get("url") or "")
    item["name"] = str(item.get("name") or "").strip()
    item["mark"] = str(item.get("mark") or "").strip()
    item["sec_user_id"] = str(item.get("sec_user_id") or "").strip()
    if item["url"] and not item["sec_user_id"]:
        item["sec_user_id"] = str(
            extract_sec_user_id(item["platform"], item["url"]) or ""
        ).strip()


def _expand_creator_identity_fields(item: dict) -> bool:
    if not item.get("url"):
        return False
    previous_url = str(item.get("url") or "").strip()
    previous_sec_user_id = str(item.get("sec_user_id") or "").strip()
    expanded_url = expand_share_url(item["platform"], previous_url)
    normalized_expanded_url = normalize_creator_url(expanded_url or previous_url)
    if normalized_expanded_url:
        item["url"] = normalized_expanded_url
    if not item.get("sec_user_id"):
        item["sec_user_id"] = str(
            extract_sec_user_id(item["platform"], expanded_url or item["url"]) or ""
        ).strip()
    return (
        str(item.get("url") or "").strip() != previous_url
        or str(item.get("sec_user_id") or "").strip() != previous_sec_user_id
    )


def _ensure_creator_not_duplicate(item: dict, *, exclude_id: int | None = None) -> None:
    incoming_sec_user_id = str(item.get("sec_user_id") or "").strip()
    incoming_url = normalize_creator_url(item.get("url") or "")
    for creator in list_sqlite_creators():
        creator_id = int(creator.get("id") or 0)
        if exclude_id is not None and creator_id == int(exclude_id):
            continue
        existing_sec_user_id = str(creator.get("sec_user_id") or "").strip()
        existing_url = normalize_creator_url(creator.get("url") or "")
        same_sec_user = (
            bool(incoming_sec_user_id) and incoming_sec_user_id == existing_sec_user_id
        )
        same_url = bool(incoming_url) and incoming_url == existing_url
        if same_sec_user or same_url:
            raise HTTPException(status_code=409, detail="Creator already exists")


def find_existing_creator_by_identity(item: dict) -> dict | None:
    incoming_sec_user_id = str(item.get("sec_user_id") or "").strip()
    incoming_url = normalize_creator_url(item.get("url") or "")
    for creator in list_sqlite_creators():
        existing_sec_user_id = str(creator.get("sec_user_id") or "").strip()
        existing_url = normalize_creator_url(creator.get("url") or "")
        same_sec_user = (
            bool(incoming_sec_user_id) and incoming_sec_user_id == existing_sec_user_id
        )
        same_url = bool(incoming_url) and incoming_url == existing_url
        if same_sec_user or same_url:
            return creator
    return None


def validate_script_access_password(password: str) -> None:
    expected = str(read_panel_config().get("access_password") or "151150")
    if str(password or "") != expected:
        raise HTTPException(status_code=401, detail="Invalid access password")


def upsert_creator_from_script(payload: dict) -> dict:
    validate_script_access_password(payload.get("password") or "")
    config = read_panel_config()
    enabled = bool(payload["enabled"]) if "enabled" in payload else True
    auto_download_enabled = (
        bool(payload["auto_download_enabled"])
        if "auto_download_enabled" in payload
        else bool(config.get("quick_add_auto_download_enabled"))
    )
    auto_download_interval_minutes = max(
        0,
        int(
            payload["auto_download_interval_minutes"]
            if "auto_download_interval_minutes" in payload
            else (config.get("quick_add_auto_download_interval_minutes") or 0)
        ),
    )
    item = {
        "platform": payload.get("platform")
        or detect_platform_from_url(payload.get("url") or ""),
        "name": str(payload.get("name") or "").strip(),
        "mark": str(payload.get("mark") or payload.get("name") or "").strip(),
        "url": payload.get("url") or "",
        "sec_user_id": payload.get("sec_user_id") or "",
        "tab": payload.get("tab") or str(config.get("quick_add_tab") or "post"),
        "enabled": enabled,
        "profile_id": payload.get("profile_id")
        or config.get("quick_add_profile_id")
        or 1,
        "auto_download_enabled": auto_download_enabled,
        "auto_download_interval_minutes": auto_download_interval_minutes,
        "auto_download_start_at": None,
        "auto_download_last_run_at": None,
        "auto_download_next_run_at": None,
        "auto_download_last_status": "",
        "auto_download_last_message": "",
        "auto_download_history": [],
    }
    _normalize_creator_identity_fields(item)
    if item.get("url") and not item.get("sec_user_id"):
        identity_expanded = _expand_creator_identity_fields(item)
        if identity_expanded:
            _normalize_creator_identity_fields(item)
    existing = find_existing_creator_by_identity(item)
    if existing:
        return {
            "ok": True,
            "status": "exists",
            "message": "账号已存在。",
            "exists": True,
            "creator": existing,
        }
    if bool(payload.get("only_check")):
        return {
            "ok": True,
            "status": "not_found",
            "message": "当前账号尚未新增。",
            "exists": False,
            "creator": None,
        }
    item["id"] = None
    item["initial_scan_completed"] = False
    _normalize_schedule_fields(item)
    item["created_at"] = now_iso()
    item["updated_at"] = item["created_at"]
    creator = save_sqlite_creator(item)
    return {
        "ok": True,
        "status": "created",
        "message": "账号已新增。",
        "exists": False,
        "creator": creator,
    }


def list_creator_history_for_script(password: str) -> dict:
    validate_script_access_password(password)
    items = []
    for creator in list_sqlite_creators():
        items.append(
            {
                "id": int(creator.get("id") or 0),
                "platform": str(creator.get("platform") or ""),
                "name": str(creator.get("name") or ""),
                "mark": str(creator.get("mark") or ""),
                "url": str(creator.get("url") or ""),
            }
        )
    return {
        "ok": True,
        "total": len(items),
        "items": items,
    }


def create_creator(payload: CreatorCreate):
    item = payload.model_dump()
    _normalize_creator_identity_fields(item)
    _ensure_creator_not_duplicate(item)
    if item.get("url") and not item.get("sec_user_id"):
        identity_expanded = _expand_creator_identity_fields(item)
        if identity_expanded:
            _ensure_creator_not_duplicate(item)
    item["id"] = None
    item["profile_id"] = item["profile_id"] or 1
    item["initial_scan_completed"] = False
    _normalize_schedule_fields(item)
    item["created_at"] = now_iso()
    item["updated_at"] = item["created_at"]
    return save_sqlite_creator(item)


def _split_quick_add_urls(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for line in str(text or "").replace("\r", "\n").split("\n"):
        value = str(line or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        urls.append(value)
    return urls


def _create_single_creator_via_quick_add(url: str):
    config = read_panel_config()
    inspected = inspect_creator_profile(
        url,
        tab=str(config.get("quick_add_tab") or "post"),
    )
    _ensure_creator_not_duplicate(inspected)

    item = {
        "id": None,
        "platform": inspected["platform"],
        "name": inspected["name"],
        "mark": inspected["mark"],
        "url": inspected["url"],
        "sec_user_id": inspected["sec_user_id"],
        "tab": str(config.get("quick_add_tab") or "post"),
        "enabled": True,
        "profile_id": max(1, int(config.get("quick_add_profile_id") or 1)),
        "auto_download_enabled": bool(config.get("quick_add_auto_download_enabled")),
        "auto_download_interval_minutes": max(
            0, int(config.get("quick_add_auto_download_interval_minutes") or 0)
        ),
        "auto_download_start_at": None,
        "auto_download_last_run_at": None,
        "auto_download_next_run_at": None,
        "auto_download_last_status": "",
        "auto_download_last_message": "",
        "auto_download_history": [],
        "initial_scan_completed": False,
    }
    _normalize_schedule_fields(item)
    item["created_at"] = now_iso()
    item["updated_at"] = item["created_at"]
    return save_sqlite_creator(item)


def create_creator_via_quick_add(payload: CreatorQuickAddRequest):
    urls = _split_quick_add_urls(payload.url)
    if not urls:
        raise HTTPException(status_code=400, detail="No valid creator urls provided")
    items: list[dict] = []
    created_count = 0
    for url in urls:
        try:
            creator = _create_single_creator_via_quick_add(url)
            items.append(
                {
                    "url": url,
                    "success": True,
                    "creator": creator,
                    "error": "",
                }
            )
            created_count += 1
        except HTTPException as error:
            items.append(
                {
                    "url": url,
                    "success": False,
                    "creator": None,
                    "error": str(error.detail or "Request failed"),
                }
            )
        except Exception as error:
            items.append(
                {
                    "url": url,
                    "success": False,
                    "creator": None,
                    "error": str(error),
                }
            )
    return {
        "total": len(urls),
        "created_count": created_count,
        "failed_count": len(urls) - created_count,
        "items": items,
    }


def update_creator(creator_id: int, payload: CreatorUpdate):
    item = get_sqlite_creator(creator_id)
    if not item:
        raise HTTPException(status_code=404, detail="Creator not found")
    schedule_snapshot = (
        item.get("auto_download_enabled"),
        item.get("auto_download_interval_minutes"),
        item.get("auto_download_start_at"),
        item.get("enabled"),
    )
    item.update(payload.model_dump())
    _normalize_creator_identity_fields(item)
    _ensure_creator_not_duplicate(item, exclude_id=creator_id)
    if item.get("url") and not item.get("sec_user_id"):
        identity_expanded = _expand_creator_identity_fields(item)
        if identity_expanded:
            _ensure_creator_not_duplicate(item, exclude_id=creator_id)
    item["profile_id"] = item["profile_id"] or 1
    new_snapshot = (
        item.get("auto_download_enabled"),
        item.get("auto_download_interval_minutes"),
        item.get("auto_download_start_at"),
        item.get("enabled"),
    )
    if schedule_snapshot != new_snapshot:
        item["auto_download_next_run_at"] = None
    _normalize_schedule_fields(item)
    item["updated_at"] = now_iso()
    return save_sqlite_creator(item)


def delete_creator(creator_id: int):
    if not get_sqlite_creator(creator_id):
        raise HTTPException(status_code=404, detail="Creator not found")
    from .auto_download_runtime import remove_manual_creator_run
    from . import tasks as task_service

    task_service.clear_creator_task_records(creator_id, purge_download_history=False)
    remove_manual_creator_run(creator_id)
    delete_sqlite_creator(creator_id)
    # 清除该账号的任务中心缓存
    delete_task_center_cache(creator_id)


def collect_creator_work_ids_for_purge(item: dict) -> list[str]:
    return collect_local_creator_work_ids_for_purge(item)


def collect_local_creator_work_ids_for_purge(item: dict) -> list[str]:
    work_ids: set[str] = set()
    for row in item.get("_task_rows", []):
        for work_id in row.get("work_ids") or []:
            value = str(work_id or "").strip()
            if value:
                work_ids.add(value)
    for row in item.get("_scan_cache_rows", []):
        for work in row.get("payload") or []:
            work_id = str(work.get("id") or "").strip()
            if work_id:
                work_ids.add(work_id)
    return sorted(work_ids)


def collect_remote_creator_work_ids_for_purge(item: dict) -> list[str]:
    from .scans import map_engine_items

    work_ids: set[str] = set()
    try:
        engine_items = map_engine_items(fetch_account_items(item))
    except Exception:
        engine_items = []
    for work in engine_items:
        work_id = str(work.get("id") or "").strip()
        if work_id:
            work_ids.add(work_id)
    return sorted(work_ids)


def delete_creator_with_download_history(creator_id: int) -> dict:
    item = get_sqlite_creator(creator_id)
    if not item:
        raise HTTPException(status_code=404, detail="Creator not found")
    from .auto_download_runtime import remove_manual_creator_run
    from . import tasks as task_service

    scan_cache_rows = list_sqlite_scan_cache(creator_id)
    item["_scan_cache_rows"] = scan_cache_rows
    item["_task_rows"] = [
        task for task in list_sqlite_tasks() if task.get("creator_id") == creator_id
    ]
    work_ids = collect_local_creator_work_ids_for_purge(item)
    work_id_source = "local_cache"
    item.pop("_scan_cache_rows", None)
    item.pop("_task_rows", None)

    task_service.clear_creator_task_records(creator_id, purge_download_history=False)
    remove_manual_creator_run(creator_id)
    deleted_count = delete_downloaded_ids(work_ids) if work_ids else 0
    delete_sqlite_creator(creator_id)
    # 清除该账号的任务中心缓存
    delete_task_center_cache(creator_id)
    return {
        "creator_id": creator_id,
        "resolved_work_ids": len(work_ids),
        "deleted_download_records": deleted_count,
        "work_id_source": work_id_source if work_ids else "none",
    }
