from datetime import datetime, timedelta
from threading import Lock

from .panel_config import read_panel_config


_STATE_LOCK = Lock()
_STATE = {
    "works_count": 0,
    "creators_count": 0,
    "paused_until": None,
    "last_reason": "",
}


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _with_runtime_fields(state: dict) -> dict:
    paused_until = _parse_dt(state.get("paused_until"))
    remaining_seconds = max(0, int((paused_until - datetime.now()).total_seconds())) if paused_until else 0
    return {
        **state,
        "is_paused": bool(paused_until and paused_until > datetime.now()),
        "remaining_seconds": remaining_seconds,
    }


def get_auto_download_throttle_state() -> dict:
    with _STATE_LOCK:
        paused_until = _parse_dt(_STATE.get("paused_until"))
        if paused_until and paused_until <= datetime.now():
            _STATE["paused_until"] = None
            _STATE["last_reason"] = ""
        return _with_runtime_fields(dict(_STATE))


def is_auto_download_paused() -> bool:
    return bool(get_auto_download_throttle_state()["is_paused"])


def record_auto_download_progress(*, creators_count: int = 0, works_count: int = 0) -> dict:
    config = read_panel_config()
    pause_mode = str(config.get("auto_download_pause_mode") or "works").lower()
    pause_minutes = max(1, int(config.get("auto_download_pause_minutes") or 5))
    pause_after_works = max(0, int(config.get("auto_download_pause_after_works") or 0))
    pause_after_creators = max(0, int(config.get("auto_download_pause_after_creators") or 0))
    with _STATE_LOCK:
        paused_until = _parse_dt(_STATE.get("paused_until"))
        if paused_until and paused_until > datetime.now():
            return _with_runtime_fields(dict(_STATE))
        if paused_until and paused_until <= datetime.now():
            _STATE["paused_until"] = None
            _STATE["last_reason"] = ""

        _STATE["works_count"] += max(0, int(works_count))
        _STATE["creators_count"] += max(0, int(creators_count))

        reason = ""
        if pause_mode == "creators":
            if pause_after_creators and _STATE["creators_count"] >= pause_after_creators:
                reason = f"自动下载已累计处理 {_STATE['creators_count']} 个账号，暂停 {pause_minutes} 分钟后继续。"
        else:
            if pause_after_works and _STATE["works_count"] >= pause_after_works:
                reason = f"自动下载已累计处理 {_STATE['works_count']} 个作品，暂停 {pause_minutes} 分钟后继续。"

        if reason:
            _STATE["paused_until"] = (datetime.now() + timedelta(minutes=pause_minutes)).isoformat(timespec="seconds")
            _STATE["last_reason"] = reason
            _STATE["works_count"] = 0
            _STATE["creators_count"] = 0
        return _with_runtime_fields(dict(_STATE))
