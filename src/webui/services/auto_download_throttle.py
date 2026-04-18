from datetime import datetime, timedelta
from threading import Lock

from .panel_config import read_panel_config


_STATE_LOCK = Lock()
_STATE = {
    "works_count": 0,
    "creators_count": 0,
    "works_window_minutes": 30,
    "paused_until": None,
    "last_reason": "",
    "work_events": [],
    "creator_events": [],
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


def _prune_events(events: list[str], *, window_minutes: int, now: datetime) -> list[str]:
    if window_minutes <= 0:
        return []
    cutoff = now - timedelta(minutes=window_minutes)
    kept: list[str] = []
    for value in events:
        parsed = _parse_dt(value)
        if parsed and parsed >= cutoff:
            kept.append(parsed.isoformat(timespec="seconds"))
    return kept


def get_auto_download_throttle_state() -> dict:
    with _STATE_LOCK:
        config = read_panel_config()
        window_minutes = max(1, int(config.get("auto_download_pause_window_minutes") or 30))
        now = datetime.now()
        _STATE["works_window_minutes"] = window_minutes
        _STATE["work_events"] = _prune_events(
            list(_STATE.get("work_events") or []),
            window_minutes=window_minutes,
            now=now,
        )
        _STATE["creator_events"] = _prune_events(
            list(_STATE.get("creator_events") or []),
            window_minutes=window_minutes,
            now=now,
        )
        _STATE["works_count"] = len(_STATE["work_events"])
        _STATE["creators_count"] = len(_STATE["creator_events"])
        paused_until = _parse_dt(_STATE.get("paused_until"))
        if paused_until and paused_until <= now:
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
    pause_window_minutes = max(1, int(config.get("auto_download_pause_window_minutes") or 30))
    pause_after_creators = max(0, int(config.get("auto_download_pause_after_creators") or 0))
    with _STATE_LOCK:
        now = datetime.now()
        _STATE["works_window_minutes"] = pause_window_minutes
        paused_until = _parse_dt(_STATE.get("paused_until"))
        if paused_until and paused_until > now:
            return _with_runtime_fields(dict(_STATE))
        if paused_until and paused_until <= now:
            _STATE["paused_until"] = None
            _STATE["last_reason"] = ""
        work_events = _prune_events(
            list(_STATE.get("work_events") or []),
            window_minutes=pause_window_minutes,
            now=now,
        )
        creator_events = _prune_events(
            list(_STATE.get("creator_events") or []),
            window_minutes=pause_window_minutes,
            now=now,
        )
        current_mark = now.isoformat(timespec="seconds")
        work_events.extend([current_mark] * max(0, int(works_count)))
        creator_events.extend([current_mark] * max(0, int(creators_count)))
        _STATE["work_events"] = work_events
        _STATE["creator_events"] = creator_events
        _STATE["works_count"] = len(work_events)
        _STATE["creators_count"] = len(creator_events)

        reason = ""
        if pause_mode == "creators":
            if pause_after_creators and _STATE["creators_count"] >= pause_after_creators:
                reason = f"自动下载最近 {pause_window_minutes} 分钟内已处理 {_STATE['creators_count']} 个账号，暂停 {pause_minutes} 分钟后继续。"
        else:
            if pause_after_works and _STATE["works_count"] >= pause_after_works:
                reason = f"自动下载最近 {pause_window_minutes} 分钟内已成功下载 {_STATE['works_count']} 个作品，暂停 {pause_minutes} 分钟后继续。"

        if reason:
            _STATE["paused_until"] = (now + timedelta(minutes=pause_minutes)).isoformat(timespec="seconds")
            _STATE["last_reason"] = reason
            _STATE["works_count"] = 0
            _STATE["creators_count"] = 0
            _STATE["work_events"] = []
            _STATE["creator_events"] = []
        return _with_runtime_fields(dict(_STATE))
