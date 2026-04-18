import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from platform import system
from threading import Lock

from ..config import DATA_DIR
from .panel_config import read_panel_config


RISK_GUARD_STATE_PATH = DATA_DIR / "risk_guard_state.json"
RISK_GUARD_ENCODING = "utf-8-sig" if system() == "Windows" else "utf-8"
_LOCK = Lock()
_STATUS_CODE_PATTERN = re.compile(r"\b(4\d{2}|5\d{2})\b")

_DEFAULT_STATE = {
    "cooldown_until": None,
    "last_reason": "",
    "last_triggered_at": None,
    "http_error_streak": 0,
    "empty_download_streak": 0,
    "low_quality_streak": 0,
}


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _runtime_fields(state: dict) -> dict:
    cooldown_until = _parse_dt(state.get("cooldown_until"))
    remaining_seconds = max(0, int((cooldown_until - datetime.now()).total_seconds())) if cooldown_until else 0
    return {
        **state,
        "is_active": bool(cooldown_until and cooldown_until > datetime.now()),
        "remaining_seconds": remaining_seconds,
    }


def _read_state() -> dict:
    if not RISK_GUARD_STATE_PATH.exists():
        _write_state(dict(_DEFAULT_STATE))
    data = json.loads(RISK_GUARD_STATE_PATH.read_text(encoding=RISK_GUARD_ENCODING))
    changed = False
    for key, value in _DEFAULT_STATE.items():
        if key not in data:
            data[key] = value
            changed = True
    cooldown_until = _parse_dt(data.get("cooldown_until"))
    if cooldown_until and cooldown_until <= datetime.now():
        data["cooldown_until"] = None
        data["last_reason"] = ""
        changed = True
    if changed:
        _write_state(data)
    return data


def _write_state(state: dict) -> None:
    RISK_GUARD_STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding=RISK_GUARD_ENCODING,
    )


def get_risk_guard_state() -> dict:
    with _LOCK:
        return _runtime_fields(_read_state())


def reset_risk_guard_state() -> dict:
    with _LOCK:
        state = dict(_DEFAULT_STATE)
        _write_state(state)
        return _runtime_fields(state)


def is_risk_guard_active() -> bool:
    config = read_panel_config()
    if not config.get("risk_guard_enabled"):
        return False
    return bool(get_risk_guard_state()["is_active"])


def _parse_status_code_set() -> set[int]:
    config = read_panel_config()
    raw = str(config.get("risk_guard_status_codes") or "")
    result = set()
    for piece in raw.split(","):
        piece = piece.strip()
        if piece.isdigit():
            result.add(int(piece))
    return result or {403, 429}


def trigger_risk_guard(reason: str) -> dict:
    config = read_panel_config()
    cooldown_hours = max(1, int(config.get("risk_guard_cooldown_hours") or 24))
    with _LOCK:
        state = _read_state()
        state["cooldown_until"] = (
            datetime.now() + timedelta(hours=cooldown_hours)
        ).isoformat(timespec="seconds")
        state["last_reason"] = reason
        state["last_triggered_at"] = datetime.now().isoformat(timespec="seconds")
        state["http_error_streak"] = 0
        state["empty_download_streak"] = 0
        state["low_quality_streak"] = 0
        _write_state(state)
        return _runtime_fields(state)


def _update_streak(key: str, triggered: bool) -> tuple[dict, int]:
    state = _read_state()
    state[key] = int(state.get(key) or 0) + 1 if triggered else 0
    _write_state(state)
    return state, int(state[key])


def record_http_error_signal(status_codes: list[int], raw_text: str) -> dict:
    config = read_panel_config()
    if not config.get("risk_guard_enabled"):
        return get_risk_guard_state()
    watched = _parse_status_code_set()
    has_signal = any(code in watched for code in status_codes) or "HTTPStatusError" in raw_text
    state, streak = _update_streak("http_error_streak", has_signal)
    threshold = max(1, int(config.get("risk_guard_http_error_streak") or 3))
    if has_signal and streak >= threshold:
        codes_text = ",".join(str(code) for code in sorted(set(status_codes) & watched)) or ",".join(str(code) for code in sorted(watched))
        return trigger_risk_guard(f"连续 {streak} 次出现 HTTP 状态异常，命中状态码 {codes_text}，已暂停 24 小时。")
    return _runtime_fields(state)


def record_empty_download_signal(triggered: bool) -> dict:
    config = read_panel_config()
    if not config.get("risk_guard_enabled"):
        return get_risk_guard_state()
    state, streak = _update_streak("empty_download_streak", triggered)
    threshold = max(1, int(config.get("risk_guard_empty_download_streak") or 3))
    if triggered and streak >= threshold:
        return trigger_risk_guard(f"连续 {streak} 次出现下载地址为空或提取失败，已暂停 24 小时。")
    return _runtime_fields(state)


def record_low_quality_signal(triggered: bool) -> dict:
    config = read_panel_config()
    if not config.get("risk_guard_enabled"):
        return get_risk_guard_state()
    state, streak = _update_streak("low_quality_streak", triggered)
    threshold = max(1, int(config.get("risk_guard_low_quality_streak") or 3))
    if triggered and streak >= threshold:
        return trigger_risk_guard(f"连续 {streak} 次出现低清晰度比例异常，已暂停 24 小时。")
    return _runtime_fields(state)


def extract_status_codes(raw_text: str) -> list[int]:
    return [int(match.group(1)) for match in _STATUS_CODE_PATTERN.finditer(raw_text)]


def detect_empty_download(raw_text: str) -> bool:
    patterns = [
        "提取文件下载地址失败",
        "下载地址失败",
        "failed to extract",
        "download url",
    ]
    lowered = raw_text.lower()
    return any(pattern in raw_text or pattern in lowered for pattern in patterns)


def assess_low_quality_items(items: list[dict]) -> bool:
    config = read_panel_config()
    if not config.get("risk_guard_enabled"):
        return False
    threshold_ratio = float(config.get("risk_guard_low_quality_ratio") or 0.8)
    max_dimension = max(1, int(config.get("risk_guard_low_quality_max_dimension") or 720))
    videos = []
    for item in items:
        raw = item.get("raw") or item
        if str(raw.get("type", "")).lower() not in {"video", "视频"}:
            continue
        width = int(raw.get("width") or 0)
        height = int(raw.get("height") or 0)
        if width <= 0 and height <= 0:
            continue
        videos.append(max(width, height))
    if len(videos) < 3:
        return False
    low_count = sum(1 for dimension in videos if dimension <= max_dimension)
    return (low_count / len(videos)) >= threshold_ratio