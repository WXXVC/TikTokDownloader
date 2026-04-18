import json
from copy import deepcopy
from platform import system

from ..config import AUTO_DOWNLOAD_WORK_BATCH_SIZE, DATA_DIR


PANEL_CONFIG_PATH = DATA_DIR / "panel_config.json"
PANEL_CONFIG_ENCODING = "utf-8-sig" if system() == "Windows" else "utf-8"

PANEL_CONFIG_DEFAULTS = {
    "access_password": "151150",
    "auto_download_scheduler_enabled": False,
    "quick_add_profile_id": 1,
    "quick_add_tab": "post",
    "quick_add_auto_download_enabled": False,
    "quick_add_auto_download_interval_minutes": 0,
    "detail_fetch_concurrency": 2,
    "file_download_max_workers": 4,
    "initial_account_scan_max_pages": 0,
    "initial_account_scan_timeout_seconds": 180,
    "incremental_account_scan_max_pages": 0,
    "incremental_account_scan_timeout_seconds": 180,
    "auto_download_pause_mode": "works",
    "auto_download_pause_after_works": 1000,
    "auto_download_pause_window_minutes": 30,
    "auto_download_pause_after_creators": 10,
    "auto_download_pause_minutes": 5,
    "auto_download_split_batches_enabled": True,
    "auto_download_work_batch_size": AUTO_DOWNLOAD_WORK_BATCH_SIZE,
    "risk_guard_enabled": False,
    "risk_guard_cooldown_hours": 24,
    "risk_guard_http_error_streak": 3,
    "risk_guard_status_codes": "403,429",
    "risk_guard_empty_download_streak": 3,
    "risk_guard_low_quality_streak": 3,
    "risk_guard_low_quality_ratio": 0.8,
    "risk_guard_low_quality_max_dimension": 720,
}


def read_panel_config() -> dict:
    if not PANEL_CONFIG_PATH.exists():
        save_panel_config({})
    data = json.loads(PANEL_CONFIG_PATH.read_text(encoding=PANEL_CONFIG_ENCODING))
    changed = False
    legacy_scan_max_pages = data.get("account_scan_max_pages")
    legacy_scan_timeout_seconds = data.get("account_scan_timeout_seconds")
    if "initial_account_scan_max_pages" not in data:
        data["initial_account_scan_max_pages"] = (
            legacy_scan_max_pages
            if legacy_scan_max_pages is not None
            else PANEL_CONFIG_DEFAULTS["initial_account_scan_max_pages"]
        )
        changed = True
    if "initial_account_scan_timeout_seconds" not in data:
        data["initial_account_scan_timeout_seconds"] = (
            legacy_scan_timeout_seconds
            if legacy_scan_timeout_seconds is not None
            else PANEL_CONFIG_DEFAULTS["initial_account_scan_timeout_seconds"]
        )
        changed = True
    if "incremental_account_scan_max_pages" not in data:
        data["incremental_account_scan_max_pages"] = (
            legacy_scan_max_pages
            if legacy_scan_max_pages is not None
            else PANEL_CONFIG_DEFAULTS["incremental_account_scan_max_pages"]
        )
        changed = True
    if "incremental_account_scan_timeout_seconds" not in data:
        data["incremental_account_scan_timeout_seconds"] = (
            legacy_scan_timeout_seconds
            if legacy_scan_timeout_seconds is not None
            else PANEL_CONFIG_DEFAULTS["incremental_account_scan_timeout_seconds"]
        )
        changed = True
    for key, value in PANEL_CONFIG_DEFAULTS.items():
        if key not in data:
            data[key] = deepcopy(value)
            changed = True
    if changed:
        PANEL_CONFIG_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding=PANEL_CONFIG_ENCODING,
        )
    return data


def save_panel_config(payload: dict) -> dict:
    current = read_panel_config() if PANEL_CONFIG_PATH.exists() else deepcopy(PANEL_CONFIG_DEFAULTS)
    for key in PANEL_CONFIG_DEFAULTS:
        if key in payload:
            current[key] = payload[key]
    PANEL_CONFIG_PATH.write_text(
        json.dumps(current, ensure_ascii=False, indent=2),
        encoding=PANEL_CONFIG_ENCODING,
    )
    return current


def is_auto_download_scheduler_enabled() -> bool:
    return bool(read_panel_config().get("auto_download_scheduler_enabled"))


def is_auto_download_split_batches_enabled() -> bool:
    return bool(read_panel_config().get("auto_download_split_batches_enabled", True))


def get_detail_fetch_concurrency() -> int:
    return max(1, int(read_panel_config().get("detail_fetch_concurrency") or 2))


def get_file_download_max_workers() -> int:
    return max(1, int(read_panel_config().get("file_download_max_workers") or 4))
