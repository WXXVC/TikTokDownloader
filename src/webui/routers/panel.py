from fastapi import APIRouter

from ..config import APP_SQLITE_PATH, ENGINE_DB_PATH, ENGINE_SETTINGS_PATH
from ..db import get_sqlite_table_digest
from ..schemas import AutoDownloadThrottleStatus, PanelConfig, RiskGuardStatus
from ..services import auto_download_runtime as auto_download_runtime_service
from ..services import auto_download_throttle as throttle_service
from ..services import creators as creator_service
from ..services import panel_config as panel_service
from ..services import risk_guard as risk_guard_service
from ..services import tasks as task_service


router = APIRouter(prefix="/panel", tags=["panel"])


@router.get("/config", response_model=PanelConfig)
def panel_config_get():
    return panel_service.read_panel_config()


@router.put("/config", response_model=PanelConfig)
def panel_config_update(payload: PanelConfig):
    config = panel_service.save_panel_config(payload.model_dump())
    auto_download_runtime_service.request_auto_download_wakeup()
    return config


@router.get("/auto-download-throttle", response_model=AutoDownloadThrottleStatus)
def auto_download_throttle_status():
    return throttle_service.get_auto_download_throttle_state()


@router.get("/risk-guard", response_model=RiskGuardStatus)
def risk_guard_status():
    return risk_guard_service.get_risk_guard_state()


@router.post("/risk-guard/reset", response_model=RiskGuardStatus)
def risk_guard_reset():
    state = risk_guard_service.reset_risk_guard_state()
    auto_download_runtime_service.request_auto_download_wakeup()
    return state


@router.get("/poll-state")
def panel_poll_state():
    return {
        "health": {
            "status": "ok",
            "app_db_ready": APP_SQLITE_PATH.exists(),
            "engine_db_found": ENGINE_DB_PATH.exists(),
            "engine_settings_found": ENGINE_SETTINGS_PATH.exists(),
        },
        "throttle": throttle_service.get_auto_download_throttle_state(),
        "risk_guard": risk_guard_service.get_risk_guard_state(),
        "manual_queue": auto_download_runtime_service.get_manual_queue_state(),
        "dashboard": creator_service.get_dashboard_summary(),
        "running_tasks": task_service.list_running_task_cards(),
        "digests": {
            "profiles": get_sqlite_table_digest("profiles"),
            "creators": get_sqlite_table_digest("creators"),
            "tasks": get_sqlite_table_digest("download_tasks"),
            "scan_cache": get_sqlite_table_digest("scan_cache"),
        },
    }
