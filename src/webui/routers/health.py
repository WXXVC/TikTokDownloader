from fastapi import APIRouter

from ..config import APP_SQLITE_PATH, ENGINE_DB_PATH, ENGINE_SETTINGS_PATH
from ..schemas import DownloadHistorySummary, HealthResponse
from ..services.engine import read_downloaded_ids


router = APIRouter(prefix="/health", tags=["health"])


@router.get("", response_model=HealthResponse)
def health_check():
    return HealthResponse(
        status="ok",
        app_db_ready=APP_SQLITE_PATH.exists(),
        engine_db_found=ENGINE_DB_PATH.exists(),
        engine_settings_found=ENGINE_SETTINGS_PATH.exists(),
    )


@router.get("/history", response_model=DownloadHistorySummary)
def history_summary():
    return DownloadHistorySummary(total_downloaded_ids=len(read_downloaded_ids()))
