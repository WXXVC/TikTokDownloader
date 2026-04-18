import os
from pathlib import Path

from src.custom import SERVER_PORT


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCKER_APP_DATA_DIR = Path("/app/data")
LEGACY_DATA_DIR = PROJECT_ROOT / "NEWWEB" / "data"
DEFAULT_DATA_DIR = (
    DOCKER_APP_DATA_DIR
    if DOCKER_APP_DATA_DIR.exists()
    else LEGACY_DATA_DIR
    if LEGACY_DATA_DIR.exists()
    else PROJECT_ROOT / "Volume" / "newweb"
)
DATA_DIR = Path(
    os.getenv(
        "WEBUI_DATA_DIR",
        os.getenv("NEWWEB_DATA_DIR", DEFAULT_DATA_DIR),
    )
)
DATA_DIR.mkdir(parents=True, exist_ok=True)
TASK_LOG_DIR = DATA_DIR / "task_logs"
TASK_LOG_DIR.mkdir(parents=True, exist_ok=True)
TASK_RUNTIME_DIR = DATA_DIR / "task_runtime"
TASK_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
FRONTEND_DIR = PROJECT_ROOT / "static" / "webui"

APP_DB_PATH = DATA_DIR / "panel_store.json"
APP_SQLITE_PATH = DATA_DIR / "panel_store.sqlite3"
ENGINE_PROJECT_ROOT = PROJECT_ROOT
ENGINE_MAIN_PATH = ENGINE_PROJECT_ROOT / "main.py"
WORKER_DETAIL_PATH = PROJECT_ROOT / "src" / "webui" / "worker_detail_download.py"
WORKER_ISOLATED_MAIN_PATH = (
    PROJECT_ROOT / "src" / "webui" / "worker_isolated_main.py"
)
WORKER_ISOLATED_DETAIL_PATH = (
    PROJECT_ROOT / "src" / "webui" / "worker_isolated_detail.py"
)
WORKER_ISOLATED_BATCH_PATH = (
    PROJECT_ROOT / "src" / "webui" / "worker_isolated_batch.py"
)
ENGINE_VOLUME_PATH = Path(os.getenv("ENGINE_VOLUME_PATH", ENGINE_PROJECT_ROOT / "Volume"))
ENGINE_DB_PATH = ENGINE_VOLUME_PATH / "DouK-Downloader.db"
ENGINE_SETTINGS_PATH = ENGINE_VOLUME_PATH / "settings.json"
AUTO_DOWNLOAD_MAX_CONCURRENCY = max(
    1, int(os.getenv("AUTO_DOWNLOAD_MAX_CONCURRENCY", "1"))
)
DOWNLOADED_IDS_CACHE_SECONDS = max(
    1, int(os.getenv("DOWNLOADED_IDS_CACHE_SECONDS", "30"))
)
MAX_SCAN_CACHE_PER_CREATOR = max(
    1, int(os.getenv("MAX_SCAN_CACHE_PER_CREATOR", "3"))
)
AUTO_DOWNLOAD_TASK_MAX_CONCURRENCY = max(
    1, int(os.getenv("AUTO_DOWNLOAD_TASK_MAX_CONCURRENCY", "1"))
)
AUTO_DOWNLOAD_WORK_BATCH_SIZE = max(
    1, int(os.getenv("AUTO_DOWNLOAD_WORK_BATCH_SIZE", "20"))
)
TASK_DISPATCH_INTERVAL_SECONDS = max(
    1, int(os.getenv("TASK_DISPATCH_INTERVAL_SECONDS", "5"))
)
TASK_RUNTIME_RETENTION_SECONDS = max(
    60, int(os.getenv("TASK_RUNTIME_RETENTION_SECONDS", "1800"))
)
TASK_RUNTIME_CLEANUP_INTERVAL_SECONDS = max(
    5, int(os.getenv("TASK_RUNTIME_CLEANUP_INTERVAL_SECONDS", "30"))
)
ENGINE_API_DEFAULT_TIMEOUT_SECONDS = max(
    5, int(os.getenv("ENGINE_API_DEFAULT_TIMEOUT_SECONDS", "30"))
)
ENGINE_API_ACCOUNT_TIMEOUT_SECONDS = max(
    ENGINE_API_DEFAULT_TIMEOUT_SECONDS,
    int(os.getenv("ENGINE_API_ACCOUNT_TIMEOUT_SECONDS", "180")),
)

APP_NAME = "DouK-Downloader WebUI"
APP_VERSION = "0.1.0"


def get_engine_api_base() -> str:
    return os.getenv("ENGINE_API_BASE", f"http://127.0.0.1:{SERVER_PORT}/api")


def get_engine_api_token() -> str:
    return os.getenv("ENGINE_API_TOKEN", "")
