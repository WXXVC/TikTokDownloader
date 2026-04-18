from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ProfileBase(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    root_path: str = ""
    folder_name: str = "Download"
    name_format: str = "create_time type nickname desc"
    folder_mode: bool = False
    music: bool = False
    dynamic_cover: bool = False
    static_cover: bool = False
    enabled: bool = True


class ProfileCreate(ProfileBase):
    pass


class ProfileUpdate(ProfileBase):
    pass


class ProfileRead(ProfileBase):
    id: int
    created_at: datetime
    updated_at: datetime


class CreatorBase(BaseModel):
    platform: str
    name: str = Field(min_length=1, max_length=100)
    mark: str = ""
    url: str = Field(min_length=1)
    sec_user_id: str = ""
    tab: str = "post"
    enabled: bool = True
    profile_id: int | None = None
    auto_download_enabled: bool = False
    auto_download_interval_minutes: int = 0
    auto_download_start_at: datetime | None = None
    auto_download_last_run_at: datetime | None = None
    auto_download_next_run_at: datetime | None = None
    auto_download_last_status: str = ""
    auto_download_last_message: str = ""
    auto_download_history: list[dict[str, Any]] = Field(default_factory=list)


class CreatorCreate(CreatorBase):
    pass


class CreatorUpdate(CreatorBase):
    pass


class CreatorRead(CreatorBase):
    id: int
    created_at: datetime
    updated_at: datetime


class CreatorListItem(BaseModel):
    id: int
    platform: str
    name: str
    mark: str = ""
    url: str
    sec_user_id: str = ""
    tab: str = "post"
    enabled: bool = True
    profile_id: int | None = None
    auto_download_enabled: bool = False
    auto_download_interval_minutes: int = 0
    auto_download_start_at: datetime | None = None
    auto_download_last_run_at: datetime | None = None
    auto_download_next_run_at: datetime | None = None
    auto_download_last_status: str = ""
    auto_download_last_message: str = ""
    created_at: datetime
    updated_at: datetime


class CreatorPage(BaseModel):
    items: list[CreatorListItem] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 100


class HealthResponse(BaseModel):
    status: str
    app_db_ready: bool
    engine_db_found: bool
    engine_settings_found: bool


class EngineConfig(BaseModel):
    desc_length: int = 64
    name_length: int = 128
    date_format: str = "%Y-%m-%d %H:%M:%S"
    split: str = "-"
    truncate: int = 50
    storage_format: str = ""
    cookie: str = ""
    cookie_tiktok: str = ""
    proxy: str = ""
    proxy_tiktok: str = ""
    twc_tiktok: str = ""
    download: bool = True
    max_size: int = 0
    chunk: int = 1024 * 1024 * 2
    timeout: int = 10
    max_retry: int = 5
    max_pages: int = 0
    run_command: str = ""
    ffmpeg: str = ""
    live_qualities: str = ""
    douyin_platform: bool = True
    tiktok_platform: bool = True
    browser_info: dict[str, Any] = Field(default_factory=dict)
    browser_info_tiktok: dict[str, Any] = Field(default_factory=dict)


class PanelConfig(BaseModel):
    access_password: str = Field(default="151150", min_length=1)
    auto_download_scheduler_enabled: bool = False
    quick_add_profile_id: int = 1
    quick_add_tab: str = "post"
    quick_add_auto_download_enabled: bool = False
    quick_add_auto_download_interval_minutes: int = 0
    detail_fetch_concurrency: int = 2
    file_download_max_workers: int = 4
    initial_account_scan_max_pages: int = 0
    initial_account_scan_timeout_seconds: int = 180
    incremental_account_scan_max_pages: int = 0
    incremental_account_scan_timeout_seconds: int = 180
    auto_download_pause_mode: str = "works"
    auto_download_pause_after_works: int = 1000
    auto_download_pause_window_minutes: int = 30
    auto_download_pause_after_creators: int = 10
    auto_download_pause_minutes: int = 5
    auto_download_split_batches_enabled: bool = True
    auto_download_work_batch_size: int = 20
    risk_guard_enabled: bool = False
    risk_guard_cooldown_hours: int = 24
    risk_guard_http_error_streak: int = 3
    risk_guard_status_codes: str = "403,429"
    risk_guard_empty_download_streak: int = 3
    risk_guard_low_quality_streak: int = 3
    risk_guard_low_quality_ratio: float = 0.8
    risk_guard_low_quality_max_dimension: int = 720


class AutoDownloadThrottleStatus(BaseModel):
    works_count: int = 0
    creators_count: int = 0
    works_window_minutes: int = 30
    paused_until: datetime | None = None
    last_reason: str = ""
    is_paused: bool = False
    remaining_seconds: int = 0


class RiskGuardStatus(BaseModel):
    cooldown_until: datetime | None = None
    last_reason: str = ""
    last_triggered_at: datetime | None = None
    is_active: bool = False
    remaining_seconds: int = 0
    http_error_streak: int = 0
    empty_download_streak: int = 0
    low_quality_streak: int = 0


class DownloadHistorySummary(BaseModel):
    total_downloaded_ids: int


class CreatorQuickAddRequest(BaseModel):
    url: str = Field(min_length=1)


class CreatorQuickAddPreview(BaseModel):
    platform: str
    name: str
    mark: str
    url: str
    sec_user_id: str = ""
    tab: str = "post"
    profile_id: int = 1
    enabled: bool = True
    auto_download_enabled: bool = False
    auto_download_interval_minutes: int = 0


class CreatorQuickAddItemResult(BaseModel):
    url: str
    success: bool
    creator: CreatorRead | None = None
    error: str = ""


class CreatorQuickAddBatchResponse(BaseModel):
    total: int
    created_count: int
    failed_count: int
    items: list[CreatorQuickAddItemResult] = Field(default_factory=list)


class ScanItem(BaseModel):
    id: str
    title: str
    type: str
    published_at: str = ""
    is_downloaded: bool
    cover: str = ""
    share_url: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


class ScanResponse(BaseModel):
    creator_id: int
    creator_name: str
    source: str
    all_count: int
    downloaded_count: int
    undownloaded_count: int
    items: list[ScanItem]
    hidden_items: list[ScanItem] = Field(default_factory=list)


class ScanPageResponse(BaseModel):
    creator_id: int
    creator_name: str
    source: str
    all_count: int
    downloaded_count: int
    undownloaded_count: int
    total_visible: int
    page: int = 1
    page_size: int = 50
    available_types: list[str] = Field(default_factory=list)
    items: list[ScanItem] = Field(default_factory=list)


class DownloadTaskRead(BaseModel):
    id: int
    creator_id: int
    creator_name: str
    platform: str
    profile_id: int
    status: str
    mode: str
    item_count: int = 0
    run_command: str
    pid: int | None = None
    message: str = ""
    stdout_log: str = ""
    stderr_log: str = ""
    exit_code: int | None = None
    runtime_volume_path: str = ""
    runtime_volume_cleaned_at: datetime | None = None
    work_ids: list[str] = Field(default_factory=list)
    target_folder_name: str = ""
    created_at: datetime
    updated_at: datetime


class DownloadTaskListItem(BaseModel):
    id: int
    creator_id: int
    creator_name: str
    platform: str
    profile_id: int
    status: str
    mode: str
    item_count: int = 0
    run_command: str
    pid: int | None = None
    message: str = ""
    exit_code: int | None = None
    runtime_volume_cleaned_at: datetime | None = None
    target_folder_name: str = ""
    created_at: datetime
    updated_at: datetime


class DownloadTaskPage(BaseModel):
    items: list[DownloadTaskListItem] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 100


class RunningTaskCard(BaseModel):
    task_id: int
    creator_id: int
    creator_name: str
    platform: str = ""
    mode: str = ""
    total_count: int = 0
    completed_count: int = 0
    progress_percent: int = 0
    target_folder_name: str = ""
    message: str = ""


class TaskCenterSummaryItem(BaseModel):
    creator_id: int
    creator_name: str
    platform: str = ""
    mark: str = ""
    video_count: int = 0
    collection_count: int = 0
    live_count: int = 0
    failed_count: int = 0
    last_download_at: datetime | None = None


class TaskCenterSummaryPage(BaseModel):
    items: list[TaskCenterSummaryItem] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 100


class DownloadTaskCreate(BaseModel):
    creator_id: int
    profile_id: int | None = None


class DownloadWorksTaskCreate(BaseModel):
    creator_id: int
    work_ids: list[str] = Field(min_length=1)
