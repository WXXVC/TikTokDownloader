import json
import sqlite3
import threading
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime

from .config import DATA_DIR


STORE_PATH = DATA_DIR / "panel_store.json"
SQLITE_PATH = DATA_DIR / "panel_store.sqlite3"
DEFAULT_STORE = {
    "meta": {
        "creator_next_id": 1,
        "profile_next_id": 2,
        "scan_next_id": 1,
        "task_next_id": 1,
    },
    "profiles": [
        {
            "id": 1,
            "name": "Default Profile",
            "root_path": "",
            "folder_name": "Download",
            "name_format": "create_time type nickname desc",
            "folder_mode": False,
            "music": False,
            "dynamic_cover": False,
            "static_cover": False,
            "enabled": True,
            "created_at": "2026-04-14T00:00:00",
            "updated_at": "2026-04-14T00:00:00",
        }
    ],
    "creators": [],
    "scan_cache": [],
    "download_tasks": [],
}

_DB_INIT_LOCK = threading.RLock()
_DB_READY = False


def _unique_items_by_id(items: list[dict], *, key_name: str = "id") -> list[dict]:
    unique: dict[object, dict] = {}
    ordered_without_id: list[dict] = []
    next_generated_id = 1
    for item in items or []:
        current = deepcopy(item)
        item_id = current.get(key_name)
        if item_id in (None, ""):
            while next_generated_id in unique:
                next_generated_id += 1
            current[key_name] = next_generated_id
            unique[next_generated_id] = current
            next_generated_id += 1
            continue
        unique[item_id] = current
    return list(unique.values()) + ordered_without_id


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


CREATOR_DEFAULTS = {
    "auto_download_enabled": False,
    "auto_download_interval_minutes": 0,
    "auto_download_start_at": None,
    "auto_download_last_run_at": None,
    "auto_download_next_run_at": None,
    "auto_download_last_status": "",
    "auto_download_last_message": "",
    "auto_download_history": [],
    "initial_scan_completed": False,
}


def ensure_database() -> None:
    global _DB_READY
    with _DB_INIT_LOCK:
        if _DB_READY and STORE_PATH.exists() and SQLITE_PATH.exists():
            return
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if not STORE_PATH.exists():
            store = deepcopy(DEFAULT_STORE)
            STORE_PATH.write_text(
                json.dumps(store, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            ensure_sqlite_database(store)
            _DB_READY = True
            return
        store = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        changed = False
        for key, value in DEFAULT_STORE.items():
            if key not in store:
                store[key] = deepcopy(value)
                changed = True
        for key, value in DEFAULT_STORE["meta"].items():
            if key not in store["meta"]:
                store["meta"][key] = value
                changed = True
        for task in store.get("download_tasks", []):
            for key, value in {
                "stdout_log": "",
                "stderr_log": "",
                "exit_code": None,
            }.items():
                if key not in task:
                    task[key] = value
                    changed = True
        scan_cache_creator_ids = {
            int(item.get("creator_id") or 0)
            for item in store.get("scan_cache", [])
            if int(item.get("creator_id") or 0) > 0
        }
        for creator in store.get("creators", []):
            for key, value in CREATOR_DEFAULTS.items():
                if key not in creator:
                    if key == "initial_scan_completed":
                        creator[key] = (
                            int(creator.get("id") or 0) in scan_cache_creator_ids
                        )
                    else:
                        creator[key] = deepcopy(value)
                    changed = True
        normalized_profiles = _unique_items_by_id(store.get("profiles", []))
        if normalized_profiles != store.get("profiles", []):
            store["profiles"] = normalized_profiles
            changed = True
        normalized_creators = _unique_items_by_id(store.get("creators", []))
        if normalized_creators != store.get("creators", []):
            store["creators"] = normalized_creators
            changed = True
        normalized_tasks = _unique_items_by_id(store.get("download_tasks", []))
        if normalized_tasks != store.get("download_tasks", []):
            store["download_tasks"] = normalized_tasks
            changed = True
        normalized_scan_cache = _unique_items_by_id(store.get("scan_cache", []))
        if normalized_scan_cache != store.get("scan_cache", []):
            store["scan_cache"] = normalized_scan_cache
            changed = True
        if changed:
            STORE_PATH.write_text(
                json.dumps(store, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        ensure_sqlite_database(store)
        _DB_READY = True


def load_store() -> dict:
    ensure_database()
    return json.loads(STORE_PATH.read_text(encoding="utf-8"))


def save_store(store: dict) -> None:
    global _DB_READY
    STORE_PATH.write_text(
        json.dumps(store, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _DB_READY = False


@contextmanager
def get_store():
    store = load_store()
    yield store
    save_store(store)


def clone_default_store() -> dict:
    return deepcopy(DEFAULT_STORE)


@contextmanager
def _connect_sqlite():
    conn = sqlite3.connect(SQLITE_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=TRUNCATE")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        yield conn
    finally:
        conn.close()


def _table_empty(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(f"SELECT COUNT(1) AS count FROM {table}").fetchone()
    return not row or row["count"] == 0


def _should_rebuild_sqlite(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "disk i/o error" in message or "database disk image is malformed" in message


def _backup_broken_sqlite() -> None:
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    candidates = [
        SQLITE_PATH,
        SQLITE_PATH.with_name(f"{SQLITE_PATH.name}-journal"),
        SQLITE_PATH.with_name(f"{SQLITE_PATH.name}-wal"),
        SQLITE_PATH.with_name(f"{SQLITE_PATH.name}-shm"),
    ]
    for path in candidates:
        if path.exists():
            backup_path = path.with_name(f"{path.name}.broken.{timestamp}")
            try:
                path.replace(backup_path)
            except PermissionError:
                continue


def _sqlite_sidecar_paths() -> list:
    return [
        SQLITE_PATH,
        SQLITE_PATH.with_name(f"{SQLITE_PATH.name}-journal"),
        SQLITE_PATH.with_name(f"{SQLITE_PATH.name}-wal"),
        SQLITE_PATH.with_name(f"{SQLITE_PATH.name}-shm"),
    ]


def _should_reset_sqlite_files() -> bool:
    if not SQLITE_PATH.exists():
        return False
    try:
        if SQLITE_PATH.stat().st_size == 0:
            return True
    except OSError:
        return True
    return False


def _reset_sqlite_files() -> None:
    for path in reversed(_sqlite_sidecar_paths()):
        if not path.exists():
            continue
        try:
            path.unlink()
        except PermissionError:
            continue


def _normalize_seed_store(seed_store: dict) -> dict:
    store = deepcopy(seed_store or DEFAULT_STORE)
    for key, value in DEFAULT_STORE.items():
        if key not in store:
            store[key] = deepcopy(value)
    if "meta" not in store:
        store["meta"] = deepcopy(DEFAULT_STORE["meta"])
    for key, value in DEFAULT_STORE["meta"].items():
        store["meta"].setdefault(key, value)
    store["profiles"] = _unique_items_by_id(store.get("profiles", []))
    store["creators"] = _unique_items_by_id(store.get("creators", []))
    store["download_tasks"] = _unique_items_by_id(store.get("download_tasks", []))
    store["scan_cache"] = _unique_items_by_id(store.get("scan_cache", []))
    return store


def _bootstrap_sqlite(seed_store: dict) -> None:
    with _connect_sqlite() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS creators (
                id INTEGER PRIMARY KEY,
                profile_id INTEGER NOT NULL,
                enabled INTEGER NOT NULL,
                auto_download_enabled INTEGER NOT NULL,
                auto_download_next_run_at TEXT,
                updated_at TEXT NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS download_tasks (
                id INTEGER PRIMARY KEY,
                creator_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scan_cache (
                id INTEGER PRIMARY KEY,
                creator_id INTEGER NOT NULL,
                scanned_at TEXT NOT NULL,
                source TEXT NOT NULL,
                total_count INTEGER NOT NULL,
                undownloaded_count INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_center_cache (
                creator_id INTEGER PRIMARY KEY,
                video_count INTEGER NOT NULL DEFAULT 0,
                collection_count INTEGER NOT NULL DEFAULT 0,
                live_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                last_download_at TEXT,
                downloaded_work_ids_hash TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_creators_auto_schedule
            ON creators (enabled, auto_download_enabled, auto_download_next_run_at, id);
            CREATE INDEX IF NOT EXISTS idx_tasks_creator_status
            ON download_tasks (creator_id, status, id);
            CREATE INDEX IF NOT EXISTS idx_tasks_mode_status
            ON download_tasks (json_extract(data, '$.mode'), status, id);
            CREATE INDEX IF NOT EXISTS idx_scan_cache_creator_id
            ON scan_cache (creator_id, id DESC);
            """
        )
        if _table_empty(conn, "profiles"):
            for item in seed_store.get("profiles", []):
                conn.execute(
                    """
                    INSERT INTO profiles (id, name, enabled, updated_at, data)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        enabled = excluded.enabled,
                        updated_at = excluded.updated_at,
                        data = excluded.data
                    """,
                    (
                        item["id"],
                        item["name"],
                        1 if item.get("enabled", True) else 0,
                        item.get("updated_at") or now_iso(),
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
        if _table_empty(conn, "creators"):
            for item in seed_store.get("creators", []):
                conn.execute(
                    """
                    INSERT INTO creators (id, profile_id, enabled, auto_download_enabled, auto_download_next_run_at, updated_at, data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        profile_id = excluded.profile_id,
                        enabled = excluded.enabled,
                        auto_download_enabled = excluded.auto_download_enabled,
                        auto_download_next_run_at = excluded.auto_download_next_run_at,
                        updated_at = excluded.updated_at,
                        data = excluded.data
                    """,
                    (
                        item["id"],
                        item.get("profile_id") or 1,
                        1 if item.get("enabled", True) else 0,
                        1 if item.get("auto_download_enabled") else 0,
                        item.get("auto_download_next_run_at"),
                        item.get("updated_at") or now_iso(),
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
        if _table_empty(conn, "download_tasks"):
            for item in seed_store.get("download_tasks", []):
                conn.execute(
                    """
                    INSERT INTO download_tasks (id, creator_id, status, created_at, updated_at, data)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        creator_id = excluded.creator_id,
                        status = excluded.status,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        data = excluded.data
                    """,
                    (
                        item["id"],
                        item["creator_id"],
                        item.get("status", ""),
                        item.get("created_at") or now_iso(),
                        item.get("updated_at") or now_iso(),
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
        if _table_empty(conn, "scan_cache"):
            for item in seed_store.get("scan_cache", []):
                conn.execute(
                    """
                    INSERT INTO scan_cache (id, creator_id, scanned_at, source, total_count, undownloaded_count, payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        creator_id = excluded.creator_id,
                        scanned_at = excluded.scanned_at,
                        source = excluded.source,
                        total_count = excluded.total_count,
                        undownloaded_count = excluded.undownloaded_count,
                        payload = excluded.payload
                    """,
                    (
                        item["id"],
                        item["creator_id"],
                        item.get("scanned_at") or now_iso(),
                        item.get("source", "scan_cache"),
                        item.get("total_count", 0),
                        item.get("undownloaded_count", 0),
                        json.dumps(item.get("payload", []), ensure_ascii=False),
                    ),
                )
        conn.commit()


def ensure_sqlite_database(seed_store: dict | None = None) -> None:
    with _DB_INIT_LOCK:
        if seed_store is None:
            seed_store = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        seed_store = _normalize_seed_store(seed_store)
        if _should_reset_sqlite_files():
            _reset_sqlite_files()
        try:
            _bootstrap_sqlite(seed_store)
        except sqlite3.IntegrityError:
            _backup_broken_sqlite()
            if _should_reset_sqlite_files():
                _reset_sqlite_files()
            _bootstrap_sqlite(seed_store)
        except sqlite3.OperationalError as exc:
            if not _should_rebuild_sqlite(exc):
                raise
            _backup_broken_sqlite()
            if _should_reset_sqlite_files():
                _reset_sqlite_files()
            _bootstrap_sqlite(seed_store)


def _json_row(row: sqlite3.Row | None, column: str = "data") -> dict | None:
    if not row:
        return None
    return json.loads(row[column])


def list_sqlite_creators() -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute("SELECT data FROM creators ORDER BY id DESC").fetchall()
    return [json.loads(row["data"]) for row in rows]


def list_sqlite_creator_summaries() -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                json_extract(data, '$.platform') AS platform,
                json_extract(data, '$.name') AS name,
                COALESCE(json_extract(data, '$.mark'), '') AS mark,
                json_extract(data, '$.url') AS url,
                COALESCE(json_extract(data, '$.sec_user_id'), '') AS sec_user_id,
                COALESCE(json_extract(data, '$.tab'), 'post') AS tab,
                enabled,
                profile_id,
                auto_download_enabled,
                COALESCE(json_extract(data, '$.auto_download_interval_minutes'), 0) AS auto_download_interval_minutes,
                json_extract(data, '$.auto_download_start_at') AS auto_download_start_at,
                json_extract(data, '$.auto_download_last_run_at') AS auto_download_last_run_at,
                auto_download_next_run_at,
                COALESCE(json_extract(data, '$.auto_download_last_status'), '') AS auto_download_last_status,
                COALESCE(json_extract(data, '$.auto_download_last_message'), '') AS auto_download_last_message,
                json_extract(data, '$.created_at') AS created_at,
                updated_at
            FROM creators
            ORDER BY id DESC
            """
        ).fetchall()
    return [
        {
            "id": row["id"],
            "platform": row["platform"] or "",
            "name": row["name"] or "",
            "mark": row["mark"] or "",
            "url": row["url"] or "",
            "sec_user_id": row["sec_user_id"] or "",
            "tab": row["tab"] or "post",
            "enabled": bool(row["enabled"]),
            "profile_id": row["profile_id"],
            "auto_download_enabled": bool(row["auto_download_enabled"]),
            "auto_download_interval_minutes": int(
                row["auto_download_interval_minutes"] or 0
            ),
            "auto_download_start_at": row["auto_download_start_at"],
            "auto_download_last_run_at": row["auto_download_last_run_at"],
            "auto_download_next_run_at": row["auto_download_next_run_at"],
            "auto_download_last_status": row["auto_download_last_status"] or "",
            "auto_download_last_message": row["auto_download_last_message"] or "",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }
        for row in rows
    ]


def list_sqlite_creator_summaries_paginated(
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    platform: str = "",
    profile_id: int | None = None,
    enabled: str = "",
    auto_enabled: str = "",
    download_status: str = "",
    get_creator_download_status=None,
) -> dict:
    ensure_database()
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 10)))
    clauses: list[str] = []
    params: list[object] = []

    keyword_text = str(keyword or "").strip().lower()
    if keyword_text:
        clauses.append(
            """(
                lower(COALESCE(json_extract(data, '$.name'), '')) LIKE ?
                OR lower(COALESCE(json_extract(data, '$.mark'), '')) LIKE ?
                OR lower(COALESCE(json_extract(data, '$.url'), '')) LIKE ?
                OR lower(COALESCE(json_extract(data, '$.sec_user_id'), '')) LIKE ?
            )"""
        )
        like_value = f"%{keyword_text}%"
        params.extend([like_value, like_value, like_value, like_value])

    if platform:
        clauses.append("COALESCE(json_extract(data, '$.platform'), '') = ?")
        params.append(platform)

    if profile_id is not None:
        clauses.append("profile_id = ?")
        params.append(int(profile_id))

    if enabled in {"true", "false"}:
        clauses.append("enabled = ?")
        params.append(1 if enabled == "true" else 0)

    if auto_enabled in {"true", "false"}:
        clauses.append("auto_download_enabled = ?")
        params.append(1 if auto_enabled == "true" else 0)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    offset = (page - 1) * page_size
    with _connect_sqlite() as conn:
        if download_status and get_creator_download_status:
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    json_extract(data, '$.platform') AS platform,
                    json_extract(data, '$.name') AS name,
                    COALESCE(json_extract(data, '$.mark'), '') AS mark,
                    json_extract(data, '$.url') AS url,
                    COALESCE(json_extract(data, '$.sec_user_id'), '') AS sec_user_id,
                    COALESCE(json_extract(data, '$.tab'), 'post') AS tab,
                    enabled,
                    profile_id,
                    auto_download_enabled,
                    COALESCE(json_extract(data, '$.auto_download_interval_minutes'), 0) AS auto_download_interval_minutes,
                    json_extract(data, '$.auto_download_start_at') AS auto_download_start_at,
                    json_extract(data, '$.auto_download_last_run_at') AS auto_download_last_run_at,
                    auto_download_next_run_at,
                    COALESCE(json_extract(data, '$.auto_download_last_status'), '') AS auto_download_last_status,
                    COALESCE(json_extract(data, '$.auto_download_last_message'), '') AS auto_download_last_message,
                    json_extract(data, '$.created_at') AS created_at,
                    updated_at
                FROM creators
                {where_clause}
                ORDER BY id DESC
                """,
                tuple(params),
            ).fetchall()
            total_row = None
        else:
            total_row = conn.execute(
                f"SELECT COUNT(1) AS count FROM creators {where_clause}",
                tuple(params),
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    json_extract(data, '$.platform') AS platform,
                    json_extract(data, '$.name') AS name,
                    COALESCE(json_extract(data, '$.mark'), '') AS mark,
                    json_extract(data, '$.url') AS url,
                    COALESCE(json_extract(data, '$.sec_user_id'), '') AS sec_user_id,
                    COALESCE(json_extract(data, '$.tab'), 'post') AS tab,
                    enabled,
                    profile_id,
                    auto_download_enabled,
                    COALESCE(json_extract(data, '$.auto_download_interval_minutes'), 0) AS auto_download_interval_minutes,
                    json_extract(data, '$.auto_download_start_at') AS auto_download_start_at,
                    json_extract(data, '$.auto_download_last_run_at') AS auto_download_last_run_at,
                    auto_download_next_run_at,
                    COALESCE(json_extract(data, '$.auto_download_last_status'), '') AS auto_download_last_status,
                    COALESCE(json_extract(data, '$.auto_download_last_message'), '') AS auto_download_last_message,
                    json_extract(data, '$.created_at') AS created_at,
                    updated_at
                FROM creators
                {where_clause}
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, page_size, offset),
            ).fetchall()

    items = [
        {
            "id": row["id"],
            "platform": row["platform"] or "",
            "name": row["name"] or "",
            "mark": row["mark"] or "",
            "url": row["url"] or "",
            "sec_user_id": row["sec_user_id"] or "",
            "tab": row["tab"] or "post",
            "enabled": bool(row["enabled"]),
            "profile_id": row["profile_id"],
            "auto_download_enabled": bool(row["auto_download_enabled"]),
            "auto_download_interval_minutes": int(
                row["auto_download_interval_minutes"] or 0
            ),
            "auto_download_start_at": row["auto_download_start_at"],
            "auto_download_last_run_at": row["auto_download_last_run_at"],
            "auto_download_next_run_at": row["auto_download_next_run_at"],
            "auto_download_last_status": row["auto_download_last_status"] or "",
            "auto_download_last_message": row["auto_download_last_message"] or "",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }
        for row in rows
    ]

    if download_status and get_creator_download_status:
        download_status_text = str(download_status or "").strip().lower()
        if download_status_text in {"queued", "scanning", "downloading", "idle"}:
            items = [
                item
                for item in items
                if get_creator_download_status(item["id"]) == download_status_text
            ]
        total = len(items)
        paged_items = items[offset : offset + page_size]
    else:
        total = int(total_row["count"] or 0) if total_row else len(items)
        paged_items = items

    return {
        "items": paged_items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


def list_sqlite_creator_options() -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                COALESCE(json_extract(data, '$.name'), '') AS name,
                COALESCE(json_extract(data, '$.mark'), '') AS mark,
                enabled
            FROM creators
            ORDER BY enabled DESC, id DESC
            """
        ).fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"] or "",
            "mark": row["mark"] or "",
            "enabled": bool(row["enabled"]),
        }
        for row in rows
    ]


def get_sqlite_dashboard_summary() -> dict:
    ensure_database()
    with _connect_sqlite() as conn:
        creator_stats = conn.execute(
            """
            SELECT
                SUM(CASE WHEN auto_download_enabled = 1 THEN 1 ELSE 0 END) AS auto_enabled_count,
                SUM(CASE WHEN auto_download_enabled = 1
                          AND COALESCE(json_extract(data, '$.auto_download_last_status'), '') = 'failed'
                    THEN 1 ELSE 0 END) AS auto_failed_count
            FROM creators
            """
        ).fetchone()
        next_creator_row = conn.execute(
            """
            SELECT
                id,
                COALESCE(json_extract(data, '$.name'), '') AS name,
                COALESCE(json_extract(data, '$.mark'), '') AS mark,
                auto_download_next_run_at
            FROM creators
            WHERE auto_download_enabled = 1
              AND enabled = 1
              AND auto_download_next_run_at IS NOT NULL
              AND auto_download_next_run_at != ''
            ORDER BY auto_download_next_run_at ASC, id ASC
            LIMIT 1
            """
        ).fetchone()
        task_stats = conn.execute(
            """
            SELECT COUNT(1) AS running_auto_tasks
            FROM download_tasks
            WHERE status = 'running'
              AND COALESCE(json_extract(data, '$.mode'), '') IN ('auto_detail_download', 'auto_creator_batch_download')
            """
        ).fetchone()
    return {
        "auto_enabled_count": int(creator_stats["auto_enabled_count"] or 0)
        if creator_stats
        else 0,
        "auto_failed_count": int(creator_stats["auto_failed_count"] or 0)
        if creator_stats
        else 0,
        "running_auto_tasks": int(task_stats["running_auto_tasks"] or 0)
        if task_stats
        else 0,
        "next_creator": (
            {
                "id": next_creator_row["id"],
                "name": next_creator_row["name"] or "",
                "mark": next_creator_row["mark"] or "",
                "auto_download_next_run_at": next_creator_row[
                    "auto_download_next_run_at"
                ]
                or "",
            }
            if next_creator_row
            else None
        ),
    }


def get_sqlite_table_digest(table: str) -> dict:
    ensure_database()
    if table not in {"creators", "profiles", "download_tasks", "scan_cache"}:
        raise ValueError(f"Unsupported sqlite table: {table}")
    with _connect_sqlite() as conn:
        row = conn.execute(
            f"SELECT COUNT(1) AS count, COALESCE(MAX(updated_at), '') AS updated_at FROM {table}"
            if table in {"creators", "profiles", "download_tasks"}
            else f"SELECT COUNT(1) AS count, COALESCE(MAX(scanned_at), '') AS updated_at FROM {table}"
        ).fetchone()
    return {
        "count": int(row["count"]) if row else 0,
        "updated_at": row["updated_at"] if row else "",
    }


def list_due_sqlite_creators(now_value: str, limit: int | None = None) -> list[dict]:
    ensure_database()
    sql = """
        SELECT data
        FROM creators
        WHERE enabled = 1
          AND auto_download_enabled = 1
          AND (auto_download_next_run_at IS NULL OR auto_download_next_run_at = '' OR auto_download_next_run_at <= ?)
        ORDER BY auto_download_next_run_at IS NULL DESC, auto_download_next_run_at ASC, id ASC
    """
    params: tuple = (now_value,)
    if limit is not None:
        sql += " LIMIT ?"
        params = (now_value, limit)
    with _connect_sqlite() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [json.loads(row["data"]) for row in rows]


def list_sqlite_profiles() -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute("SELECT data FROM profiles ORDER BY id").fetchall()
    return [json.loads(row["data"]) for row in rows]


def get_sqlite_profile(profile_id: int) -> dict | None:
    ensure_database()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT data FROM profiles WHERE id = ?", (profile_id,)
        ).fetchone()
    return _json_row(row)


def save_sqlite_profile(item: dict) -> dict:
    ensure_database()
    with _connect_sqlite() as conn:
        if item.get("id") is None:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM profiles"
            ).fetchone()
            item["id"] = row["next_id"]
        conn.execute(
            """
            INSERT INTO profiles (id, name, enabled, updated_at, data)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                enabled = excluded.enabled,
                updated_at = excluded.updated_at,
                data = excluded.data
            """,
            (
                item["id"],
                item["name"],
                1 if item.get("enabled", True) else 0,
                item.get("updated_at") or now_iso(),
                json.dumps(item, ensure_ascii=False),
            ),
        )
        conn.commit()
    return item


def delete_sqlite_profile(profile_id: int) -> None:
    ensure_database()
    with _connect_sqlite() as conn:
        conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
        conn.commit()


def get_sqlite_creator(creator_id: int) -> dict | None:
    ensure_database()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT data FROM creators WHERE id = ?", (creator_id,)
        ).fetchone()
    return _json_row(row)


def save_sqlite_creator(item: dict) -> dict:
    ensure_database()
    with _connect_sqlite() as conn:
        if item.get("id") is None:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM creators"
            ).fetchone()
            item["id"] = row["next_id"]
        conn.execute(
            """
            INSERT INTO creators (id, profile_id, enabled, auto_download_enabled, auto_download_next_run_at, updated_at, data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                profile_id = excluded.profile_id,
                enabled = excluded.enabled,
                auto_download_enabled = excluded.auto_download_enabled,
                auto_download_next_run_at = excluded.auto_download_next_run_at,
                updated_at = excluded.updated_at,
                data = excluded.data
            """,
            (
                item["id"],
                item.get("profile_id") or 1,
                1 if item.get("enabled", True) else 0,
                1 if item.get("auto_download_enabled") else 0,
                item.get("auto_download_next_run_at"),
                item.get("updated_at") or now_iso(),
                json.dumps(item, ensure_ascii=False),
            ),
        )
        conn.commit()
    return item


def delete_sqlite_creator(creator_id: int) -> None:
    ensure_database()
    with _connect_sqlite() as conn:
        conn.execute("DELETE FROM creators WHERE id = ?", (creator_id,))
        conn.execute("DELETE FROM scan_cache WHERE creator_id = ?", (creator_id,))
        conn.commit()


def update_sqlite_creator_profile(profile_id: int, new_profile_id: int) -> None:
    ensure_database()
    creators = list_sqlite_creators()
    for item in creators:
        if item.get("profile_id") == profile_id:
            item["profile_id"] = new_profile_id
            item["updated_at"] = now_iso()
            save_sqlite_creator(item)


def update_sqlite_creator_sec_user_id(creator_id: int, sec_user_id: str) -> None:
    item = get_sqlite_creator(creator_id)
    if not item:
        return
    item["sec_user_id"] = sec_user_id
    item["updated_at"] = now_iso()
    save_sqlite_creator(item)


def list_sqlite_tasks() -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute(
            "SELECT data FROM download_tasks ORDER BY id DESC"
        ).fetchall()
    return [json.loads(row["data"]) for row in rows]


def list_sqlite_task_summaries() -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                creator_id,
                COALESCE(json_extract(data, '$.creator_name'), '') AS creator_name,
                COALESCE(json_extract(data, '$.platform'), '') AS platform,
                COALESCE(json_extract(data, '$.profile_id'), 1) AS profile_id,
                status,
                COALESCE(json_extract(data, '$.mode'), '') AS mode,
                COALESCE(json_extract(data, '$.item_count'), 0) AS item_count,
                COALESCE(json_extract(data, '$.run_command'), '') AS run_command,
                json_extract(data, '$.pid') AS pid,
                COALESCE(json_extract(data, '$.message'), '') AS message,
                json_extract(data, '$.exit_code') AS exit_code,
                json_extract(data, '$.runtime_volume_cleaned_at') AS runtime_volume_cleaned_at,
                COALESCE(json_extract(data, '$.target_folder_name'), '') AS target_folder_name,
                created_at,
                updated_at
            FROM download_tasks
            ORDER BY id DESC
            """
        ).fetchall()
    return [
        {
            "id": row["id"],
            "creator_id": row["creator_id"],
            "creator_name": row["creator_name"] or "",
            "platform": row["platform"] or "",
            "profile_id": int(row["profile_id"] or 1),
            "status": row["status"] or "",
            "mode": row["mode"] or "",
            "item_count": int(row["item_count"] or 0),
            "run_command": row["run_command"] or "",
            "pid": row["pid"],
            "message": row["message"] or "",
            "exit_code": row["exit_code"],
            "runtime_volume_cleaned_at": row["runtime_volume_cleaned_at"],
            "target_folder_name": row["target_folder_name"] or "",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }
        for row in rows
    ]


def list_sqlite_task_summaries_paginated(
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    status: str = "",
    mode: str = "",
    kind: str = "",
) -> dict:
    ensure_database()
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 10)))
    clauses: list[str] = []
    params: list[object] = []

    keyword_text = str(keyword or "").strip().lower()
    if keyword_text:
        clauses.append(
            """(
                lower(COALESCE(json_extract(data, '$.creator_name'), '')) LIKE ?
                OR lower(COALESCE(json_extract(data, '$.run_command'), '')) LIKE ?
                OR lower(COALESCE(json_extract(data, '$.message'), '')) LIKE ?
                OR lower(COALESCE(json_extract(data, '$.platform'), '')) LIKE ?
            )"""
        )
        like_value = f"%{keyword_text}%"
        params.extend([like_value, like_value, like_value, like_value])

    if status:
        clauses.append("status = ?")
        params.append(status)

    if mode:
        clauses.append("COALESCE(json_extract(data, '$.mode'), '') = ?")
        params.append(mode)

    if kind == "auto":
        clauses.append(
            "COALESCE(json_extract(data, '$.mode'), '') IN ('auto_detail_download', 'auto_creator_batch_download')"
        )
    elif kind == "manual":
        clauses.append(
            "COALESCE(json_extract(data, '$.mode'), '') NOT IN ('auto_detail_download', 'auto_creator_batch_download')"
        )

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    offset = (page - 1) * page_size
    with _connect_sqlite() as conn:
        total_row = conn.execute(
            f"SELECT COUNT(1) AS count FROM download_tasks {where_clause}",
            tuple(params),
        ).fetchone()
        rows = conn.execute(
            f"""
            SELECT
                id,
                creator_id,
                COALESCE(json_extract(data, '$.creator_name'), '') AS creator_name,
                COALESCE(json_extract(data, '$.platform'), '') AS platform,
                COALESCE(json_extract(data, '$.profile_id'), 1) AS profile_id,
                status,
                COALESCE(json_extract(data, '$.mode'), '') AS mode,
                COALESCE(json_extract(data, '$.item_count'), 0) AS item_count,
                COALESCE(json_extract(data, '$.run_command'), '') AS run_command,
                json_extract(data, '$.pid') AS pid,
                COALESCE(json_extract(data, '$.message'), '') AS message,
                json_extract(data, '$.exit_code') AS exit_code,
                json_extract(data, '$.runtime_volume_cleaned_at') AS runtime_volume_cleaned_at,
                COALESCE(json_extract(data, '$.target_folder_name'), '') AS target_folder_name,
                created_at,
                updated_at
            FROM download_tasks
            {where_clause}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, page_size, offset),
        ).fetchall()
    return {
        "items": [
            {
                "id": row["id"],
                "creator_id": row["creator_id"],
                "creator_name": row["creator_name"] or "",
                "platform": row["platform"] or "",
                "profile_id": int(row["profile_id"] or 1),
                "status": row["status"] or "",
                "mode": row["mode"] or "",
                "item_count": int(row["item_count"] or 0),
                "run_command": row["run_command"] or "",
                "pid": row["pid"],
                "message": row["message"] or "",
                "exit_code": row["exit_code"],
                "runtime_volume_cleaned_at": row["runtime_volume_cleaned_at"],
                "target_folder_name": row["target_folder_name"] or "",
                "created_at": row["created_at"] or "",
                "updated_at": row["updated_at"] or "",
            }
            for row in rows
        ],
        "total": int(total_row["count"]) if total_row else 0,
        "page": page,
        "page_size": page_size,
    }


def count_sqlite_tasks(
    *, mode: str | None = None, status: str | None = None, creator_id: int | None = None
) -> int:
    ensure_database()
    clauses = []
    params: list[object] = []
    if mode is not None:
        clauses.append("json_extract(data, '$.mode') = ?")
        params.append(mode)
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    if creator_id is not None:
        clauses.append("creator_id = ?")
        params.append(creator_id)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect_sqlite() as conn:
        row = conn.execute(
            f"SELECT COUNT(1) AS count FROM download_tasks {where_clause}",
            tuple(params),
        ).fetchone()
    return int(row["count"]) if row else 0


def list_sqlite_tasks_by_statuses(
    *,
    mode: str | None = None,
    creator_id: int | None = None,
    statuses: tuple[str, ...] = (),
    limit: int | None = None,
    order_asc: bool = False,
) -> list[dict]:
    ensure_database()
    clauses = []
    params: list[object] = []
    if mode is not None:
        clauses.append("json_extract(data, '$.mode') = ?")
        params.append(mode)
    if creator_id is not None:
        clauses.append("creator_id = ?")
        params.append(creator_id)
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order_clause = "ORDER BY id ASC" if order_asc else "ORDER BY id DESC"
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)
    with _connect_sqlite() as conn:
        rows = conn.execute(
            f"SELECT data FROM download_tasks {where_clause} {order_clause}{limit_clause}",
            tuple(params),
        ).fetchall()
    return [json.loads(row["data"]) for row in rows]


def get_sqlite_task(task_id: int) -> dict | None:
    ensure_database()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT data FROM download_tasks WHERE id = ?", (task_id,)
        ).fetchone()
    return _json_row(row)


def save_sqlite_task(item: dict) -> dict:
    ensure_database()
    with _connect_sqlite() as conn:
        if item.get("id") is None:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM download_tasks"
            ).fetchone()
            item["id"] = row["next_id"]
        conn.execute(
            """
            INSERT INTO download_tasks (id, creator_id, status, created_at, updated_at, data)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                creator_id = excluded.creator_id,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                data = excluded.data
            """,
            (
                item["id"],
                item["creator_id"],
                item.get("status", ""),
                item.get("created_at") or now_iso(),
                item.get("updated_at") or now_iso(),
                json.dumps(item, ensure_ascii=False),
            ),
        )
        conn.commit()
    return item


def next_sqlite_task_id() -> int:
    ensure_database()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM download_tasks"
        ).fetchone()
    return row["next_id"]


def delete_sqlite_tasks_by_creator(creator_id: int) -> int:
    ensure_database()
    with _connect_sqlite() as conn:
        cursor = conn.execute(
            "DELETE FROM download_tasks WHERE creator_id = ?", (creator_id,)
        )
        conn.commit()
    return int(cursor.rowcount or 0)


def list_sqlite_scan_cache(creator_id: int | None = None) -> list[dict]:
    ensure_database()
    with _connect_sqlite() as conn:
        if creator_id is None:
            rows = conn.execute("SELECT * FROM scan_cache ORDER BY id DESC").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM scan_cache WHERE creator_id = ? ORDER BY id DESC",
                (creator_id,),
            ).fetchall()
    return [
        {
            "id": row["id"],
            "creator_id": row["creator_id"],
            "scanned_at": row["scanned_at"],
            "source": row["source"],
            "total_count": row["total_count"],
            "undownloaded_count": row["undownloaded_count"],
            "payload": json.loads(row["payload"]),
        }
        for row in rows
    ]


def save_sqlite_scan_cache(item: dict) -> dict:
    ensure_database()
    with _connect_sqlite() as conn:
        if item.get("id") is None:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM scan_cache"
            ).fetchone()
            item["id"] = row["next_id"]
        conn.execute(
            """
            INSERT INTO scan_cache (id, creator_id, scanned_at, source, total_count, undownloaded_count, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                creator_id = excluded.creator_id,
                scanned_at = excluded.scanned_at,
                source = excluded.source,
                total_count = excluded.total_count,
                undownloaded_count = excluded.undownloaded_count,
                payload = excluded.payload
            """,
            (
                item["id"],
                item["creator_id"],
                item.get("scanned_at") or now_iso(),
                item.get("source", "scan_cache"),
                item.get("total_count", 0),
                item.get("undownloaded_count", 0),
                json.dumps(item.get("payload", []), ensure_ascii=False),
            ),
        )
        conn.commit()
    return item


def delete_sqlite_scan_cache_for_creator(creator_id: int) -> None:
    ensure_database()
    with _connect_sqlite() as conn:
        conn.execute("DELETE FROM scan_cache WHERE creator_id = ?", (creator_id,))
        conn.commit()


def delete_sqlite_scan_cache_except(creator_id: int, keep_ids: set[int]) -> None:
    ensure_database()
    with _connect_sqlite() as conn:
        if keep_ids:
            placeholders = ",".join("?" for _ in keep_ids)
            conn.execute(
                f"DELETE FROM scan_cache WHERE creator_id = ? AND id NOT IN ({placeholders})",
                (creator_id, *sorted(keep_ids)),
            )
        else:
            conn.execute("DELETE FROM scan_cache WHERE creator_id = ?", (creator_id,))
        conn.commit()


# ============ Task Center Cache Functions ============


def get_task_center_cache(creator_id: int) -> dict | None:
    """获取单个账号的任务中心缓存"""
    ensure_database()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT * FROM task_center_cache WHERE creator_id = ?",
            (creator_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "creator_id": row["creator_id"],
        "video_count": row["video_count"],
        "collection_count": row["collection_count"],
        "live_count": row["live_count"],
        "failed_count": row["failed_count"],
        "last_download_at": row["last_download_at"],
        "downloaded_work_ids_hash": row["downloaded_work_ids_hash"],
        "updated_at": row["updated_at"],
    }


def get_all_task_center_cache() -> list[dict]:
    """获取所有账号的任务中心缓存"""
    ensure_database()
    with _connect_sqlite() as conn:
        rows = conn.execute("SELECT * FROM task_center_cache").fetchall()
    return [
        {
            "creator_id": row["creator_id"],
            "video_count": row["video_count"],
            "collection_count": row["collection_count"],
            "live_count": row["live_count"],
            "failed_count": row["failed_count"],
            "last_download_at": row["last_download_at"],
            "downloaded_work_ids_hash": row["downloaded_work_ids_hash"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def save_task_center_cache(
    creator_id: int,
    video_count: int,
    collection_count: int,
    live_count: int,
    failed_count: int,
    last_download_at: str | None,
    downloaded_work_ids_hash: str,
) -> None:
    """保存单个账号的任务中心缓存"""
    ensure_database()
    with _connect_sqlite() as conn:
        conn.execute(
            """
            INSERT INTO task_center_cache (
                creator_id, video_count, collection_count, live_count,
                failed_count, last_download_at, downloaded_work_ids_hash, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(creator_id) DO UPDATE SET
                video_count = excluded.video_count,
                collection_count = excluded.collection_count,
                live_count = excluded.live_count,
                failed_count = excluded.failed_count,
                last_download_at = excluded.last_download_at,
                downloaded_work_ids_hash = excluded.downloaded_work_ids_hash,
                updated_at = excluded.updated_at
            """,
            (
                creator_id,
                video_count,
                collection_count,
                live_count,
                failed_count,
                last_download_at,
                downloaded_work_ids_hash,
                now_iso(),
            ),
        )
        conn.commit()


def delete_task_center_cache(creator_id: int) -> None:
    """删除单个账号的任务中心缓存"""
    ensure_database()
    with _connect_sqlite() as conn:
        conn.execute(
            "DELETE FROM task_center_cache WHERE creator_id = ?", (creator_id,)
        )
        conn.commit()


def invalidate_all_task_center_cache() -> None:
    """清除所有任务中心缓存"""
    ensure_database()
    with _connect_sqlite() as conn:
        conn.execute("DELETE FROM task_center_cache")
        conn.commit()


def invalidate_task_center_cache_for_creators(
    creator_ids: list[int] | set[int],
) -> None:
    """批量清除指定账号的任务中心缓存"""
    if not creator_ids:
        return
    ensure_database()
    with _connect_sqlite() as conn:
        placeholders = ",".join("?" for _ in creator_ids)
        conn.execute(
            f"DELETE FROM task_center_cache WHERE creator_id IN ({placeholders})",
            tuple(creator_ids),
        )
        conn.commit()
