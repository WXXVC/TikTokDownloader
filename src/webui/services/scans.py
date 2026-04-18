import json
import re

from fastapi import HTTPException

from ..config import DATA_DIR, MAX_SCAN_CACHE_PER_CREATOR
from ..db import (
    delete_sqlite_scan_cache_except,
    list_sqlite_scan_cache,
    now_iso,
    save_sqlite_scan_cache,
)
from ..schemas import ScanItem
from .creators import get_creator
from .engine import fetch_account_items, read_downloaded_ids


MOCK_SCAN_PATH = DATA_DIR / "mock_scan_results.json"
INVALID_FOLDER_CHARS = re.compile(r'[\\/:*?"<>|]+')


def load_mock_scan_results() -> list[dict]:
    if MOCK_SCAN_PATH.exists():
        return json.loads(MOCK_SCAN_PATH.read_text(encoding="utf-8"))
    sample = [
        {
            "id": "7485639200000000001",
            "title": "示例作品一",
            "type": "video",
            "published_at": "2026-04-10 12:00:00",
            "cover": "",
            "share_url": "https://www.douyin.com/video/7485639200000000001",
        },
        {
            "id": "7485639200000000002",
            "title": "示例作品二",
            "type": "video",
            "published_at": "2026-04-11 09:30:00",
            "cover": "",
            "share_url": "https://www.douyin.com/video/7485639200000000002",
        },
    ]
    MOCK_SCAN_PATH.write_text(
        json.dumps(sample, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return sample


def map_engine_items(items: list[dict]) -> list[dict]:
    mapped = []
    for item in items:
        cover = item.get("static_cover", "")
        if not cover and item.get("type") == "图集":
            downloads = item.get("downloads") or [""]
            cover = downloads[0] if downloads else ""
        mapped.append(
            {
                "id": str(item.get("id", "")),
                "title": item.get("desc") or item.get("id") or "未命名作品",
                "type": item.get("type", "unknown"),
                "published_at": item.get("create_time", ""),
                "cover": cover,
                "share_url": item.get("share_url", ""),
                "raw": item,
            }
        )
    return [item for item in mapped if item["id"]]


def _clean_folder_part(value: str) -> str:
    text = INVALID_FOLDER_CHARS.sub("_", str(value or "")).strip().strip(".")
    return text or ""


def _creator_folder_suffix(tab: str) -> str:
    return {
        "favorite": "喜欢作品",
        "collection": "收藏作品",
    }.get(tab or "post", "发布作品")


def _extract_creator_identity_from_scan_payload(payload: list[dict]) -> tuple[str, str]:
    for item in payload:
        raw = item.get("raw") or {}
        uid = _clean_folder_part(raw.get("uid") or "")
        nickname = _clean_folder_part(raw.get("nickname") or "")
        if uid or nickname:
            return uid, nickname
    return "", ""


def infer_creator_folder_name(creator: dict) -> str:
    uid = ""
    nickname = ""
    for row in list_sqlite_scan_cache(creator["id"]):
        uid, nickname = _extract_creator_identity_from_scan_payload(row.get("payload") or [])
        if uid or nickname:
            break
    if not (uid or nickname):
        try:
            engine_items = map_engine_items(fetch_account_items(creator))
        except Exception:
            engine_items = []
        uid, nickname = _extract_creator_identity_from_scan_payload(engine_items)

    suffix = _creator_folder_suffix(creator.get("tab") or "post")
    name = _clean_folder_part(creator.get("mark") or nickname or creator.get("name") or "账号")
    creator_uid = _clean_folder_part(uid or creator.get("sec_user_id") or str(creator["id"]))
    return f"UID{creator_uid}_{name}_{suffix}"


def find_scan_items_by_work_ids(creator_id: int, work_ids: list[str]) -> list[dict]:
    if not work_ids:
        return []
    work_id_set = {str(item) for item in work_ids}
    for row in list_sqlite_scan_cache(creator_id):
        payload = row.get("payload") or []
        matched = [item for item in payload if str(item.get("id") or "") in work_id_set]
        if matched:
            return matched
    return []


def build_scan_payload(creator: dict, source_items: list[dict], source_name: str):
    downloaded_ids = read_downloaded_ids()
    items = [
        ScanItem(
            id=item["id"],
            title=item["title"],
            type=item["type"],
            published_at=item.get("published_at", ""),
            is_downloaded=item["id"] in downloaded_ids,
            cover=item.get("cover", ""),
            share_url=item.get("share_url", ""),
            raw=item.get("raw", item),
        )
        for item in source_items
    ]
    undownloaded_items = [item for item in items if not item.is_downloaded]
    downloaded_items = [item for item in items if item.is_downloaded]

    save_sqlite_scan_cache(
        {
            "id": None,
            "creator_id": creator["id"],
            "scanned_at": now_iso(),
            "total_count": len(items),
            "undownloaded_count": len(undownloaded_items),
            "payload": [item.model_dump() for item in items],
            "source": source_name,
        }
    )
    creator_cache = list_sqlite_scan_cache(creator["id"])[:MAX_SCAN_CACHE_PER_CREATOR]
    keep_ids = {item["id"] for item in creator_cache}
    delete_sqlite_scan_cache_except(creator["id"], keep_ids)

    return {
        "creator_id": creator["id"],
        "creator_name": creator["name"],
        "source": source_name,
        "all_count": len(items),
        "downloaded_count": len(items) - len(undownloaded_items),
        "undownloaded_count": len(undownloaded_items),
        "items": undownloaded_items,
        "hidden_items": downloaded_items,
    }


def _normalize_scan_items(payload: list[dict]) -> list[ScanItem]:
    return [item if isinstance(item, ScanItem) else ScanItem(**item) for item in payload]


def _filter_scan_items(
    payload: list[dict],
    *,
    keyword: str = "",
    item_type: str = "",
    show_downloaded: bool = False,
) -> tuple[list[ScanItem], list[str], int, int]:
    all_items = _normalize_scan_items(payload)
    available_types = sorted({item.type for item in all_items if item.type})
    downloaded_count = sum(1 for item in all_items if item.is_downloaded)
    undownloaded_count = len(all_items) - downloaded_count
    visible_items = all_items if show_downloaded else [item for item in all_items if not item.is_downloaded]

    keyword_text = str(keyword or "").strip().lower()
    if keyword_text:
        visible_items = [
            item for item in visible_items
            if keyword_text in " ".join([
                item.title,
                item.id,
                item.share_url,
                item.published_at,
            ]).lower()
        ]

    if item_type:
        visible_items = [item for item in visible_items if item.type == item_type]

    return visible_items, available_types, downloaded_count, undownloaded_count


def _build_scan_page_from_payload(
    *,
    creator: dict,
    source: str,
    payload: list[dict],
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    item_type: str = "",
    show_downloaded: bool = False,
):
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 10)))
    visible_items, available_types, downloaded_count, undownloaded_count = _filter_scan_items(
        payload,
        keyword=keyword,
        item_type=item_type,
        show_downloaded=show_downloaded,
    )
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "creator_id": creator["id"],
        "creator_name": creator["name"],
        "source": source,
        "all_count": len(payload),
        "downloaded_count": downloaded_count,
        "undownloaded_count": undownloaded_count,
        "total_visible": len(visible_items),
        "page": page,
        "page_size": page_size,
        "available_types": available_types,
        "items": visible_items[start:end],
    }


def scan_creator_works(
    creator_id: int,
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    item_type: str = "",
    show_downloaded: bool = False,
):
    creator = get_creator(creator_id)
    if not creator["enabled"]:
        raise HTTPException(status_code=400, detail="Creator is disabled")
    try:
        engine_items = map_engine_items(fetch_account_items(creator))
        if engine_items:
            build_scan_payload(creator, engine_items, "engine_api")
            return latest_scan(
                creator_id,
                page=page,
                page_size=page_size,
                keyword=keyword,
                item_type=item_type,
                show_downloaded=show_downloaded,
            )
    except Exception:
        pass
    build_scan_payload(creator, load_mock_scan_results(), "mock_scan_results")
    return latest_scan(
        creator_id,
        page=page,
        page_size=page_size,
        keyword=keyword,
        item_type=item_type,
        show_downloaded=show_downloaded,
    )


def latest_scan(
    creator_id: int,
    *,
    page: int = 1,
    page_size: int = 10,
    keyword: str = "",
    item_type: str = "",
    show_downloaded: bool = False,
):
    creator = get_creator(creator_id)
    candidates = list_sqlite_scan_cache(creator_id)
    if not candidates:
        raise HTTPException(status_code=404, detail="No scan cache found")
    row = candidates[0]
    return _build_scan_page_from_payload(
        creator=creator,
        source=row.get("source", "scan_cache"),
        payload=row["payload"],
        page=page,
        page_size=page_size,
        keyword=keyword,
        item_type=item_type,
        show_downloaded=show_downloaded,
    )
