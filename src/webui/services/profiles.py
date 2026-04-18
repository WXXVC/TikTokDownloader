from fastapi import HTTPException

from ..db import (
    delete_sqlite_profile,
    get_sqlite_profile,
    list_sqlite_profiles,
    now_iso,
    save_sqlite_profile,
    update_sqlite_creator_profile,
)
from ..schemas import ProfileCreate, ProfileUpdate


def list_profiles():
    return list_sqlite_profiles()


def get_profile(profile_id: int):
    item = get_sqlite_profile(profile_id)
    if item:
        return item
    raise HTTPException(status_code=404, detail="Profile not found")


def create_profile(payload: ProfileCreate):
    if any(item["name"] == payload.name for item in list_sqlite_profiles()):
        raise HTTPException(status_code=409, detail="Profile name already exists")
    item = payload.model_dump()
    item["id"] = None
    item["created_at"] = now_iso()
    item["updated_at"] = item["created_at"]
    return save_sqlite_profile(item)


def update_profile(profile_id: int, payload: ProfileUpdate):
    item = get_sqlite_profile(profile_id)
    if not item:
        raise HTTPException(status_code=404, detail="Profile not found")
    for candidate in list_sqlite_profiles():
        if candidate["id"] != profile_id and candidate["name"] == payload.name:
            raise HTTPException(status_code=409, detail="Profile name already exists")
    item.update(payload.model_dump())
    item["updated_at"] = now_iso()
    return save_sqlite_profile(item)


def delete_profile(profile_id: int):
    if profile_id == 1:
        raise HTTPException(status_code=400, detail="Default profile cannot be deleted")
    if not get_sqlite_profile(profile_id):
        raise HTTPException(status_code=404, detail="Profile not found")
    update_sqlite_creator_profile(profile_id, 1)
    delete_sqlite_profile(profile_id)
