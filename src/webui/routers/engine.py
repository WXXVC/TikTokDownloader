from fastapi import APIRouter

from ..services import creators as creator_service
from ..services import engine as engine_service
from ..services import profiles as profile_service
from ..schemas import EngineConfig


router = APIRouter(prefix="/engine", tags=["engine"])


@router.get("/settings-preview")
def engine_settings_preview(profile_id: int = 1):
    profile = profile_service.get_profile(profile_id)
    creators = [item for item in creator_service.list_creators() if item["enabled"]]
    return engine_service.build_settings_payload(profile, creators)


@router.get("/config", response_model=EngineConfig)
def engine_config_get():
    return engine_service.read_engine_config()


@router.put("/config", response_model=EngineConfig)
def engine_config_update(payload: EngineConfig):
    engine_service.update_engine_config(payload.model_dump())
    return engine_service.read_engine_config()


@router.post("/settings-sync")
def engine_settings_sync(profile_id: int = 1):
    profile = profile_service.get_profile(profile_id)
    creators = [item for item in creator_service.list_creators() if item["enabled"]]
    payload = engine_service.build_settings_payload(profile, creators)
    path = engine_service.save_engine_settings(
        {**engine_service.read_engine_settings(), **payload}
    )
    return {"message": "Engine settings synced", "path": str(path)}
