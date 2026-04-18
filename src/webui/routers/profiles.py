from fastapi import APIRouter, Response, status

from ..schemas import ProfileCreate, ProfileRead, ProfileUpdate
from ..services import profiles as service


router = APIRouter(prefix="/profiles", tags=["profiles"])


@router.get("", response_model=list[ProfileRead])
def list_profiles():
    return service.list_profiles()


@router.get("/{profile_id}", response_model=ProfileRead)
def get_profile(profile_id: int):
    return service.get_profile(profile_id)


@router.post("", response_model=ProfileRead, status_code=status.HTTP_201_CREATED)
def create_profile(payload: ProfileCreate):
    return service.create_profile(payload)


@router.put("/{profile_id}", response_model=ProfileRead)
def update_profile(profile_id: int, payload: ProfileUpdate):
    return service.update_profile(profile_id, payload)


@router.delete("/{profile_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_profile(profile_id: int):
    service.delete_profile(profile_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
