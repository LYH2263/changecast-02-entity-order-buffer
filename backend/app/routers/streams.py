from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import project_service as svc

router = APIRouter(tags=["streams"])


@router.get("/streams")
def list_streams():
    return svc.list_streams()


class ConfigBody(BaseModel):
    buffer_max_events: int
    buffer_max_wait_seconds: int
    start_seq: int


@router.post("/streams/{stream_id}/config")
def update_config(stream_id: int, body: ConfigBody):
    try:
        return svc.update_stream_config(stream_id, body.model_dump())
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
