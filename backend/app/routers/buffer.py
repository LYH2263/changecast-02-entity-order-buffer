from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import project_service as svc

router = APIRouter(tags=["buffer"])


@router.get("/ooo-buffer")
def list_ooo_buffer(stream_id: int | None = None, status: str = "active"):
    return svc.list_ooo_buffer(stream_id=stream_id, status=status)


@router.get("/buffer-summary")
def buffer_summary():
    return svc.buffer_summary()


class FlushBody(BaseModel):
    stream_id: int
    entity_key: str
    note: str | None = None


@router.post("/ooo-buffer/flush")
def flush_buffer(body: FlushBody):
    try:
        return svc.flush_entity_buffer(body.stream_id, body.entity_key, body.note)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
