"""Race-session endpoints.

Reads come straight from DynamoDB/S3. Writes (create/update/delete/start/stop)
are proxied to the scheduler Lambda so the EventBridge scheduling logic lives in
exactly one place.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from models.dashboard import CreateSessionRequest, ScheduleActionResponse
from services.dynamodb_service import dynamodb_service
from services.lambda_service import lambda_service
from services.opensearch_service import opensearch_service
from services.s3_service import s3_service
from utils.time_utils import list_timezones

router = APIRouter(prefix="/race", tags=["race"])


# ---- reads ----
@router.get("/sessions")
def list_sessions(status: str | None = Query(None)):
    sessions = dynamodb_service.list_sessions()
    if status:
        wanted = status.strip().upper()
        allowed = ({"ACTIVE", "SCHEDULED"} if wanted == "ACTIVE"
                   else {"COMPLETED", "STOPPED"} if wanted == "COMPLETED"
                   else {wanted})
        sessions = [s for s in sessions if s.get("status") in allowed]
    return {"sessions": sessions, "count": len(sessions)}


@router.get("/sessions/{session_id}")
def get_session(session_id: str):
    s = dynamodb_service.get_session(session_id)
    if not s:
        raise HTTPException(404, "Session not found")
    return {"session": s}


@router.get("/timezones")
def timezones():
    zones = list_timezones()
    return {"timezones": zones, "count": len(zones)}


# ---- replay source helpers (full-sessions/ in S3) ----
@router.get("/replay/sessions")
def replay_sessions():
    return {"sessions": s3_service.list_sessions()}


@router.get("/replay/drivers")
def replay_drivers(session: str = Query(...)):
    return {"session": session, "drivers": s3_service.list_drivers_with_audio(session)}


# ---- transcript search ----
@router.get("/sessions/{session_id}/transcripts")
def transcripts(session_id: str, q: str | None = None, driver: str | None = None,
                 limit: int = Query(100, ge=1, le=500)):
    if not opensearch_service.enabled:
        raise HTTPException(503, "OpenSearch is not configured (set OPENSEARCH_ENDPOINT)")
    return {"results": opensearch_service.search_transcripts(session_id, q, driver, size=limit)}


# ---- writes (proxied to scheduler Lambda) ----
def _wrap(result: tuple[int, object]) -> ScheduleActionResponse:
    status, body = result
    return ScheduleActionResponse(ok=200 <= status < 300, status_code=status, body=body)


@router.post("/sessions", response_model=ScheduleActionResponse)
def create_session(req: CreateSessionRequest):
    return _wrap(lambda_service.create_session(req.model_dump(exclude_none=True)))


@router.put("/sessions/{session_id}", response_model=ScheduleActionResponse)
def update_session(session_id: str, req: CreateSessionRequest):
    return _wrap(lambda_service.update_session(session_id, req.model_dump(exclude_none=True)))


@router.delete("/sessions/{session_id}", response_model=ScheduleActionResponse)
def delete_session(session_id: str, created_by: str | None = None):
    return _wrap(lambda_service.delete_session(session_id, created_by))


@router.post("/sessions/{session_id}/start", response_model=ScheduleActionResponse)
def start_session(session_id: str, created_by: str | None = None):
    return _wrap(lambda_service.start_session(session_id, created_by))


@router.post("/sessions/{session_id}/stop", response_model=ScheduleActionResponse)
def stop_session(session_id: str, created_by: str | None = None):
    return _wrap(lambda_service.stop_session(session_id, created_by))
