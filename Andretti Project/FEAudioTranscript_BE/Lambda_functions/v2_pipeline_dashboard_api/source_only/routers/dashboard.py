"""Dashboard aggregation endpoints."""
from __future__ import annotations

from collections import Counter

from fastapi import APIRouter

from models.dashboard import DashboardSummary, SessionSummary
from services.dynamodb_service import dynamodb_service

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _to_summary(s: dict) -> SessionSummary:
    return SessionSummary(
        session_id=s.get("session_id"),
        session_name=s.get("session_name"),
        race_name=s.get("race_name"),
        status=s.get("status"),
        timezone=s.get("timezone"),
        start_time=s.get("start_time"),
        end_time=s.get("end_time"),
        driver_ids=s.get("driver_ids", []) or [],
    )


@router.get("/summary", response_model=DashboardSummary)
def summary() -> DashboardSummary:
    sessions = dynamodb_service.list_sessions()
    drivers = dynamodb_service.list_drivers()
    by_status = Counter(s.get("status", "UNKNOWN") for s in sessions)

    active = next((s for s in sessions if s.get("status") == "ACTIVE"), None)
    upcoming = [s for s in sessions if s.get("status") == "SCHEDULED"][:5]
    completed = [s for s in sessions if s.get("status") in ("COMPLETED", "STOPPED")][:5]

    return DashboardSummary(
        total_sessions=len(sessions),
        by_status=dict(by_status),
        active_session=_to_summary(active) if active else None,
        upcoming=[_to_summary(s) for s in upcoming],
        recent_completed=[_to_summary(s) for s in completed],
        driver_count=len(drivers),
    )
