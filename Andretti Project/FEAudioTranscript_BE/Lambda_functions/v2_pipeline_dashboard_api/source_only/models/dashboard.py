"""Pydantic models for dashboard, race, driver, and metrics responses."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class SessionSummary(BaseModel):
    session_id: str
    session_name: str | None = None
    race_name: str | None = None
    status: str | None = None
    timezone: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    driver_ids: list[str] = []


class DriverInfo(BaseModel):
    driver_id: str
    driver_name: str | None = None
    driver_number: str | None = None
    team: str | None = None
    is_active: bool = True


class DashboardSummary(BaseModel):
    total_sessions: int
    by_status: dict[str, int]
    active_session: SessionSummary | None = None
    upcoming: list[SessionSummary] = []
    recent_completed: list[SessionSummary] = []
    driver_count: int


class MetricPoint(BaseModel):
    timestamp: str
    value: float


class MetricSeries(BaseModel):
    label: str
    metric: str
    unit: str | None = None
    points: list[MetricPoint] = []


class CreateSessionRequest(BaseModel):
    session_name: str
    start_time: str
    end_time: str
    timezone: str = "UTC"
    driver_ids: list[str] | None = None
    driver_numbers: list[str] | None = None
    driver_names: list[str] | None = None
    created_by: str | None = None
    session_type: str = "RACE"


class ScheduleActionResponse(BaseModel):
    ok: bool
    status_code: int
    body: Any
