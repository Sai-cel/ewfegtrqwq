"""Pydantic models for the pipeline status endpoints."""
from __future__ import annotations

from pydantic import BaseModel


class StageCount(BaseModel):
    stage: str
    prefix: str
    object_count: int
    last_modified: str | None = None


class DriverPipeline(BaseModel):
    driver_folder: str
    raw_chunks: int = 0
    silence_removed: int = 0
    transcriptions: int = 0
    has_full_session: bool = False


class SessionPipelineStatus(BaseModel):
    session_id: str
    stages: list[StageCount]
    drivers: list[DriverPipeline]
    bottleneck: str | None = None  # stage where counts drop off most


class PipelineHealth(BaseModel):
    healthy: bool
    stage_totals: dict[str, int]
    notes: list[str] = []


class PipelineStageDetail(BaseModel):
    stage: str
    label: str
    lambda_name: str | None = None
    object_count: int = 0
    last_modified: str | None = None
    status: str = "pending"


class PipelineEvent(BaseModel):
    timestamp: str | None = None
    lambda_name: str | None = None
    stage: str | None = None
    level: str = "info"
    message: str


class PipelineAlert(BaseModel):
    title: str
    body: str
    severity: str = "warning"
