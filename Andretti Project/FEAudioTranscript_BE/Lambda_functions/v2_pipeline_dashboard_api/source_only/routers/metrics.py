"""Metrics endpoints backed by CloudWatch."""
from __future__ import annotations

from fastapi import APIRouter, Query

from config import settings
from services.cloudwatch_service import cloudwatch_service

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/pipeline")
def pipeline(minutes: int = Query(180, ge=5, le=1440)):
    return {"window_minutes": minutes, "lambdas": cloudwatch_service.pipeline_overview(minutes)}


@router.get("/lambda/{function_name}")
def lambda_metric(function_name: str,
                  metric: str = Query("Invocations"),
                  minutes: int = Query(180, ge=5, le=1440)):
    return {
        "function": function_name,
        "metric": metric,
        "points": cloudwatch_service.lambda_metrics(function_name, metric, minutes),
    }


@router.get("/ecs")
def ecs(minutes: int = Query(180, ge=5, le=1440)):
    return {
        "cluster": settings.ecs_cluster,
        "running_tasks": cloudwatch_service.ecs_running_tasks(minutes),
    }
