"""Driver endpoints (read-only over v2_DriverConfig)."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from services.dynamodb_service import dynamodb_service

router = APIRouter(prefix="/drivers", tags=["drivers"])


@router.get("")
def list_drivers(active_only: bool = False):
    drivers = dynamodb_service.list_drivers()
    if active_only:
        drivers = [d for d in drivers if d.get("is_active", True)]
    return {"drivers": drivers, "count": len(drivers)}


@router.get("/{driver_id}")
def get_driver(driver_id: str):
    d = dynamodb_service.get_driver(driver_id)
    if not d:
        raise HTTPException(404, "Driver not found")
    return {"driver": d}
