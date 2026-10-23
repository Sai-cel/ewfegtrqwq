"""DynamoDB access for sessions, active sessions, and drivers."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import boto3

from config import settings


def _clean(obj: Any) -> Any:
    """Recursively convert DynamoDB Decimals to int/float for JSON."""
    if isinstance(obj, list):
        return [_clean(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


class DynamoDBService:
    def __init__(self) -> None:
        self._ddb = boto3.resource("dynamodb", region_name=settings.aws_region)
        self.sessions = self._ddb.Table(settings.session_table)
        self.drivers = self._ddb.Table(settings.driver_table)
        self.active = self._ddb.Table(settings.active_sessions_table)

    # ---- sessions ----
    def list_sessions(self) -> list[dict]:
        items = self.sessions.scan().get("Items", [])
        # Overlay live status from the active-sessions table.
        for s in items:
            row = self.active.get_item(Key={"SessionId": s["session_id"]}).get("Item")
            if row:
                s["StartedAt"] = row.get("StartedAt")
                if row.get("Status") == "ACTIVE":
                    s["status"] = "ACTIVE"
                elif row.get("Status") == "STOPPED":
                    s["status"] = "COMPLETED"
        items.sort(key=lambda x: x.get("start_time", ""), reverse=True)
        return _clean(items)

    def get_session(self, session_id: str) -> dict | None:
        item = self.sessions.get_item(Key={"session_id": session_id}).get("Item")
        return _clean(item) if item else None

    def get_active_session(self) -> dict | None:
        for item in self.active.scan().get("Items", []):
            if item.get("Status") == "ACTIVE":
                return _clean(item)
        return None

    # ---- drivers ----
    def list_drivers(self) -> list[dict]:
        return _clean(self.drivers.scan().get("Items", []))

    def get_driver(self, driver_id: str) -> dict | None:
        item = self.drivers.get_item(Key={"driver_id": driver_id}).get("Item")
        return _clean(item) if item else None


dynamodb_service = DynamoDBService()
