"""Invoke the scheduler Lambda for session writes, reusing its EventBridge logic
instead of duplicating it here. We build the same API-Gateway-shaped event its
handler expects (httpMethod / path / body / pathParameters).
"""
from __future__ import annotations

import json
from typing import Any

import boto3

from config import settings


class LambdaService:
    def __init__(self) -> None:
        self._lambda = boto3.client("lambda", region_name=settings.aws_region)

    def _invoke_scheduler(self, method: str, path: str, body: dict | None = None,
                          path_params: dict | None = None) -> tuple[int, Any]:
        event = {
            "httpMethod": method,
            "path": path,
            "pathParameters": path_params or {},
            "queryStringParameters": {},
            "body": json.dumps(body) if body is not None else None,
        }
        resp = self._lambda.invoke(
            FunctionName=settings.scheduler_lambda,
            InvocationType="RequestResponse",
            Payload=json.dumps(event).encode(),
        )
        payload = json.loads(resp["Payload"].read() or b"{}")
        status = int(payload.get("statusCode", 502))
        raw_body = payload.get("body")
        try:
            parsed = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
        except (TypeError, json.JSONDecodeError):
            parsed = raw_body
        return status, parsed

    # ---- session write operations ----
    def create_session(self, payload: dict) -> tuple[int, Any]:
        return self._invoke_scheduler("POST", "/sessions", body=payload)

    def update_session(self, session_id: str, payload: dict) -> tuple[int, Any]:
        return self._invoke_scheduler("PUT", f"/sessions/{session_id}", body=payload,
                                      path_params={"session_id": session_id})

    def delete_session(self, session_id: str, created_by: str | None = None) -> tuple[int, Any]:
        body = {"created_by": created_by} if created_by else None
        return self._invoke_scheduler("DELETE", f"/sessions/{session_id}", body=body,
                                      path_params={"session_id": session_id})

    def start_session(self, session_id: str, created_by: str | None = None) -> tuple[int, Any]:
        body = {"created_by": created_by} if created_by else None
        return self._invoke_scheduler("POST", f"/sessions/{session_id}/start", body=body,
                                      path_params={"session_id": session_id})

    def stop_session(self, session_id: str, created_by: str | None = None) -> tuple[int, Any]:
        body = {"created_by": created_by} if created_by else None
        return self._invoke_scheduler("POST", f"/sessions/{session_id}/stop", body=body,
                                      path_params={"session_id": session_id})

    def trigger_audio_merge(self, session_id: str) -> None:
        self._lambda.invoke(
            FunctionName=settings.audio_merge_lambda,
            InvocationType="Event",
            Payload=json.dumps({"session_id": session_id}).encode(),
        )


lambda_service = LambdaService()
