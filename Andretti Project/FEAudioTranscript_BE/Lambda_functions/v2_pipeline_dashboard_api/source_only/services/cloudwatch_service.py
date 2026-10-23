"""CloudWatch metrics for pipeline health: Lambda invocations/errors/duration
and ECS running task counts.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import boto3

from config import settings
from utils.time_utils import now_utc


class CloudWatchService:
    def __init__(self) -> None:
        self._cw = boto3.client("cloudwatch", region_name=settings.aws_region)
        self._logs = boto3.client("logs", region_name=settings.aws_region)
        self._logs = boto3.client("logs", region_name=settings.aws_region)

    def _metric(self, namespace: str, name: str, dims: list[dict], stat: str,
                minutes: int, period: int) -> list[dict]:
        end = now_utc()
        start = end - timedelta(minutes=minutes)
        res = self._cw.get_metric_statistics(
            Namespace=namespace, MetricName=name, Dimensions=dims,
            StartTime=start, EndTime=end, Period=period, Statistics=[stat],
        )
        points = sorted(res.get("Datapoints", []), key=lambda d: d["Timestamp"])
        return [{"timestamp": p["Timestamp"].isoformat(), "value": float(p[stat])} for p in points]

    def lambda_metrics(self, function_name: str, metric: str = "Invocations",
                       minutes: int = 180, period: int = 300) -> list[dict]:
        stat = "Sum" if metric in ("Invocations", "Errors", "Throttles") else "Average"
        return self._metric("AWS/Lambda", metric,
                            [{"Name": "FunctionName", "Value": function_name}],
                            stat, minutes, period)

    def pipeline_overview(self, minutes: int = 180) -> dict:
        out = {}
        for fn in settings.pipeline_lambdas:
            out[fn] = {
                "invocations": self.lambda_metrics(fn, "Invocations", minutes),
                "errors": self.lambda_metrics(fn, "Errors", minutes),
                "duration_ms": self.lambda_metrics(fn, "Duration", minutes),
            }
        return out

    def ecs_running_tasks(self, minutes: int = 180, period: int = 300) -> list[dict]:
        return self._metric(
            "ECS/ContainerInsights", "RunningTaskCount",
            [{"Name": "ClusterName", "Value": settings.ecs_cluster}],
            "Average", minutes, period,
        )

    def get_recent_pipeline_events(self, session_id: str, minutes: int = 60, limit: int = 50) -> list[dict]:
        """Best-effort CloudWatch Logs Insights tail; fall back to metric-derived events."""
        end = now_utc()
        start = end - timedelta(minutes=minutes)
        try:
            for function_name in settings.pipeline_lambdas:
                groups = self._logs.describe_log_groups(logGroupNamePrefix=f"/aws/lambda/{function_name}").get("logGroups", [])
                for group in groups[:1]:
                    query = (
                        "fields @timestamp, @message | filter @message like /" + session_id + "/i "
                        "| sort @timestamp desc | limit " + str(limit)
                    )
                    response = self._logs.start_query(
                        logGroupName=group["logGroupName"],
                        startTime=int(start.timestamp() * 1000),
                        endTime=int(end.timestamp() * 1000),
                        query=query,
                    )
                    query_id = response.get("queryId")
                    if not query_id:
                        continue
                    results = self._logs.get_query_results(queryId=query_id)
                    if results.get("status") == "Complete":
                        events = []
                        for row in results.get("results", [])[:limit]:
                            msg = ""
                            ts = None
                            for item in row:
                                if item.get("field") == "@timestamp":
                                    ts = item.get("value")
                                elif item.get("field") == "@message":
                                    msg = item.get("value")
                            if msg:
                                events.append({
                                    "timestamp": ts,
                                    "lambda_name": function_name,
                                    "level": "err" if "error" in msg.lower() or "exception" in msg.lower() else "info",
                                    "message": msg,
                                })
                        if events:
                            return events
        except Exception:
            pass

        return []

    def get_recent_pipeline_events(self, session_id: str, minutes: int = 60, limit: int = 50) -> list[dict]:
        if not session_id:
            return []

        end = now_utc()
        start = end - timedelta(minutes=minutes)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        collected: list[dict] = []
        for function_name in settings.pipeline_lambdas:
            try:
                log_group = f"/aws/lambda/{function_name}"
                resp = self._logs.describe_log_groups(logGroupNamePrefix=log_group, limit=20)
                log_groups = [group.get("logGroupName") for group in resp.get("logGroups", []) if group.get("logGroupName")]
                if not log_groups:
                    log_groups = [log_group]

                for group_name in log_groups:
                    events = self._logs.filter_log_events(
                        logGroupName=group_name,
                        startTime=start_ms,
                        endTime=end_ms,
                        filterPattern=session_id,
                        limit=20,
                    )
                    for event in events.get("events", []):
                        message = (event.get("message") or "").strip()
                        if not message:
                            continue
                        collected.append({
                            "timestamp": event.get("timestamp"),
                            "lambda": function_name,
                            "message": message,
                            "level": "error" if "error" in message.lower() else "warn" if "warn" in message.lower() else "ok",
                        })
            except Exception:
                continue

        collected = sorted(collected, key=lambda item: item.get("timestamp") or 0, reverse=True)
        return collected[:limit]


cloudwatch_service = CloudWatchService()
