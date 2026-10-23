"""OpenSearch service using basic auth."""
import os
import json
import boto3
from datetime import datetime, timezone

from config import settings


def _get_opensearch_credentials():
    client = boto3.client("secretsmanager", region_name="us-west-2")
    response = client.get_secret_value(SecretId="andretti/opensearch-credentials")
    return json.loads(response["SecretString"])


def _epoch_to_iso(value) -> str | None:
    """Convert an epoch-seconds float/int to the same ISO format S3's
    LastModified uses, so the frontend's existing UTC parsing just works."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        return None


class OpenSearchService:
    def __init__(self):
        self.enabled = False
        self.client = None
        endpoint = os.environ.get("OPENSEARCH_ENDPOINT", "")
        _creds = _get_opensearch_credentials()
        user = _creds["username"]
        password = _creds["password"]
        if endpoint and user and password:
            try:
                from opensearchpy import OpenSearch, RequestsHttpConnection
                host = endpoint.replace("https://", "").rstrip("/")
                self.client = OpenSearch(
                    hosts=[{"host": host, "port": 443}],
                    http_auth=(user, password),
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection,
                    timeout=5,
                )
                self.enabled = True
                print("OpenSearch connected successfully")
            except Exception as e:
                print(f"OpenSearch init failed: {e}")

    def count_documents(self, session_id: str) -> int:
        if not self.enabled or not self.client:
            return 0
        try:
            r = self.client.count(
                index=settings.opensearch_index,
                body={"query": {"term": {"SessionId": session_id}}}
            )
            return r.get("count", 0)
        except Exception as e:
            print(f"OpenSearch count error: {e}")
            return 0

    def driver_stage_range(self, session_id: str, driver: str) -> dict:
        """Count + first/last indexed time for one driver, in a single query
        using min/max aggregations (cheaper than paging through hits)."""
        if not self.enabled or not self.client:
            return {"count": 0, "first": None, "last": None}
        try:
            r = self.client.search(
                index=settings.opensearch_index,
                body={
                    "size": 0,
                    "track_total_hits": True,
                    "query": {"bool": {"must": [
                        {"term": {"SessionId": session_id}},
                        {"term": {"Driver": driver}},
                    ]}},
                    "aggs": {
                        "first_ts": {"min": {"field": "Timestamp"}},
                        "last_ts": {"max": {"field": "Timestamp"}},
                    },
                }
            )
            count = r.get("hits", {}).get("total", {}).get("value", 0)
            aggs = r.get("aggregations", {})
            first_val = aggs.get("first_ts", {}).get("value")
            last_val = aggs.get("last_ts", {}).get("value")
            return {
                "count": count,
                "first": _epoch_to_iso(first_val),
                "last": _epoch_to_iso(last_val),
            }
        except Exception as e:
            print(f"OpenSearch driver range error: {e}")
            return {"count": 0, "first": None, "last": None}

    def driver_stage_chunks(self, session_id: str, driver: str, size: int = 5000) -> list[dict]:
        """Full per-record list for one driver — file name + indexed time,
        sorted oldest first. Only call this on expand, same rule as S3."""
        if not self.enabled or not self.client:
            return []
        try:
            r = self.client.search(
                index=settings.opensearch_index,
                body={
                    "size": size,
                    "query": {"bool": {"must": [
                        {"term": {"SessionId": session_id}},
                        {"term": {"Driver": driver}},
                    ]}},
                    "sort": [{"Timestamp": "asc"}],
                    "_source": ["SourceFile", "Timestamp"],
                }
            )
            out = []
            for hit in r.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                out.append({
                    "file": src.get("SourceFile", hit.get("_id", "")),
                    "last_modified": _epoch_to_iso(src.get("Timestamp")),
                })
            return out
        except Exception as e:
            print(f"OpenSearch driver chunks error: {e}")
            return []

    def driver_transcript_lookup(self, session_id: str, driver: str, size: int = 5000) -> dict:
        """Returns {SourceFile: {message, confidence, team, session_time}}
        for every transcribed chunk of one driver — used to attach the
        actual transcript text onto transcription/opensearch/dynamodb
        chunk rows, in one query instead of one S3 read per file."""
        if not self.enabled or not self.client:
            return {}
        try:
            r = self.client.search(
                index=settings.opensearch_index,
                body={
                    "size": size,
                    "query": {"bool": {"must": [
                        {"term": {"SessionId": session_id}},
                        {"term": {"Driver": driver}},
                    ]}},
                    "_source": ["SourceFile", "Message", "OverallConfidence", "Team", "SessionTime"],
                }
            )
            out = {}
            for hit in r.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                sf = src.get("SourceFile", "")
                out[sf] = {
                    "message": src.get("Message"),
                    "confidence": src.get("OverallConfidence"),
                    "team": src.get("Team"),
                    "session_time": src.get("SessionTime"),
                }
            return out
        except Exception as e:
            print(f"OpenSearch transcript lookup error: {e}")
            return {}

    def search_transcripts(self, session_id: str, q: str | None = None,
                            driver: str | None = None, size: int = 100) -> list[dict]:
        """Used by /race/sessions/{id}/transcripts — was previously missing."""

        if not self.enabled or not self.client:
            return []
        must = [{"term": {"SessionId": session_id}}]
        if driver:
            must.append({"term": {"Driver": driver}})
        if q:
            must.append({"match": {"Message": q}})
        try:
            r = self.client.search(
                index=settings.opensearch_index,
                body={"query": {"bool": {"must": must}}, "size": size,
                      "sort": [{"Timestamp": "asc"}]}
            )
            return [h["_source"] for h in r.get("hits", {}).get("hits", [])]
        except Exception as e:
            print(f"OpenSearch search error: {e}")
            return []

opensearch_service = OpenSearchService()
