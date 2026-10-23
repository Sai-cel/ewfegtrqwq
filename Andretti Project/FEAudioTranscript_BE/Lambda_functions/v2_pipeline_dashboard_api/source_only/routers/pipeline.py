from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from services.dynamodb_service import dynamodb_service
from services.s3_service import s3_service
from services.opensearch_service import opensearch_service
import boto3
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import settings

router = APIRouter(prefix="/pipeline", tags=["pipeline"])
_ddb = boto3.client("dynamodb", region_name=settings.aws_region)

AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".wav", ".mp4")
JSON_EXTS = (".json",)

# stage name -> S3 prefix, used by the driver drill-down below
STAGE_PREFIXES = {
    "raw_chunks": settings.raw_chunks_prefix,
    "silence_removed": settings.silence_removed_prefix,
    "transcriptions": settings.transcriptions_prefix,
}

_SEGMENT_RE = re.compile(r"(segment-\d+)")

def segment_key(filename: str) -> str:
    """Every stage's filename contains 'segment-NNNNN' somewhere, even
    though the rest differs (segment-00003-00m30s.mp3 vs
    transcribe-BAR-segment-00003-00m30s-172....json). Pull out just that
    common part so records from different stages can be matched up."""
    m = _SEGMENT_RE.search(filename)
    return m.group(1) if m else filename

def _epoch_to_iso_ddb(value) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        return None

def _parse_ddb_timestamp(raw) -> float | None:
    """Seconds-epoch from the table's Timestamp sort key, whichever format it is.

    OLD rows: "1784798855"                      (epoch SECONDS, whole numbers)
    NEW rows: "1784798855664#transcribe-..."    (epoch MILLISECONDS + unique suffix)

    The suffix was added because the table is keyed on SessionId+Timestamp and
    whole-second timestamps collided: messages written in the same second
    overwrote each other and ~34% of rows were silently lost. Parsing must now
    take the part before the '#', and treat a 13-digit value as milliseconds.
    Returns None for anything unparseable so one odd row can't abort a page.
    """
    if raw is None:
        return None
    head = str(raw).split("#", 1)[0]
    try:
        value = float(head)
    except (TypeError, ValueError):
        return None
    # 1e11 seconds is year 5138, so anything above it must be milliseconds.
    return value / 1000.0 if value > 1e11 else value


def count_transcripts(session_id: str) -> int:
    """Total rows for a session in v2_texthash-table.

    MUST PAGE. A DynamoDB Query scans at most 1 MB per request and returns
    LastEvaluatedKey when there is more; a single call therefore counts only the
    first page. At roughly 1 KB per row that capped the answer near ~1000, so a
    1,416-chunk session reported 986 and looked like data loss on the board even
    though every row was present (the per-driver panel, which already paged,
    showed the correct figures). Same pattern as the per-driver query below.
    """
    total = 0
    kwargs = {
        "TableName": "v2_texthash-table",
        "KeyConditionExpression": "SessionId = :sid",
        "ExpressionAttributeValues": {":sid": {"S": session_id}},
        "Select": "COUNT",
    }
    try:
        while True:
            resp = _ddb.query(**kwargs)
            total += resp.get("Count", 0)
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except Exception as e:
        print(f"DynamoDB count error for {session_id}: {e}")
        return total
    return total

def driver_stage_range_dynamodb(session_id: str, driver: str) -> dict:
    """Count + first/last stored time for one driver in v2_texthash-table.

    The table's sort key is Timestamp (confirmed via its LastEvaluatedKey
    shape), so paging in the table's natural order means the first matching
    row we see for this driver is genuinely the earliest, and the last one
    we see is genuinely the latest — no separate sort needed.

    TIMESTAMP is a reserved word in DynamoDB, so it's aliased via
    ExpressionAttributeNames rather than referenced directly.
    """
    count = 0
    first_epoch = last_epoch = None
    kwargs = {
        "TableName": "v2_texthash-table",
        "KeyConditionExpression": "SessionId = :sid",
        "FilterExpression": "Driver = :d",
        "ExpressionAttributeValues": {":sid": {"S": session_id}, ":d": {"S": driver}},
        "ExpressionAttributeNames": {"#ts": "Timestamp"},
        "ProjectionExpression": "Driver, #ts",
    }
    try:
        while True:
            resp = _ddb.query(**kwargs)
            for item in resp.get("Items", []):
                count += 1
                raw_ts = item.get("Timestamp", {}).get("S") or item.get("Timestamp", {}).get("N")
                ts = _parse_ddb_timestamp(raw_ts)
                if ts is None:
                    continue
                if first_epoch is None or ts < first_epoch:
                    first_epoch = ts
                if last_epoch is None or ts > last_epoch:
                    last_epoch = ts
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except Exception as e:
        print(f"DynamoDB driver range error: {e}")
    return {
        "count": count,
        "first": _epoch_to_iso_ddb(first_epoch),
        "last": _epoch_to_iso_ddb(last_epoch),
    }

def driver_stage_chunks_dynamodb(session_id: str, driver: str) -> list[dict]:
    """Full per-record list for one driver from v2_texthash-table.
    Rows come back in the table's own sort-key (Timestamp) order, so no
    separate sort step is needed — same rule as the S3 chunk lists: only
    call this on expand, not for every driver up front."""
    out = []
    kwargs = {
        "TableName": "v2_texthash-table",
        "KeyConditionExpression": "SessionId = :sid",
        "FilterExpression": "Driver = :d",
        "ExpressionAttributeValues": {":sid": {"S": session_id}, ":d": {"S": driver}},
        "ExpressionAttributeNames": {"#ts": "Timestamp"},
        "ProjectionExpression": "SourceFile, #ts",
    }
    try:
        while True:
            resp = _ddb.query(**kwargs)
            for item in resp.get("Items", []):
                raw_ts = item.get("Timestamp", {}).get("S") or item.get("Timestamp", {}).get("N")
                parsed_ts = _parse_ddb_timestamp(raw_ts)
                out.append({
                    "file": item.get("SourceFile", {}).get("S", ""),
                    "last_modified": _epoch_to_iso_ddb(parsed_ts),
                })
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except Exception as e:
        print(f"DynamoDB driver chunks error: {e}")
    return out

def build_driver_row(session_id: str, driver: str) -> dict:
    raw = s3_service.driver_stage_range(STAGE_PREFIXES["raw_chunks"], session_id, driver, AUDIO_EXTS)
    silence = s3_service.driver_stage_range(STAGE_PREFIXES["silence_removed"], session_id, driver, AUDIO_EXTS)
    transcription = s3_service.driver_stage_range(STAGE_PREFIXES["transcriptions"], session_id, driver, JSON_EXTS)
    opensearch = opensearch_service.driver_stage_range(session_id, driver)
    dynamodb = driver_stage_range_dynamodb(session_id, driver)
    base = raw["count"]
    flagged = (base == 0) or (silence["count"] < base or transcription["count"] < base
                               or opensearch["count"] < base or dynamodb["count"] < base)
    return {
        "driver": driver,
        "flagged": flagged,
        "raw_chunks": raw,
        "silence_removed": silence,
        "transcription": {**transcription, "complete": transcription["count"] >= base and base > 0},
        "opensearch": {**opensearch, "complete": opensearch["count"] >= base and base > 0},
        "dynamodb": {**dynamodb, "complete": dynamodb["count"] >= base and base > 0},
        # The frontend's separate "Transcripts" box (actual spoken text,
        # not just presence) is backed by the same indexed OpenSearch
        # records — mirroring those same count/first/last values here
        # lets that box highlight immediately, without waiting for a
        # separate expand-triggered fetch of the transcript content.
        "transcripts": {**opensearch, "complete": opensearch["count"] >= base and base > 0},
    }

def get_session_row(s: dict) -> dict:
    sid = s["session_id"]
    try:
        counts = s3_service.stage_counts_for_session(sid)
    except Exception:
        counts = {}
    return {
        "session_id": sid,
        "race_name": s.get("race_name") or s.get("session_name") or sid,
        "status": s.get("status"),
        "streaming": s.get("status") == "ACTIVE",
        # Kept so the board can sort chronologically — sorting by
        # session_id string instead (the previous approach) sorted by
        # each race's *name*, not its actual date, which is why races
        # appeared in a seemingly random order on screen.
        "start_time": s.get("start_time") or s.get("created_at") or "",
        "stages": {
            "raw_chunks": counts.get("raw_chunks", 0),
            "silence_removed": counts.get("silence_removed", 0),
            "transcription": counts.get("transcriptions", 0),
            "opensearch": opensearch_service.count_documents(sid),
            "dynamodb": count_transcripts(sid),
        },
    }

@router.get("/board")
def board(status: str | None = Query(None),
          search: str | None = Query(None, description="Case-insensitive substring match on race name"),
          page: int = Query(1, ge=1),
          page_size: int = Query(10, ge=1, le=50)):
    """Single endpoint for all races, paginated. Status filter is an exact
    match (status=ACTIVE returns only ACTIVE, not SCHEDULED too). Search
    filter is a substring match against race_name/session_name.

    Only the sessions on the requested page get their S3/OpenSearch/
    DynamoDB counts computed — never the whole list — so page_size=10 is
    always fast (~1-3s) no matter how many total races exist. This is
    what actually makes "browse everything" safe, instead of raising a
    single-call limit number (which we tried, and which timed out at 50).
    """
    sessions = dynamodb_service.list_sessions()
    if status:
        wanted = status.strip().upper()
        sessions = [s for s in sessions if s.get("status") == wanted]
    if search:
        needle = search.strip().lower()
        sessions = [
            s for s in sessions
            if needle in (str(s.get("race_name") or "")).lower()
            or needle in (str(s.get("session_name") or "")).lower()
            or needle in (str(s.get("session_id") or "")).lower()
        ]
    total = len(sessions)
    total_pages = max(1, (total + page_size - 1) // page_size)
    start = (page - 1) * page_size
    page_sessions = sessions[start:start + page_size]
    rows = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(get_session_row, s): s for s in page_sessions}
        for future in as_completed(futures):
            try:
                rows.append(future.result())
            except Exception:
                pass
    rows.sort(key=lambda x: x.get("start_time") or "", reverse=True)
    return {
        "sessions": rows,
        "count": len(rows),
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }

@router.get("/health")
def health():
    return {"healthy": True, "stage_totals": {}, "notes": []}

@router.get("/session/{session_id}/drivers")
def session_drivers(session_id: str):
    """Per-driver stage summary for one session — collapsed range view.
    Powers the driver drill-down screen (one row expands per driver)."""
    session = dynamodb_service.get_session(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    drivers = [d.get("driver_name") for d in session.get("driver_snapshot", []) if d.get("driver_name")]
    if not drivers:
        drivers = s3_service._common_prefixes(f"{STAGE_PREFIXES['raw_chunks']}{session_id}")

    rows = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(build_driver_row, session_id, d): d for d in drivers}
        for future in as_completed(futures):
            try:
                rows.append(future.result())
            except Exception:
                pass
    rows.sort(key=lambda r: r["driver"])
    return {"session_id": session_id, "drivers": rows, "count": len(rows)}

@router.get("/session/{session_id}/drivers/{driver}/chunks")
def driver_chunks(session_id: str, driver: str,
                   stage: str = Query("raw_chunks", description="raw_chunks | silence_removed | transcriptions | opensearch | dynamodb")):
    """Full per-chunk/record list for one driver, one stage — only call this
    when a driver row is expanded, never for all drivers at once."""
    if stage in STAGE_PREFIXES:
        exts = JSON_EXTS if stage == "transcriptions" else AUDIO_EXTS
        chunks = s3_service.driver_stage_chunks(STAGE_PREFIXES[stage], session_id, driver, exts)
    elif stage == "opensearch":
        chunks = opensearch_service.driver_stage_chunks(session_id, driver)
    elif stage == "dynamodb":
        chunks = driver_stage_chunks_dynamodb(session_id, driver)
    else:
        raise HTTPException(400, f"stage must be one of {list(STAGE_PREFIXES) + ['opensearch', 'dynamodb']}")

    # Every chunk, regardless of stage, gets a link to its clean playable
    # audio — a temporary (1hr) link to the silence-removed .mp3 for that
    # same segment, so you can listen while inspecting any stage's data.
    # Transcription filenames look like "transcribe-BAR-segment-00000-00m00s-<epoch>.json",
    # so the plain "segment-NNNNN-MMmSSs" core is extracted with a regex
    # rather than assumed to already be a clean filename.
    segment_pattern = re.compile(r"segment-\d{5}-\d+m\d+s")
    for c in chunks:
        raw_name = c.get("file", "").split("/")[-1]
        match = segment_pattern.search(raw_name)
        segment_name = f"{match.group()}.mp3" if match else raw_name
        key = f"{settings.silence_removed_prefix.rstrip('/')}/{session_id}/{driver}/{segment_name}"
        try:
            c["audio_url"] = s3_service.presign(key, expires=3600)
        except Exception:
            c["audio_url"] = None

    # For the three stages backed by actual transcribed speech, attach the
    # real transcript text (message, confidence, team, session time) to
    # each row — one OpenSearch query for the whole driver, not one read
    # per chunk, since OpenSearch already has this content indexed.
    if stage in ("transcriptions", "opensearch", "dynamodb"):
        lookup = opensearch_service.driver_transcript_lookup(session_id, driver)
        for c in chunks:
            raw_name = c.get("file", "").split("/")[-1]
            match = segment_pattern.search(raw_name)
            plain_name = f"{match.group()}.mp3" if match else raw_name
            c["transcript"] = lookup.get(plain_name)

    return {"session_id": session_id, "driver": driver, "stage": stage,
            "chunks": chunks, "count": len(chunks)}

def _numeric_segment(seg: str) -> int:
    """segment-00003 -> 3, used to sort matrix rows in the right order."""
    m = re.search(r"\d+", seg)
    return int(m.group()) if m else 0

@router.get("/session/{session_id}/drivers/{driver}/matrix")
def driver_matrix(session_id: str, driver: str):
    """One row per audio segment, all 5 stages aligned side by side —
    built for the 'click a chunk, see it highlighted everywhere' debug
    feature. Scoped to a single driver, so this stays fast (a few
    seconds) even though it fetches all 5 stages at once — unlike
    /pipeline/board, this never scales with total session count."""
    raw = s3_service.driver_stage_chunks(STAGE_PREFIXES["raw_chunks"], session_id, driver, AUDIO_EXTS)
    silence = s3_service.driver_stage_chunks(STAGE_PREFIXES["silence_removed"], session_id, driver, AUDIO_EXTS)
    transcription = s3_service.driver_stage_chunks(STAGE_PREFIXES["transcriptions"], session_id, driver, JSON_EXTS)
    opensearch = opensearch_service.driver_stage_chunks(session_id, driver)
    dynamodb = driver_stage_chunks_dynamodb(session_id, driver)
    transcript_lookup = opensearch_service.driver_transcript_lookup(session_id, driver)

    stage_lists = {
        "raw_chunks": raw,
        "silence_removed": silence,
        "transcription": transcription,
        "opensearch": opensearch,
        "dynamodb": dynamodb,
    }

    # Build one lookup per stage: segment_key -> {file, last_modified}
    by_segment: dict[str, dict[str, dict]] = {}
    for stage_name, items in stage_lists.items():
        for item in items:
            filename = item.get("file", "")
            key = segment_key(filename)
            by_segment.setdefault(key, {})[stage_name] = {
                "present": True,
                "file": filename,
                "last_modified": item.get("last_modified"),
            }

    rows = []
    for seg, stages in sorted(by_segment.items(), key=lambda kv: _numeric_segment(kv[0])):
        for stage_name in stage_lists:
            stages.setdefault(stage_name, {"present": False, "file": None, "last_modified": None})

        # One audio link and one transcript per segment — same physical
        # clip regardless of which stage column you're looking at.
        raw_file = stages["raw_chunks"]["file"] or stages["silence_removed"]["file"]
        audio_url = None
        if raw_file:
            key_path = f"{settings.silence_removed_prefix.rstrip('/')}/{session_id}/{driver}/{raw_file}"
            try:
                audio_url = s3_service.presign(key_path, expires=3600)
            except Exception:
                audio_url = None
        transcript = transcript_lookup.get(raw_file) if raw_file else None

        flagged = any(not s["present"] for s in stages.values())
        rows.append({
            "segment": seg,
            "audio_url": audio_url,
            "transcript": transcript,
            "flagged": flagged,
            "stages": stages,
        })

    return {"session_id": session_id, "driver": driver, "segments": rows, "count": len(rows)}

