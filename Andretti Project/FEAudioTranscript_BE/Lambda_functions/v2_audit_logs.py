############ 07/08/2026 #################
"""Pipeline Logs API Lambda.

A separate, read-only Lambda that lets the UI pull end-to-end logs for a
session across every pipeline Lambda, without touching any of them. Each
pipeline stage (prod/*.py) already runs independently (S3/EventBridge/API
Gateway triggered) — this Lambda doesn't invoke or trigger them, it only
reads what they've already written, via three sources:

  GET /session-logs   (primary — complete, durable, per session)
      Every prod/*.py Lambda calls record_audit_event(...) at its key
      start/success/failure points. Session-scoped events (pipeline stages,
      session lifecycle, session config, recordings) get written to
      session_logs/{session_id}/ in AUDIT_S3_BUCKET. This endpoint lists
      that prefix for one session_id and returns every event, sorted
      chronologically — a guaranteed-complete record, independent of
      CloudWatch log retention.

  GET /system-logs   (primary — complete, durable, not session-scoped)
      Everything record_audit_event() writes that ISN'T session-scoped
      (auth, websocket connect/disconnect, driver/team/keywords config
      admin actions) lands in system_logs/YYYY/MM/DD/ instead. This
      endpoint lists a given day (default: today, UTC), optionally
      filtered, newest-first — same shape as config_management.py's
      existing /audit-logs endpoint, just reading the system_logs/ prefix.

  GET /pipeline-logs  (secondary — raw stdout, for debugging)
      Runs a CloudWatch Logs Insights query across every pipeline Lambda's
      log group, filtered by session_id appearing in the raw print() text.
      Useful for step-by-step debugging, but NOT complete: most print()
      lines don't mention session_id, so this only catches a minority of
      each invocation's output. Insights is async, so a request either
      returns results immediately or {status:"running", query_id} to poll.
      Params: session_id, function_name (one exact deployed Lambda name,
      e.g. "v2_transcriptions" — wins over stage if both given), stage (a
      STAGE_MAP nickname instead, e.g. "transcription"), start_time/end_time
      (epoch seconds) or hours (lookback shortcut, ignored if start_time is
      given), limit, query_id (to poll an existing query). Log group names
      that don't currently exist in CloudWatch are silently skipped rather
      than failing the whole request — see "missing_log_groups" in the
      response to tell a real "no matches" apart from a naming mismatch.

  GET /pipeline-logs/functions
      Lists the configured function -> stage mapping, for a UI filter/legend.

Environment variables
----------------------
  AUDIT_S3_BUCKET       S3 bucket the pipeline Lambdas write audit events to
                         (default: test-v2-andretti-audit-logs)
  AUDIT_S3_PREFIX        date-partitioned audit trail (default: audit-logs/)
  SESSION_LOGS_PREFIX     (default: session_logs/)
  SYSTEM_LOGS_PREFIX      (default: system_logs/)
  PIPELINE_FUNCTIONS     comma-separated Lambda function names to search via
                          CloudWatch (defaults to the deployed pipeline/API
                          Lambdas below)

IAM
---
This Lambda's execution role needs:
  s3:ListBucket, s3:GetObject on AUDIT_S3_BUCKET  (for /session-logs, /system-logs)
  logs:StartQuery, logs:GetQueryResults, logs:StopQuery
    scoped to the target log groups                (for /pipeline-logs)
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config as BotoConfig

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - only on very old runtimes
    ZoneInfo = None

S3_READ_WORKERS = 40  # parallel GetObject calls when reading a session/day's events

logs_client = boto3.client('logs')
# max_pool_connections must be >= S3_READ_WORKERS: botocore's default pool is
# 10, so 40 threads would fight over 10 sockets and the overflow connections
# get built + discarded on every call ("Connection pool is full, discarding
# connection"), paying a fresh TLS handshake per object. That alone turned a
# few-hundred-object session into a 29s API Gateway timeout.
s3_client = boto3.client('s3', config=BotoConfig(
    max_pool_connections=S3_READ_WORKERS,
    connect_timeout=3,
    read_timeout=5,
    retries={'max_attempts': 3, 'mode': 'standard'},
))

AUDIT_S3_BUCKET = os.environ.get('AUDIT_S3_BUCKET', 'test-v2-andretti-audit-logs')
AUDIT_S3_PREFIX = os.environ.get('AUDIT_S3_PREFIX', 'audit-logs/')
SESSION_LOGS_PREFIX = os.environ.get('SESSION_LOGS_PREFIX', 'session_logs/')
SYSTEM_LOGS_PREFIX = os.environ.get('SYSTEM_LOGS_PREFIX', 'system_logs/')
MAX_S3_LOG_SCAN = 2000  # cap objects read per request so a huge day/session can't time the Lambda out

# --- Function -> friendly stage name, in pipeline order -------------------
# Defaults match the Lambda files deployed from prod/. Override with
# PIPELINE_FUNCTIONS (comma-separated function names) if your deployed names
# differ.
DEFAULT_STAGE_MAP = {
    'v2_signup_login': 'auth',
    'v2_session_scheduler_lambda': 'scheduler',
    'v2_silence_removal': 'silence_removal',
    'v2_transcriptions': 'transcription',
    'v2_opensearch_dynamodb_storing': 'opensearch_dynamodb',
    'v2_websocket': 'websocket',
    'config_management': 'config_api',
}

_override = [f.strip() for f in os.environ.get('PIPELINE_FUNCTIONS', '').split(',') if f.strip()]
STAGE_MAP = (
    {name: DEFAULT_STAGE_MAP.get(name, name) for name in _override}
    if _override else DEFAULT_STAGE_MAP
)
LOG_GROUPS = [f"/aws/lambda/{name}" for name in STAGE_MAP]

# Action prefixes to drop from /session-logs and /system-logs entirely (still
# written to S3 by their source Lambdas, just not surfaced here).
EXCLUDED_ACTION_PREFIXES = ('CHUNKING_', 'AUDIO_MERGE_')

MAX_LIMIT = 1000
DEFAULT_LIMIT = 200
DEFAULT_WINDOW_SECONDS = 3600          # look back 1 hour if no start_time given
POLL_INTERVAL_SECONDS = 0.5
POLL_SAFETY_MARGIN_MS = 2000           # stop polling this long before Lambda times out
S3_SCAN_SAFETY_MARGIN_MS = 5000        # leave this much room to serialize + return partial results
API_GATEWAY_TIMEOUT_SECONDS = 29       # hard ceiling regardless of the Lambda's own timeout

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'GET,OPTIONS',
}


def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': CORS_HEADERS,
        'body': json.dumps(body),
    }


def _escape_for_insights_regex(value):
    """Escape a value for safe use inside a CloudWatch Insights `like /.../ ` literal."""
    return re.sub(r'([.^$|()\[\]{}*+?\\/])', r'\\\1', value)


def _stage_for_log_group(log_group):
    name = log_group.replace('/aws/lambda/', '', 1)
    return STAGE_MAP.get(name, name)


def _detect_level(message):
    upper = message.upper()
    if 'TRACEBACK' in upper or 'EXCEPTION' in upper or '[ERROR]' in upper or 'ERROR:' in upper:
        return 'ERROR'
    if '[WARN' in upper or 'WARNING' in upper:
        return 'WARN'
    return 'INFO'


def _row_to_dict(row):
    """Insights returns a list of {field, value} dicts per result row."""
    return {field['field']: field['value'] for field in row}


def _coerce_epoch_seconds(raw, default):
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


_MISSING_LOG_GROUP_RE = re.compile(r"Log group '([^']+)' does not exist")


def _start_query_skipping_missing(log_groups, start_time, end_time, query_string, limit):
    """Call start_query; if CloudWatch rejects it because one of the log
    groups doesn't exist, drop exactly that one (parsed from the error
    message) and retry, repeating until it succeeds or every candidate has
    been tried. This makes CloudWatch itself the source of truth for
    "does this log group exist" instead of a separate describe_log_groups
    pre-check, which can silently misreport "missing" if IT fails for any
    reason (e.g. a permissions gap on describe_log_groups specifically) even
    though the log group is fine and start_query would have accepted it.

    Returns (query_id, used_log_groups, missing_log_groups). query_id is
    None if every candidate got rejected."""
    remaining = list(log_groups)
    missing = []
    while remaining:
        try:
            started = logs_client.start_query(
                logGroupNames=remaining,
                startTime=start_time,
                endTime=end_time,
                queryString=query_string,
                limit=limit,
            )
            return started['queryId'], remaining, missing
        except logs_client.exceptions.ResourceNotFoundException as e:
            match = _MISSING_LOG_GROUP_RE.search(str(e))
            if not match or match.group(1) not in remaining:
                # Can't tell which specific group is at fault -> stop rather
                # than risk looping forever removing nothing.
                raise
            bad = match.group(1)
            print(f"[PIPELINE-LOGS] Log group does not exist, skipping: {bad}")
            remaining.remove(bad)
            missing.append(bad)
    return None, [], missing


def handle_query_logs(params, context):
    session_id = (params.get('session_id') or '').strip()
    query_id = (params.get('query_id') or '').strip()

    if query_id:
        return _poll_query(query_id)

    now = int(time.time())
    end_time = _coerce_epoch_seconds(params.get('end_time'), now)
    hours = (params.get('hours') or '').strip()
    if hours and not params.get('start_time'):
        try:
            start_time = end_time - int(float(hours) * 3600)
        except (TypeError, ValueError):
            start_time = end_time - DEFAULT_WINDOW_SECONDS
    else:
        start_time = _coerce_epoch_seconds(params.get('start_time'), end_time - DEFAULT_WINDOW_SECONDS)
    if start_time >= end_time:
        return response(400, {'error': 'start_time must be before end_time'})

    try:
        limit = min(int(params.get('limit', DEFAULT_LIMIT)), MAX_LIMIT)
    except (TypeError, ValueError):
        limit = DEFAULT_LIMIT

    # `function_name` filters to one exact deployed Lambda name directly.
    # `stage` filters via the friendly STAGE_MAP nickname instead. Either,
    # neither, or (redundantly) both may be given; function_name wins if both.
    function_name = (params.get('function_name') or '').strip()
    stage = (params.get('stage') or '').strip()
    if function_name:
        log_groups = [f"/aws/lambda/{function_name}"]
    elif stage:
        log_groups = [f"/aws/lambda/{name}" for name, s in STAGE_MAP.items() if s == stage]
        if not log_groups:
            return response(400, {'error': f'Unknown stage "{stage}"', 'valid_stages': sorted(set(STAGE_MAP.values()))})
    else:
        log_groups = LOG_GROUPS

    query_parts = ['fields @timestamp, @message, @log']
    if session_id:
        pattern = _escape_for_insights_regex(session_id)
        query_parts.append(f'filter @message like /{pattern}/')
    query_parts.append('sort @timestamp asc')
    query_parts.append(f'limit {limit}')
    query_string = ' | '.join(query_parts)

    try:
        query_id, log_groups, missing_log_groups = _start_query_skipping_missing(
            log_groups, start_time, end_time, query_string, limit
        )
    except Exception as e:  # noqa: BLE001
        print(f"[PIPELINE-LOGS] start_query failed: {e}")
        return response(500, {'error': str(e)})

    if query_id is None:
        return response(404, {
            'error': 'None of the requested log groups exist in CloudWatch (check the function name, or that it has run at least once).',
            'missing_log_groups': missing_log_groups,
        })

    remaining_ms = getattr(context, 'get_remaining_time_in_millis', lambda: 10000)()
    deadline = time.time() + max((remaining_ms - POLL_SAFETY_MARGIN_MS) / 1000.0, 0)

    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_SECONDS)
        result = _poll_query(query_id, wrap_response=False)
        if result['status'] in ('Complete', 'Failed', 'Cancelled', 'Timeout'):
            result['missing_log_groups'] = missing_log_groups
            return response(200, result)

    return response(202, {
        'status': 'running', 'query_id': query_id,
        'log_groups': log_groups, 'missing_log_groups': missing_log_groups,
    })


def _poll_query(query_id, wrap_response=True):
    try:
        result = logs_client.get_query_results(queryId=query_id)
    except Exception as e:  # noqa: BLE001
        print(f"[PIPELINE-LOGS] get_query_results failed for {query_id}: {e}")
        payload = {'status': 'error', 'query_id': query_id, 'error': str(e)}
        return response(500, payload) if wrap_response else payload

    status = result.get('status', 'Unknown')
    entries = []
    for row in result.get('results', []):
        fields = _row_to_dict(row)
        message = fields.get('@message', '')
        entries.append({
            'timestamp': fields.get('@timestamp'),
            'log_group': fields.get('@log', '').split(':')[-1] if fields.get('@log') else '',
            'stage': _stage_for_log_group(fields.get('@log', '').split(':')[-1]) if fields.get('@log') else '',
            'level': _detect_level(message),
            'message': message,
        })

    payload = {
        'status': status,
        'query_id': query_id,
        'count': len(entries),
        'entries': entries,
        'stats': result.get('statistics'),
    }
    return response(200, payload) if wrap_response else payload


def _readable_message(rec):
    """Build a one-line, plain-English summary of an audit event, e.g.
    'Silence Removal Start — TOKYO_EPRIX... (chunk id: segment-00040-06m40s, driver folder: BUE)'.
    Generic by design (derived from action/entity_id/details) so it stays in
    sync automatically as new action types get added upstream, without
    needing a template per action here."""
    action = str(rec.get('action', '')).replace('_', ' ').title()
    entity_id = rec.get('entity_id', '')
    details = rec.get('details') or {}
    detail_str = ', '.join(f"{k.replace('_', ' ')}: {v}" for k, v in details.items() if v not in (None, ''))

    parts = [action]
    if entity_id:
        parts.append(f"— {entity_id}")
    message = ' '.join(parts)
    if detail_str:
        message += f" ({detail_str})"
    return message


def _fetch_event(key, log_tag):
    """Read + parse one audit-event object. Returns the dict, or None if it
    can't be read/parsed (a missing/racing/corrupt object shouldn't take down
    the whole request)."""
    try:
        body = s3_client.get_object(Bucket=AUDIT_S3_BUCKET, Key=key)['Body'].read()
        rec = json.loads(body)
    except Exception as e:  # noqa: BLE001
        print(f"[{log_tag}] Skipping {key}: {e}")
        return None
    return rec if isinstance(rec, dict) else None


def _parse_before(before_str):
    """Parse a `before` pagination cursor (an event's own ISO `timestamp`
    string, as returned in a previous page) into a UTC-aware datetime for
    comparison against S3 LastModified. Returns None if missing/unparseable —
    callers should treat that as "no cursor", not an error, since a bad
    cursor shouldn't break the request."""
    if not before_str:
        return None
    try:
        return datetime.fromisoformat(before_str.replace('Z', '+00:00'))
    except ValueError:
        return None


def _resolve_timezone(tz_str):
    """Resolve a `timezone` query param into a tzinfo, so /system-logs can
    treat `date` as a calendar day in a session's own timezone instead of
    always UTC (v2_session_scheduler_lambda.py lets each session set one).
    Accepts an IANA name ('Asia/Kolkata') or a fixed offset ('+05:30',
    '-04:00'). Defaults to UTC for anything empty/unrecognized, same
    fallback behavior as the scheduler uses for its own timezone field."""
    if not tz_str or tz_str.upper() == 'UTC':
        return timezone.utc
    if ZoneInfo is not None:
        try:
            return ZoneInfo(tz_str)
        except Exception:  # noqa: BLE001
            pass
    match = re.match(r'^([+-])(\d{2}):?(\d{2})$', tz_str)
    if match:
        sign = 1 if match.group(1) == '+' else -1
        hours, minutes = int(match.group(2)), int(match.group(3))
        return timezone(sign * timedelta(hours=hours, minutes=minutes))
    print(f"[SYSTEM-LOGS] Unknown timezone '{tz_str}'; defaulting to UTC")
    return timezone.utc


def _scan_deadline(context):
    """Wall-clock time by which the S3 scan must stop and hand back whatever it
    has. Bounded by BOTH the Lambda's remaining time and API Gateway's own 29s
    integration timeout — overrunning the latter is what produces a bare
    504 {"message": "Endpoint request timed out"} with no usable body, so we'd
    rather return a partial (truncated=true) page the caller can paginate."""
    remaining_ms = getattr(context, 'get_remaining_time_in_millis', lambda: None)()
    budget = API_GATEWAY_TIMEOUT_SECONDS - (S3_SCAN_SAFETY_MARGIN_MS / 1000.0)
    if remaining_ms is not None:
        budget = min(budget, max((remaining_ms - S3_SCAN_SAFETY_MARGIN_MS) / 1000.0, 1.0))
    return time.time() + budget


def _list_and_read_events(prefix, log_tag, action_filter=None, entity_filter=None, created_by_filter=None,
                          fetch_limit=None, before=None, deadline=None):
    """List every .json object under an S3 prefix, read + parse each one, and
    return (events, truncated). Shared by /session-logs and /system-logs —
    they differ only in which prefix they scan and which filters apply.

    Listing is cheap (a handful of paginated ListObjectsV2 calls covering up
    to MAX_S3_LOG_SCAN keys), but reading is not: a busy session/day can mean
    hundreds of individual objects, and fetching them one at a time in a
    sequential loop is slow enough to blow past API Gateway's 29s integration
    timeout (a real 502 seen in production, not hypothetical). Reads are
    parallelized across S3_READ_WORKERS threads to keep this fast regardless
    of volume.

    `before` is a pagination cursor (an ISO timestamp) — when given, only
    objects older than it are considered at all, letting the caller page
    backwards through history one `fetch_limit`-sized batch at a time.

    When no action/entity/created_by filter is active and fetch_limit is
    given, we only need the most recent `fetch_limit` events (after the
    `before` cutoff), so we skip fetching the rest entirely. Listing already
    gives us each object's LastModified time for free (these audit-event
    objects are write-once, so LastModified is effectively the event time) —
    sort by that and fetch only the newest slice, instead of reading every
    object just to throw most of them away afterward."""
    key_times = []
    truncated = False
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=AUDIT_S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.json'):
                continue
            if len(key_times) >= MAX_S3_LOG_SCAN:
                truncated = True
                break
            key_times.append((key, obj['LastModified']))
        if truncated:
            break
        if deadline and time.time() > deadline:
            print(f"[{log_tag}] Deadline hit while listing {prefix} at {len(key_times)} keys")
            truncated = True
            break

    before_dt = _parse_before(before)
    if before_dt:
        key_times = [(k, lm) for k, lm in key_times if lm < before_dt]

    no_filters = not (action_filter or entity_filter or created_by_filter)
    keys_to_fetch = [k for k, _ in key_times]
    if no_filters and fetch_limit and len(key_times) > fetch_limit:
        key_times.sort(key=lambda kt: kt[1], reverse=True)
        keys_to_fetch = [k for k, _ in key_times[:fetch_limit]]
        truncated = True

    events = []
    # Not a `with` block: on shutdown that would wait for every queued read
    # even after we've decided to bail, which is exactly the stall we're
    # trying to avoid. cancel_futures drops the ones that haven't started.
    executor = ThreadPoolExecutor(max_workers=S3_READ_WORKERS)
    try:
        futures = [executor.submit(_fetch_event, key, log_tag) for key in keys_to_fetch]
        for i, future in enumerate(futures):
            if deadline and time.time() > deadline:
                print(f"[{log_tag}] Deadline hit after reading {i}/{len(futures)} objects under {prefix}; "
                      "returning partial results")
                truncated = True
                break
            rec = future.result()
            if rec is None:
                continue
            if str(rec.get('action', '')).upper().startswith(EXCLUDED_ACTION_PREFIXES):
                continue
            if action_filter and str(rec.get('action', '')).upper() != action_filter:
                continue
            if entity_filter and str(rec.get('entity', '')).lower() != entity_filter:
                continue
            if created_by_filter and str(rec.get('created_by', '')).lower() != created_by_filter:
                continue
            rec['message'] = _readable_message(rec)
            events.append(rec)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return events, truncated


def handle_session_logs(params, context):
    """Complete, durable log timeline for one session, read from S3.

    GET /session-logs?session_id=...&action=...&limit=N&before=<timestamp>

    Pagination: if the response has "truncated": true, it also includes
    "next_before" — pass that back as `before` on the next call to fetch the
    next-older batch (e.g. page 1: no `before`; page 2: before=<page 1's
    next_before>; and so on until "truncated" is false / "next_before" is null).
    """
    if not AUDIT_S3_BUCKET:
        return response(500, {'error': 'AUDIT_S3_BUCKET is not configured'})

    session_id = (params.get('session_id') or '').strip()
    if not session_id:
        return response(400, {'error': 'session_id is required'})

    action_filter = (params.get('action') or '').strip().upper()
    before = (params.get('before') or '').strip()
    try:
        limit = int(params.get('limit', DEFAULT_LIMIT))
    except (TypeError, ValueError):
        limit = DEFAULT_LIMIT
    limit = max(1, min(limit, MAX_LIMIT))

    prefix = f"{SESSION_LOGS_PREFIX}{session_id}/"
    try:
        events, truncated = _list_and_read_events(
            prefix, 'SESSION-LOGS', action_filter=action_filter, fetch_limit=limit, before=before,
            deadline=_scan_deadline(context),
        )
    except Exception as e:  # noqa: BLE001
        print(f"[SESSION-LOGS] Error reading session logs for {session_id}: {e}")
        return response(500, {'error': str(e)})

    events.sort(key=lambda r: r.get('timestamp', ''))
    truncated = truncated or len(events) > limit
    events = events[:limit]
    next_before = events[0]['timestamp'] if events and truncated else None

    return response(200, {
        'session_id': session_id,
        'count': len(events),
        'truncated': truncated,
        'next_before': next_before,
        'events': events,
    })


def handle_system_logs(params, context):
    """Complete, durable log timeline for non-session-scoped events (auth,
    websocket connections, driver/team/keywords config admin actions), read
    from S3.

    GET /system-logs?date=YYYY-MM-DD&action=&entity=&created_by=&limit=N&before=<timestamp>&timezone=<tz>

    `timezone` (optional, default UTC) interprets `date` as a calendar day in
    that timezone instead of UTC — e.g. to match a session's own timezone
    (v2_session_scheduler_lambda.py lets each session set one), pass that
    same value here. Accepts an IANA name ('Asia/Kolkata') or a fixed offset
    ('+05:30'). Since a local day can span two UTC calendar days, this scans
    up to two S3 day-prefixes and filters down to the exact local-day window;
    with no timezone (or 'UTC') it behaves exactly as before — one day-prefix.

    Pagination: if the response has "truncated": true, it also includes
    "next_before" — pass that back as `before` on the next call for that same
    date (and timezone) to fetch the next-older batch, same pattern as
    /session-logs.
    """
    if not AUDIT_S3_BUCKET:
        return response(500, {'error': 'AUDIT_S3_BUCKET is not configured'})

    date_str = (params.get('date') or '').strip() or datetime.now(timezone.utc).strftime('%Y-%m-%d')
    try:
        y, m, d = (int(part) for part in date_str.split('-'))
    except ValueError:
        return response(400, {'error': 'date must be in YYYY-MM-DD format'})

    tz = _resolve_timezone((params.get('timezone') or '').strip())
    local_start = datetime(y, m, d, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)

    day_prefixes = sorted({
        f"{SYSTEM_LOGS_PREFIX}{dt.strftime('%Y/%m/%d')}/"
        for dt in (utc_start, utc_end - timedelta(microseconds=1))
    })

    action_filter = (params.get('action') or '').strip().upper()
    entity_filter = (params.get('entity') or '').strip().lower()
    created_by_filter = (params.get('created_by') or '').strip().lower()
    before = (params.get('before') or '').strip()
    try:
        limit = int(params.get('limit', DEFAULT_LIMIT))
    except (TypeError, ValueError):
        limit = DEFAULT_LIMIT
    limit = max(1, min(limit, MAX_LIMIT))

    # fetch_limit's "most recent N" cap only makes sense scanning one prefix;
    # with a non-UTC timezone spanning two UTC day-prefixes we need the full
    # range from both before we can tell which events land inside the local day.
    fetch_limit = limit if len(day_prefixes) == 1 else None

    deadline = _scan_deadline(context)
    try:
        events = []
        truncated = False
        for day_prefix in day_prefixes:
            day_events, day_truncated = _list_and_read_events(
                day_prefix, 'SYSTEM-LOGS',
                action_filter=action_filter, entity_filter=entity_filter, created_by_filter=created_by_filter,
                fetch_limit=fetch_limit, before=before, deadline=deadline,
            )
            events.extend(day_events)
            truncated = truncated or day_truncated
    except Exception as e:  # noqa: BLE001
        print(f"[SYSTEM-LOGS] Error reading system logs for {date_str}: {e}")
        return response(500, {'error': str(e)})

    if len(day_prefixes) > 1:
        filtered = []
        for rec in events:
            ts = _parse_before(rec.get('timestamp'))
            if ts is not None and utc_start <= ts < utc_end:
                filtered.append(rec)
        events = filtered

    events.sort(key=lambda r: r.get('timestamp', ''), reverse=True)
    truncated = truncated or len(events) > limit
    events = events[:limit]
    next_before = events[-1]['timestamp'] if events and truncated else None

    return response(200, {
        'date': date_str,
        'count': len(events),
        'truncated': truncated,
        'next_before': next_before,
        'events': events,
    })


def handle_list_functions(_params, _context):
    stages = sorted(set(STAGE_MAP.values()))
    return response(200, {
        'functions': [{'function_name': name, 'stage': stage} for name, stage in STAGE_MAP.items()],
        'stages': stages,
    })


def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method', ''))
    if http_method == 'OPTIONS':
        return response(200, {'message': 'OK'})

    path = event.get('path', event.get('rawPath', ''))
    params = event.get('queryStringParameters') or {}

    if path.rstrip('/').endswith('/functions'):
        return handle_list_functions(params, context)
    if '/session-logs' in path:
        return handle_session_logs(params, context)
    if '/system-logs' in path:
        return handle_system_logs(params, context)
    if '/pipeline-logs' in path:
        return handle_query_logs(params, context)

    return response(404, {'error': 'Endpoint not found'})


