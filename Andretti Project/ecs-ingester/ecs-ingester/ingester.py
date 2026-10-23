"""
ECS Audio Ingester
Live stream recording from M3U8 sources to S3
Features:
- Multi-driver concurrent recording
- Auto-generated session IDs (timestamp-based)
- Driver metadata upload for downstream processing
- Graceful shutdown with session status updates
- Offline retry mechanism for stream interruptions
Author: People Tech Group
Updated: December 29, 2024
"""
import os
import re
import boto3
import botocore.exceptions
import subprocess
import multiprocessing
import time
import glob
import signal
import json
from datetime import datetime, timezone
def sanitize_for_path(name):
    """
    Sanitize a name for use in S3 paths and local directories.
    Replaces spaces and special characters with underscores.
    Example: "Colton Herta" -> "Colton_Herta"
    """
    return re.sub(r'[^0-9a-zA-Z._-]', '_', name)
# =====================================================
# CONFIGURATION
# =====================================================
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET = os.getenv("S3_BUCKET", "test-v2-andretti-transcription-system")
S3_PREFIX = os.getenv("S3_PREFIX", "raw-chunks")
# Full-race parallel recording (records the entire session to one file per driver)
FULL_RACE_PREFIX = os.getenv("FULL_RACE_PREFIX", "full-race-recordings")
ENABLE_FULL_RECORDING = os.getenv("ENABLE_FULL_RECORDING", "true").lower() == "true"
# Segment configuration
SEGMENT_DURATION = int(os.getenv("SEGMENT_DURATION", "10"))  # seconds per chunk
OUTPUT_DIR = "/tmp/audio_output"
# Continuous mode - set to True for live streaming
CONTINUOUS_MODE = os.getenv("CONTINUOUS_MODE", "true").lower() == "true"
# Maximum segments to keep locally (to prevent disk fill)
MAX_LOCAL_SEGMENTS = int(os.getenv("MAX_LOCAL_SEGMENTS", "10"))
# Offline retry configuration
OFFLINE_RETRY_WAIT = int(os.getenv("OFFLINE_RETRY_WAIT", "300"))  # 5 minutes
MAX_OFFLINE_RETRIES = int(os.getenv("MAX_OFFLINE_RETRIES", "1"))  # 0 = infinite retries
# DynamoDB table for session tracking (v2 prefix to avoid conflicts)
ACTIVE_SESSIONS_TABLE = os.environ.get("ACTIVE_SESSIONS_TABLE", "v2_ActiveSessions")
# DynamoDB tables for frontend configuration (v2 prefix)
DRIVER_CONFIG_TABLE = os.environ.get("DRIVER_CONFIG_TABLE", "v2_DriverConfig")
SESSION_CONFIG_TABLE = os.environ.get("SESSION_CONFIG_TABLE", "v2_SessionConfig")
# =====================================================
# SESSION IDENTIFICATION
# =====================================================
TASK_START_TIME = datetime.now(timezone.utc)
# Check if session scheduler passed config via environment variables
ENV_SESSION_ID = os.environ.get("SESSION_ID")
ENV_DRIVER_URL = os.environ.get("DRIVER_01_URL")
# Use SESSION_ID from environment (passed by session scheduler) or generate one
if ENV_SESSION_ID:
    SESSION_ID = ENV_SESSION_ID
    print(f"[SESSION] Using SESSION_ID from environment: {SESSION_ID}")
else:
    SESSION_ID = TASK_START_TIME.strftime("%Y-%m-%d_%H-%M")
    print(f"[SESSION] Generated SESSION_ID: {SESSION_ID}")
# Determine config source: If DRIVER_01_URL is set, use env vars (session scheduler passed them)
# Otherwise fall back to DynamoDB
if ENV_DRIVER_URL:
    USE_DYNAMODB_CONFIG = False
    print(f"[CONFIG] Driver config passed via environment variables - will use env vars")
else:
    USE_DYNAMODB_CONFIG = os.environ.get("USE_DYNAMODB_CONFIG", "true").lower() == "true"
    print(f"[CONFIG] No env driver config - will use DynamoDB (USE_DYNAMODB_CONFIG={USE_DYNAMODB_CONFIG})")
# =====================================================
# DRIVER CONFIGURATION
# =====================================================
def get_driver_config_from_dynamodb():
    """
    Get driver configuration from DynamoDB (frontend config).
    Returns dict of driver configs if found, None otherwise.
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(DRIVER_CONFIG_TABLE)
        # Scan for active driver configs
        response = table.scan(
            FilterExpression="is_active = :active",
            ExpressionAttributeValues={":active": True}
        )
        items = response.get('Items', [])
        if not items:
            print("[CONFIG] No active driver configs found in DynamoDB")
            return None
        config = {}
        for idx, item in enumerate(items, start=1):
            driver_id = item.get('driver_id', f'driver{idx:02d}')
            config[idx] = {
                "m3u8_url": item.get('stream_url'),
                "driver_name": item.get('driver_name', driver_id),
                "team": item.get('team', 'UNKNOWN')
            }
            url_preview = config[idx]['m3u8_url'][:50] if config[idx]['m3u8_url'] else 'None'
            print(f"[CONFIG] DynamoDB Driver {idx}: Name={config[idx]['driver_name']}, "
                  f"Team={config[idx]['team']}, URL={url_preview}...")
        print(f"[CONFIG] Loaded {len(config)} drivers from DynamoDB")
        return config
    except Exception as e:
        print(f"[CONFIG] Error reading DynamoDB: {e}")
        return None
def get_driver_config_from_env():
    """
    Build driver config from environment variables (fallback).
    Reads DRIVER_XX_URL, DRIVER_XX_NAME, DRIVER_XX_TEAM for each driver.
    """
    config = {}
    for driver_num in range(1, 23):  # Driver 1 to 22
        url_var = f"DRIVER_{driver_num:02d}_URL"
        name_var = f"DRIVER_{driver_num:02d}_NAME"
        team_var = f"DRIVER_{driver_num:02d}_TEAM"
        url = os.getenv(url_var)
        name = os.getenv(name_var, f"driver{driver_num:02d}")
        team = os.getenv(team_var, "UNKNOWN")
        if url:
            config[driver_num] = {
                "m3u8_url": url,
                "driver_name": name,
                "team": team
            }
            print(f"[CONFIG] ENV Driver {driver_num:02d}: Name={name}, Team={team}, URL={url[:50]}...")
    return config
def build_driver_config():
    """
    Build driver config - tries DynamoDB first, falls back to ENV vars.
    Priority:
    1. If USE_DYNAMODB_CONFIG=true, try DynamoDB first
    2. If DynamoDB empty/error, fall back to environment variables
    """
    config = None
    if USE_DYNAMODB_CONFIG:
        print("[CONFIG] Checking DynamoDB for driver configuration...")
        config = get_driver_config_from_dynamodb()
    if not config:
        print("[CONFIG] Using environment variables for driver configuration...")
        config = get_driver_config_from_env()
    if not config:
        print("[WARNING] No driver URLs found in DynamoDB or environment variables!")
        print("[WARNING] Configure drivers via frontend or set DRIVER_01_URL, DRIVER_02_URL, etc.")
    return config
# =====================================================
# SESSION SCHEDULE CHECK
# =====================================================
def check_session_schedule():
    """
    Check if there's an active session scheduled in SessionConfig.
    Returns session info if active, None otherwise.
    Session is considered active if:
    - status is "scheduled" or "active"
    - current time is between start_time and end_time
    """
    if not USE_DYNAMODB_CONFIG:
        return None  # Skip if not using DynamoDB config
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(SESSION_CONFIG_TABLE)
        response = table.scan()
        sessions = response.get('Items', [])
        now = datetime.now(timezone.utc)
        for session in sessions:
            status = session.get('status', 'scheduled')
            if status not in ['scheduled', 'active']:
                continue
            try:
                # Parse ISO 8601 timestamps
                start_str = session.get('start_time', '')
                end_str = session.get('end_time', '')
                if not start_str or not end_str:
                    continue
                # Handle different timestamp formats
                start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                if start_time <= now <= end_time:
                    print(f"[SESSION] Found active session: {session.get('session_name')}")
                    print(f"[SESSION] Start: {start_time}, End: {end_time}")
                    return session
            except Exception as e:
                print(f"[SESSION] Error parsing session times: {e}")
                continue
        print("[SESSION] No active session found in schedule")
        return None
    except Exception as e:
        print(f"[SESSION] Error checking session schedule: {e}")
        return None
def wait_for_session_start():
    """
    Wait for a scheduled session to start.
    Polls SessionConfig every 60 seconds until a session becomes active.
    Returns when session is active or after timeout.
    """
    if not USE_DYNAMODB_CONFIG:
        print("[SESSION] DynamoDB config disabled, starting immediately")
        return True
    print("[SESSION] Checking for scheduled session...")
    max_wait_hours = 24  # Maximum time to wait
    check_interval = 60  # Check every 60 seconds
    max_checks = (max_wait_hours * 3600) // check_interval
    for check_num in range(max_checks):
        session = check_session_schedule()
        if session:
            print(f"[SESSION] Session is active: {session.get('session_name')}")
            return True
        if check_num == 0:
            print(f"[SESSION] No active session. Waiting for scheduled session...")
            print(f"[SESSION] Checking every {check_interval} seconds...")
        time.sleep(check_interval)
    print("[SESSION] Timeout waiting for session. Starting anyway.")
    return False
# Build driver config (from DynamoDB or ENV vars)
DRIVER_CONFIG = build_driver_config()
# =====================================================
# LOGGING
# =====================================================
def log(driver_id, message, level="INFO"):
    print(
        f"[{level}] [DRIVER {driver_id:02d}] "
        f"{datetime.now(timezone.utc).isoformat()} :: {message}",
        flush=True,
    )
def log_info(driver_id, message):
    log(driver_id, message, "INFO")
def log_error(driver_id, message):
    log(driver_id, message, "ERROR")
def log_warn(driver_id, message):
    log(driver_id, message, "WARN")
def log_debug(driver_id, message):
    log(driver_id, message, "DEBUG")
def log_system(message, level="INFO"):
    """Log system-wide messages without driver context"""
    print(
        f"[{level}] [SYSTEM] "
        f"{datetime.now(timezone.utc).isoformat()} :: {message}",
        flush=True,
    )
# =====================================================
# AWS CLIENTS
# =====================================================
def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)
# =====================================================
# SESSION STATUS MANAGEMENT
# =====================================================
def update_session_status(session_id, status):
    """
    Update the ActiveSessions DynamoDB table with ECS session status.
    status: 'ACTIVE' when ECS starts, 'STOPPED' when ECS stops
    IMPORTANT: Uses update_item() instead of put_item() to preserve existing
    fields like EcsTaskArn that were set by the session scheduler Lambda.
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(ACTIVE_SESSIONS_TABLE)
        now_ts = int(time.time())
        if status == 'ACTIVE':
            # Update status and timestamps, preserve EcsTaskArn etc.
            table.update_item(
                Key={'SessionId': session_id},
                UpdateExpression="SET #status = :status, #updated_at = :updated_at, #started_at = if_not_exists(#started_at, :started_at)",
                ExpressionAttributeNames={
                    '#status': 'Status',
                    '#updated_at': 'UpdatedAt',
                    '#started_at': 'StartedAt'
                },
                ExpressionAttributeValues={
                    ':status': status,
                    ':updated_at': now_ts,
                    ':started_at': now_ts
                }
            )
        elif status == 'STOPPED':
            table.update_item(
                Key={'SessionId': session_id},
                UpdateExpression="SET #status = :status, #updated_at = :updated_at, #stopped_at = :stopped_at",
                ExpressionAttributeNames={
                    '#status': 'Status',
                    '#updated_at': 'UpdatedAt',
                    '#stopped_at': 'StoppedAt'
                },
                ExpressionAttributeValues={
                    ':status': status,
                    ':updated_at': now_ts,
                    ':stopped_at': now_ts
                }
            )
        else:
            # Generic status update
            table.update_item(
                Key={'SessionId': session_id},
                UpdateExpression="SET #status = :status, #updated_at = :updated_at",
                ExpressionAttributeNames={
                    '#status': 'Status',
                    '#updated_at': 'UpdatedAt'
                },
                ExpressionAttributeValues={
                    ':status': status,
                    ':updated_at': now_ts
                }
            )
        log_system(f"[SESSION_STATUS] Updated session {session_id} to {status}")
    except Exception as e:
        log_system(f"[SESSION_STATUS] Error updating session status: {e}")
# =====================================================
# S3 UPLOAD
# =====================================================
def upload_segment_to_s3(driver_id, local_file_path, s3_key):
    """Upload a single segment to S3"""
    try:
        if not os.path.exists(local_file_path):
            log_error(driver_id, f"Local file not found: {local_file_path}")
            return False
        file_size = os.path.getsize(local_file_path)
        if file_size == 0:
            log_error(driver_id, f"Cannot upload empty file: {local_file_path}")
            return False
        s3 = get_s3_client()
        log_info(driver_id, f"Uploading {local_file_path} ({file_size / 1024:.2f} KB)")
        log_info(driver_id, f"S3 destination: s3://{S3_BUCKET}/{s3_key}")
        start_time = time.time()
        s3.upload_file(local_file_path, S3_BUCKET, s3_key)
        elapsed_time = time.time() - start_time
        log_info(driver_id, f"Upload completed in {elapsed_time:.2f} seconds")
        return True
    except botocore.exceptions.NoCredentialsError as e:
        log_error(driver_id, f"AWS credentials not found: {e}")
        return False
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        log_error(driver_id, f"S3 ClientError ({error_code}): {error_msg}")
        return False
    except Exception as e:
        log_error(driver_id, f"S3 upload failed: {type(e).__name__}: {e}")
        return False
# =====================================================
# STREAM STATUS / SIZE CONSTANTS
# =====================================================
# Minimum file size to consider valid (1KB)
MIN_VALID_FILE_SIZE = 1000


# =====================================================
# SHUTDOWN COORDINATION (per worker process)
# =====================================================
class _WorkerStop(Exception):
    """Raised inside a worker when SIGTERM arrives, so the upload loop breaks
    and the finally block (which uploads the full recording) runs."""
    pass


# Tracks the single capture FFmpeg running in THIS worker process, so the
# SIGTERM handler can stop it gracefully.
_current_capture_proc = None
_shutdown_requested = False


def _set_current_capture_proc(proc):
    global _current_capture_proc
    _current_capture_proc = proc


# =====================================================
# SINGLE-CONNECTION CAPTURE  (full recording + chunks)
# =====================================================
# WHY THIS EXISTS
# ---------------
# The old design ran TWO FFmpeg processes per driver:
#   1. a full-session recorder that opened the stream once and never closed it;
#   2. a chunk loop that did open -> record 10s -> CLOSE -> upload -> reopen.
#
# Reopening the stream costs several seconds (measured ~6s on these HLS feeds).
# During that window nobody was listening, so audio existed in the full
# recording but never became a chunk. Measured on real sessions:
#   - TESTDRIVE4 lost ~70s of audio to reopen gaps
#   - TESTDRIVE3 captured ~188s of DUPLICATE audio (reopen landed behind live)
#   - every session lost the final ~10s (SIGTERM landed during a reopen, so
#     there was no partial file to salvage - FFmpeg had only lived 0.4s)
#
# THE FIX: one FFmpeg per driver, opened once, never closed, writing BOTH
# outputs from the same decoded audio:
#
#     one connection
#         |--> full_session.ts    (continuous recording)
#         `--> part-00000.mp3     (10-second pieces, back to back)
#              part-00001.mp3
#              part-00002.mp3
#
# The chunks and the full recording are now cut from the SAME audio, so they
# cannot disagree. No reopen means no gap, no duplication, and the final chunk
# is captured right up to the moment the session stops.
#
# NOTE ON FILENAMES: FFmpeg's segmenter can only produce a counter pattern
# (part-%05d.mp3), not our "segment-00005-00m50s.mp3" name. So FFmpeg writes
# part-*.mp3 locally and Python builds the real S3 key at upload time. The S3
# object name and format are UNCHANGED, so every downstream Lambda, the
# silence remover, Transcribe, OpenSearch and the UI keep working as-is.
def start_capture(driver_id, m3u8_url, driver_folder, start_number=0, run_index=0):
    """
    Start ONE FFmpeg that reads the stream once and writes two outputs:
      - full_session.ts   : continuous recording (robust container)
      - part-%05d.mp3     : back-to-back SEGMENT_DURATION-second chunks

    start_number lets a restarted capture continue the chunk numbering instead
    of overwriting part-00000.mp3.

    Returns (process, ts_path, mp3_path, stderr_log_path, parts_glob).
    """
    driver_dir = os.path.join(OUTPUT_DIR, driver_folder)
    os.makedirs(driver_dir, exist_ok=True)
    # Each capture RUN writes its own full-recording file. A restart must never
    # reuse the same path, because FFmpeg's -y would truncate it and destroy
    # everything recorded so far. finalize_full_recording() stitches runs back
    # together (only needed if the capture ever restarted).
    #
    # We now write MP3 DIRECTLY (not .ts-then-convert). Converting a 73MB hour
    # of .ts to .mp3 took ~60s per driver at shutdown; with 8 drivers converting
    # at once, the two slowest overran the ECS stop timeout and were killed (-9),
    # so their full recording never uploaded. Writing MP3 up front means there is
    # nothing to convert at shutdown - finalize just uploads.
    local_ts_path = os.path.join(driver_dir, f"full_session_{run_index:02d}.mp3")
    local_mp3_path = os.path.join(driver_dir, "full_session.mp3")
    stderr_log_path = os.path.join(driver_dir, "capture_ffmpeg_stderr.log")
    parts_pattern = os.path.join(driver_dir, "part-%05d.mp3")
    parts_glob = os.path.join(driver_dir, "part-*.mp3")

    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "+genpts+igndts",
        # Start at the LIVE edge of the playlist. Without this FFmpeg reads from
        # the start of the playlist and requests segments the CDN already
        # expired -> HTTP 404.
        "-live_start_index", "-1",
        # Auto-reconnect on transient network errors. REQUIRED here: one FFmpeg
        # now holds BOTH the recording and the chunks, so a blip that kills it
        # would cost us both. These streams do throw HTTP 503 in practice.
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", m3u8_url,

        # ---- Output 1: continuous full-session recording ----
        # -map 0:a:0 = FIRST audio track only. NOT "0:a" (all tracks): several of
        # these feeds carry two audio tracks (main + alternate language). MPEG-TS
        # accepts two, but MP3 does not - "Exactly one MP3 audio stream is
        # required" - so the chunk output below fails to open and FFmpeg dies.
        "-map", "0:a:0",
        "-vn",
        "-c:a", "libmp3lame",
        "-b:a", "128k",
        "-f", "mp3",                 # write MP3 directly - no convert at shutdown
        local_ts_path,

        # ---- Output 2: back-to-back chunks, cut by FFmpeg itself ----
        "-map", "0:a:0",
        "-vn",
        "-c:a", "libmp3lame",
        "-b:a", "128k",
        "-f", "segment",
        "-segment_time", str(SEGMENT_DURATION),
        "-segment_format", "mp3",
        "-reset_timestamps", "1",
        "-segment_start_number", str(start_number),
        parts_pattern,
    ]

    log_info(driver_id, f"Starting single-connection capture (run={run_index}, start_number={start_number})")
    log_info(driver_id, f"  -> full recording : {local_ts_path}")
    log_info(driver_id, f"  -> chunks         : {parts_pattern}")
    log_debug(driver_id, f"FFmpeg command: {' '.join(ffmpeg_cmd)}")

    stderr_file = open(stderr_log_path, "a" if run_index else "w")
    proc = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.DEVNULL,
        stderr=stderr_file
    )
    _set_current_capture_proc(proc)
    return proc, local_ts_path, local_mp3_path, stderr_log_path, parts_glob


def _part_index(path):
    """part-00042.mp3 -> 42"""
    m = re.search(r"part-(\d+)\.mp3$", os.path.basename(path))
    return int(m.group(1)) if m else -1


def list_ready_parts(parts_glob, capture_finished):
    """
    Return the chunk files that are FINISHED and safe to upload.

    THE RULE: part-00005.mp3 is finished once part-00006.mp3 exists, because
    FFmpeg only opens the next part after closing the previous one. This is why
    we never upload a half-written chunk.

    Once the capture has stopped, the highest-numbered part is finished too -
    that is the short final chunk containing the real end of the session.
    """
    parts = sorted(glob.glob(parts_glob), key=_part_index)
    if not parts:
        return []
    if capture_finished:
        return parts
    return parts[:-1]          # the newest one is still being written


def finalize_full_recording(driver_id, proc, ts_path, mp3_path, stderr_log_path, driver_folder):
    """
    Convert the .ts recording to .mp3 and upload it.
    The capture process is expected to already be stopped by the caller.
    """
    log_info(driver_id, "===== ENTER finalize_full_recording =====")
    try:
        # --- Step: surface FFmpeg's own error output (why it may have died) ---
        try:
            if os.path.exists(stderr_log_path) and os.path.getsize(stderr_log_path) > 0:
                with open(stderr_log_path, "r", errors="ignore") as sf:
                    tail = sf.read()[-1500:]
                log_info(driver_id, f"FFmpeg stderr (tail): {tail}")
        except Exception as e:
            log_warn(driver_id, f"Could not read FFmpeg stderr log: {e}")

        # --- Step: gather every capture run's MP3 (normally just one) ---
        driver_dir = os.path.dirname(ts_path)
        run_files = sorted(glob.glob(os.path.join(driver_dir, "full_session_*.mp3")))
        run_files = [p for p in run_files if os.path.getsize(p) > MIN_VALID_FILE_SIZE]
        total_bytes = sum(os.path.getsize(p) for p in run_files)
        log_info(driver_id, f"Full-recording runs found: {len(run_files)} totalling {total_bytes} bytes")
        if not run_files:
            log_warn(driver_id, "Full recording missing or too small - not uploaded")
            return

        # --- Step: choose what to upload ---
        # Normal case (one run): the MP3 is already final - NO conversion, so
        # shutdown is fast and we never overrun the ECS stop timeout.
        # Restart case (rare): concat the runs into one MP3.
        if len(run_files) == 1:
            upload_path = run_files[0]
        else:
            log_info(driver_id, f"Stitching {len(run_files)} recording runs...")
            concat_list = os.path.join(driver_dir, "concat.txt")
            with open(concat_list, "w") as cf:
                for p in run_files:
                    cf.write(f"file '{p}'\n")
            conv = subprocess.run(
                ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                 "-f", "concat", "-safe", "0", "-i", concat_list,
                 "-c", "copy", mp3_path],   # stream-copy: no re-encode, fast
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            if conv.returncode != 0:
                log_error(driver_id, f"Stitch failed: {conv.stderr[-800:]}")
                upload_path = run_files[0]   # fall back to the first run
            else:
                upload_path = mp3_path

        up_exists = os.path.exists(upload_path)
        up_size = os.path.getsize(upload_path) if up_exists else 0
        log_info(driver_id, f"Upload file={upload_path} exists={up_exists} size={up_size}")
        if not up_exists or up_size <= MIN_VALID_FILE_SIZE:
            log_warn(driver_id, "Full recording missing/too small - not uploaded")
            return

        # --- Step: upload to S3 ---
        s3_key = f"{FULL_RACE_PREFIX}/{SESSION_ID}/{driver_folder}/full_session.mp3"
        s3 = get_s3_client()
        log_info(driver_id, f"Uploading FULL recording ({up_size/(1024*1024):.2f} MB) -> s3://{S3_BUCKET}/{s3_key}")
        s3.upload_file(upload_path, S3_BUCKET, s3_key)
        log_info(driver_id, f"FULL RECORDING uploaded: {s3_key}")
    except Exception as e:
        log_error(driver_id, f"Failed to finalize full recording: {type(e).__name__}: {e}")


def stop_capture(driver_id, proc):
    """Stop the capture FFmpeg gracefully so BOTH outputs are closed cleanly.
    SIGINT makes FFmpeg flush and finalize; SIGTERM/kill can truncate."""
    if proc is None:
        return
    alive = proc.poll() is None
    log_info(driver_id, f"Stopping capture (alive={alive}, returncode={proc.poll()})")
    if not alive:
        return
    try:
        proc.send_signal(signal.SIGINT)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=25)
    except subprocess.TimeoutExpired:
        log_warn(driver_id, "FFmpeg did not stop in time - killing")
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass


# =====================================================
# DRIVER WORKER - SINGLE-CONNECTION CAPTURE + UPLOADER
# =====================================================
def driver_worker(driver_id):
    """
    One FFmpeg captures the stream continuously and writes both the full
    recording and the chunks. This function no longer records anything - it
    only watches the folder and uploads finished chunks to S3 as they appear.

    Because recording and uploading are now separate, a slow upload can never
    interrupt listening.
    """
    worker_start_time = time.time()
    log_info(driver_id, "=" * 50)
    log_info(driver_id, "WORKER STARTED - SINGLE-CONNECTION CAPTURE")
    log_info(driver_id, "=" * 50)

    def _worker_sigterm(_signum, _frame):
        # Mark shutdown. The upload loop checks this flag, stops the capture
        # gracefully, uploads whatever chunks remain (including the short final
        # one), then finalizes the full recording.
        global _shutdown_requested
        _shutdown_requested = True
    signal.signal(signal.SIGTERM, _worker_sigterm)

    capture_proc = None
    full_ts = None
    full_mp3 = None
    capture_stderr = None
    driver_folder = None

    try:
        config = DRIVER_CONFIG[driver_id]
        m3u8_url = config["m3u8_url"]
        driver_name = config.get("driver_name", f"driver{driver_id:02d}")
        team = config.get("team", "UNKNOWN")
        driver_folder = sanitize_for_path(driver_name)

        log_info(driver_id, "Configuration loaded")
        log_info(driver_id, f"Driver Name: {driver_name}")
        log_info(driver_id, f"Driver Folder: {driver_folder}")
        log_info(driver_id, f"Team: {team}")
        log_info(driver_id, f"M3U8 URL: {m3u8_url}")
        log_info(driver_id, f"S3 Bucket: {S3_BUCKET}")
        log_info(driver_id, f"Segment Duration: {SEGMENT_DURATION} seconds")

        driver_output_dir = os.path.join(OUTPUT_DIR, driver_folder)
        os.makedirs(driver_output_dir, exist_ok=True)
        log_info(driver_id, f"Output directory: {driver_output_dir}")

        # Clear any stale parts from a previous run in this container.
        for stale in glob.glob(os.path.join(driver_output_dir, "part-*.mp3")):
            try:
                os.remove(stale)
            except Exception:
                pass

        # Upload driver metadata once, up front. The old code did this alongside
        # the first chunk; doing it here means it lands before any chunk does.
        # UNCHANGED key and contents - downstream depends on this file.
        metadata = {
            "session_id": SESSION_ID,
            "driver_name": driver_name,      # Keep original name for display
            "driver_folder": driver_folder,  # Sanitized folder name
            "team": team
        }
        metadata_key = f"{S3_PREFIX}/{SESSION_ID}/{driver_folder}/metadata.json"
        try:
            s3 = get_s3_client()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=metadata_key,
                Body=json.dumps(metadata),
                ContentType="application/json"
            )
            log_info(driver_id, f"Uploaded metadata: {metadata_key}")
        except Exception as e:
            log_warn(driver_id, f"Failed to upload metadata: {e}")

        # ---- Start the single capture ----
        session_clock_start = time.time()
        capture_proc, full_ts, full_mp3, capture_stderr, parts_glob = start_capture(
            driver_id, m3u8_url, driver_folder, start_number=0, run_index=0)
        # Real elapsed seconds at the moment this capture run began. Chunk N of
        # this run therefore starts at capture_offset + N * SEGMENT_DURATION.
        # Because FFmpeg never closes the stream, that arithmetic is now TRUE -
        # it was a lie in the old design, which is what caused the timestamp
        # drift (+82s on TESTDRIVE5) and the driver skew in the UI.
        capture_offset = 0
        next_part_to_upload = 0
        uploaded_count = 0
        restarts = 0
        MAX_RESTARTS = int(os.getenv("MAX_CAPTURE_RESTARTS", "20"))

        while True:
            capture_alive = capture_proc.poll() is None

            # ---- Shutdown requested: stop capture, then drain everything ----
            if _shutdown_requested:
                log_info(driver_id, "Shutdown signal received - stopping capture and draining chunks...")
                stop_capture(driver_id, capture_proc)
                capture_alive = False

            # ---- Capture died unexpectedly: restart it, keep numbering ----
            if not capture_alive and not _shutdown_requested:
                # Drain what exists before restarting, so nothing is lost.
                for path in list_ready_parts(parts_glob, capture_finished=True):
                    idx = _part_index(path)
                    if idx < next_part_to_upload:
                        continue
                    if _upload_part(driver_id, path, idx, capture_offset,
                                    session_clock_start, driver_folder):
                        uploaded_count += 1
                    next_part_to_upload = idx + 1

                if restarts >= MAX_RESTARTS:
                    log_error(driver_id, f"Capture died and restart limit ({MAX_RESTARTS}) reached - stopping")
                    break
                restarts += 1
                log_warn(driver_id, f"Capture process exited (rc={capture_proc.poll()}) - restarting ({restarts}/{MAX_RESTARTS})")
                # Back off progressively (2s, 4s, 8s ... capped at 30s) so a
                # broken stream cannot burn the whole restart budget in seconds.
                time.sleep(min(2 * (2 ** min(restarts - 1, 4)), 30))
                capture_offset = int(time.time() - session_clock_start)
                capture_proc, full_ts, full_mp3, capture_stderr, parts_glob = start_capture(
                    driver_id, m3u8_url, driver_folder,
                    start_number=next_part_to_upload, run_index=restarts)
                continue

            # ---- Upload every finished chunk ----
            ready = list_ready_parts(parts_glob, capture_finished=not capture_alive)
            for path in ready:
                idx = _part_index(path)
                if idx < next_part_to_upload:
                    continue
                if _upload_part(driver_id, path, idx, capture_offset,
                                session_clock_start, driver_folder):
                    uploaded_count += 1
                next_part_to_upload = idx + 1

            # ---- Capture is stopped and everything is drained: done ----
            if not capture_alive:
                log_info(driver_id, f"Capture stopped and all chunks drained ({uploaded_count} uploaded)")
                break

            if not CONTINUOUS_MODE and uploaded_count >= 1:
                log_info(driver_id, "Single segment mode - exiting")
                stop_capture(driver_id, capture_proc)
                break

            time.sleep(1)

        log_info(driver_id, f"Total chunks uploaded: {uploaded_count}")

    except _WorkerStop:
        log_info(driver_id, "Shutdown signal received - finalizing full recording...")
    except KeyError:
        log_error(driver_id, f"Driver {driver_id} not found in DRIVER_CONFIG")
        raise
    except Exception as e:
        worker_elapsed_time = time.time() - worker_start_time
        log_error(driver_id, "=" * 50)
        log_error(driver_id, "WORKER FAILED")
        log_error(driver_id, f"Error type: {type(e).__name__}")
        log_error(driver_id, f"Error message: {e}")
        log_error(driver_id, f"Time before failure: {worker_elapsed_time:.2f} seconds")
        log_error(driver_id, "=" * 50)
        raise
    finally:
        # Make sure the capture is stopped, then upload the full recording.
        try:
            stop_capture(driver_id, capture_proc)
        except Exception:
            pass
        if ENABLE_FULL_RECORDING and capture_proc is not None and driver_folder is not None:
            finalize_full_recording(driver_id, capture_proc, full_ts, full_mp3,
                                    capture_stderr, driver_folder)


def _upload_part(driver_id, path, idx, capture_offset, session_clock_start, driver_folder):
    """
    Upload one finished chunk under the SAME S3 name the pipeline already
    expects: segment-00005-00m50s.mp3

    The timestamp is capture_offset + idx * SEGMENT_DURATION. This is exact
    because FFmpeg cuts back-to-back chunks from an unbroken stream, so chunk
    idx really does start at idx * SEGMENT_DURATION into this capture run.
    """
    try:
        size = os.path.getsize(path)
    except OSError:
        return False

    if size <= MIN_VALID_FILE_SIZE:
        log_warn(driver_id, f"Chunk {idx:05d} too small ({size} bytes) - skipped")
        try:
            os.remove(path)
        except Exception:
            pass
        return False

    total_seconds = capture_offset + idx * SEGMENT_DURATION
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    filename = f"segment-{idx:05d}-{minutes:02d}m{seconds:02d}s.mp3"
    s3_key = f"{S3_PREFIX}/{SESSION_ID}/{driver_folder}/{filename}"

    log_info(driver_id, f"Chunk {idx:05d} ready ({size / 1024:.2f} KB) -> {filename}")
    ok = upload_segment_to_s3(driver_id, path, s3_key)
    if ok:
        log_info(driver_id, f"SUCCESS: Segment {idx:05d} uploaded to S3")
    else:
        log_error(driver_id, f"FAILED: Could not upload segment {idx:05d}")

    # Delete after upload so the container disk cannot fill during a long race.
    try:
        os.remove(path)
    except Exception as e:
        log_warn(driver_id, f"Could not delete {path}: {e}")
    return ok

# =====================================================
# GRACEFUL SHUTDOWN HANDLER
# =====================================================
shutdown_flag = multiprocessing.Event()
def signal_handler(signum, _frame):
    log_system(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag.set()
    # Update session status to STOPPED when ECS sends SIGTERM
    update_session_status(SESSION_ID, 'STOPPED')
    log_system(f"[SESSION_STATUS] Marked session {SESSION_ID} as STOPPED due to signal {signum}")
# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    main_start_time = time.time()
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    log_system("=" * 60)
    log_system("LIVE STREAMING AUDIO INGESTER STARTED")
    log_system("=" * 60)
    log_system(f"Session ID: {SESSION_ID}")
    log_system(f"Use DynamoDB Config: {USE_DYNAMODB_CONFIG}")
    log_system(f"Total drivers to process: {len(DRIVER_CONFIG)}")
    log_system(f"Driver IDs: {list(DRIVER_CONFIG.keys())}")
    log_system(f"AWS Region: {AWS_REGION}")
    log_system(f"S3 Bucket: {S3_BUCKET}")
    log_system(f"S3 Prefix: {S3_PREFIX}")
    log_system(f"Segment Duration: {SEGMENT_DURATION} seconds")
    log_system(f"Continuous Mode: {CONTINUOUS_MODE}")
    log_system(f"Output Directory: {OUTPUT_DIR}")
    # Check if there's a scheduled session (optional - can be disabled)
    WAIT_FOR_SESSION = os.environ.get("WAIT_FOR_SESSION", "false").lower() == "true"
    if WAIT_FOR_SESSION:
        log_system("-" * 60)
        log_system("Waiting for scheduled session to start...")
        wait_for_session_start()
        log_system("-" * 60)
    # Mark session as ACTIVE when ECS starts
    update_session_status(SESSION_ID, 'ACTIVE')
    processes = []
    log_system("-" * 60)
    log_system("Spawning worker processes...")
    log_system("-" * 60)
    for driver_id in DRIVER_CONFIG.keys():
        log_system(f"Starting process for Driver {driver_id:02d}...")
        p = multiprocessing.Process(target=driver_worker, args=(driver_id,))
        p.start()
        log_system(f"Process started for Driver {driver_id:02d} (PID: {p.pid})")
        processes.append(p)
    log_system(f"All {len(processes)} worker processes started")
    log_system("-" * 60)
    log_system("Workers running continuously. Press Ctrl+C to stop.")
    log_system("-" * 60)
    def _graceful_stop_workers():
        # Ask each worker to stop (SIGTERM -> _WorkerStop in the worker), then
        # give them time to finalize and upload the full recording before the
        # container is killed. Full-recording finalize can take ~20s, so wait
        # generously (ECS default stopTimeout is 30s; raise it if needed).
        log_system("Stopping workers gracefully (finalizing full recordings)...")
        for p in processes:
            p.terminate()  # delivers SIGTERM -> worker raises _WorkerStop
        for p in processes:
            p.join(timeout=25)
            if p.is_alive():
                log_system(f"Worker PID {p.pid} still finalizing; forcing stop")
                p.kill()
    try:
        # Wait for all processes (they run forever in continuous mode).
        # If SIGTERM set the shutdown flag, stop workers gracefully.
        while any(p.is_alive() for p in processes):
            if shutdown_flag.is_set():
                _graceful_stop_workers()
                break
            time.sleep(1)
        for p in processes:
            p.join(timeout=5)
            log_system(f"Process PID {p.pid} completed with exit code: {p.exitcode}")
    except KeyboardInterrupt:
        log_system("Keyboard interrupt received, stopping workers...")
        _graceful_stop_workers()
    # Mark session as STOPPED when ECS stops
    update_session_status(SESSION_ID, 'STOPPED')
    main_elapsed_time = time.time() - main_start_time
    log_system("=" * 60)
    log_system("ALL DRIVERS STOPPED")
    log_system(f"Total execution time: {main_elapsed_time:.2f} seconds")
    log_system("=" * 60)