######### 23/07/2026 ##########
"""
Silence Removal Lambda
Removes silence from audio files using FFmpeg

Trigger: S3 event when MP3 is uploaded to chunked_mp3_output/
Output: Silence-removed MP3 to silence-removed-mp3-chunks/

Author: People Tech Group
Updated: December 29, 2024
"""

import os
import json
import re
import uuid
import boto3
import subprocess
from datetime import datetime
from urllib.parse import unquote_plus

s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-west-2"))

S3_BUCKET = os.environ.get("S3_BUCKET", "test-v2-andretti-transcription-system")

AUDIT_S3_BUCKET = os.environ.get('AUDIT_S3_BUCKET', 'test-v2-andretti-audit-logs')
AUDIT_S3_PREFIX = os.environ.get('AUDIT_S3_PREFIX', 'audit-logs/')
SESSION_LOGS_PREFIX = os.environ.get('SESSION_LOGS_PREFIX', 'session_logs/')
SYSTEM_LOGS_PREFIX = os.environ.get('SYSTEM_LOGS_PREFIX', 'system_logs/')
SESSION_SCOPED_ENTITIES = {'pipeline', 'session', 'session_config', 'recording'}


def record_audit_event(action, entity, entity_id, created_by, details=None):
    """Write an audit-trail record to S3 in three places: a date partition
    (unchanged), session_logs/{session_id}/ for session-scoped entities (this
    Lambda's events are all entity='pipeline', so always here), or
    system_logs/YYYY/MM/DD/ otherwise. No-op if AUDIT_S3_BUCKET is unset;
    never raises."""
    if not AUDIT_S3_BUCKET:
        return
    try:
        now = datetime.utcnow()
        record = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': now.isoformat() + 'Z',
            'action': action,
            'entity': entity,
            'entity_id': str(entity_id) if entity_id is not None else '',
            'created_by': created_by or 'unknown',
            'details': details or {}
        }
        suffix = f"{action}_{int(now.timestamp())}_{record['audit_id'][:8]}.json"
        body = json.dumps(record, default=str).encode('utf-8')
        date_path = now.strftime('%Y/%m/%d')
        s3.put_object(
            Bucket=AUDIT_S3_BUCKET,
            Key=f"{AUDIT_S3_PREFIX}{date_path}/{suffix}",
            Body=body,
            ContentType='application/json'
        )
        if entity in SESSION_SCOPED_ENTITIES and record['entity_id']:
            s3.put_object(
                Bucket=AUDIT_S3_BUCKET,
                Key=f"{SESSION_LOGS_PREFIX}{record['entity_id']}/{suffix}",
                Body=body,
                ContentType='application/json'
            )
        else:
            s3.put_object(
                Bucket=AUDIT_S3_BUCKET,
                Key=f"{SYSTEM_LOGS_PREFIX}{date_path}/{suffix}",
                Body=body,
                ContentType='application/json'
            )
    except Exception as e:
        print(f"[AUDIT] Failed to record {action} for {entity}:{entity_id}: {e}")

INPUT_PREFIX = "raw-chunks/"

OUTPUT_PREFIX = "silence-removed/"

LOG_PREFIX = "logs/silence-removal/"

FFMPEG = "/opt/bin/ffmpeg"
FFPROBE = "/opt/bin/ffprobe"

SILENCE_THRESHOLD = "-45dB"
SILENCE_DURATION = "0.3"

SILENCE_START = re.compile(r"silence_start: ([\d\.]+)")
SILENCE_END = re.compile(r"silence_end: ([\d\.]+) \| silence_duration: ([\d\.]+)")


def lambda_handler(event, context):

    print("EVENT:", event)

    if "Records" not in event:
        raise ValueError("This Lambda must be triggered by S3 - not Test button.")

    record = event["Records"][0]
    input_bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])

    if not key.lower().endswith(".mp3"):
        print("Skipping non-mp3 file.")
        return {"status": "ignored", "file": key}

    base = os.path.splitext(os.path.basename(key))[0]

    key_parts = key.replace(INPUT_PREFIX, "").split("/")
    session_id = key_parts[0] if len(key_parts) >= 1 else ""
    driver_folder = key_parts[1] if len(key_parts) >= 2 else ""

    local_in = f"/tmp/{base}.mp3"
    local_out = f"/tmp/{base}_cleaned.mp3"
    local_json = f"/tmp/{base}.json"

    record_audit_event('SILENCE_REMOVAL_START', 'pipeline', session_id, 'system', {
        'chunk_id': base,
        'driver_folder': driver_folder
    })

    try:
        print(f"Downloading: s3://{input_bucket}/{key}")
        s3.download_file(input_bucket, key, local_in)

        detect_cmd = [
            FFMPEG, "-hide_banner", "-nostats",
            "-i", local_in,
            "-af", f"silencedetect=n={SILENCE_THRESHOLD}:d={SILENCE_DURATION}",
            "-f", "null", "-"
        ]

        proc = subprocess.run(detect_cmd, capture_output=True, text=True)
        ffmpeg_log = proc.stderr

        events, total_silence, total_audio = parse_ffmpeg(ffmpeg_log)
        duration = get_duration(local_in)

        # How much silence was trimmed from the FRONT of this chunk.
        # This is the piece that lets us report the REAL moment a driver spoke.
        # Without it every message lands exactly on the 10s chunk boundary
        # (00:25, 00:35 ...) because silenceremove deletes the leading gap and
        # Transcribe then always reports speech starting at ~0.0s.
        leading_silence = get_leading_silence(events)
        print(f"Leading silence trimmed: {leading_silence:.3f}s")

        print("Detected Audio Duration:", total_audio)

        print("Running silence removal...")
        remove_cmd = [
            FFMPEG, "-hide_banner", "-y",
            "-i", local_in,
            "-af",
            (
                f"silenceremove=start_periods=1:"
                f"start_silence={SILENCE_DURATION}:start_threshold={SILENCE_THRESHOLD}:"
                f"stop_periods=-1:stop_silence={SILENCE_DURATION}:stop_threshold={SILENCE_THRESHOLD}"
            ),
            "-c:a", "libmp3lame", "-b:a", "128k",
            local_out
        ]

        subprocess.run(remove_cmd, check=True)

        if session_id and driver_folder:
            cleaned_key = f"{OUTPUT_PREFIX}{session_id}/{driver_folder}/{base}.mp3"
        else:
            cleaned_key = f"{OUTPUT_PREFIX}{base}.mp3"

        print(f"Uploading cleaned file: s3://{S3_BUCKET}/{cleaned_key}")
        # Stamp the trimmed leading silence onto the object itself. The
        # transcription lambda reads this to reconstruct the real speech time.
        s3.upload_file(
            local_out, S3_BUCKET, cleaned_key,
            ExtraArgs={"Metadata": {"leading-silence": str(round(leading_silence, 3))}}
        )

        json_data = {
            "chunk_id": base,
            "session_id": session_id,
            "driver_folder": driver_folder,
            "duration": duration,
            "silence_removed": round(total_silence, 3),
            "leading_silence": round(leading_silence, 3),
            "audio_detected": round(total_audio, 3),
            "events": events
        }

        with open(local_json, "w") as f:
            json.dump(json_data, f, indent=2)

        if session_id and driver_folder:
            log_key = f"{LOG_PREFIX}{session_id}/{driver_folder}/{base}.json"
        else:
            log_key = f"{LOG_PREFIX}{base}.json"

        print(f"Uploading log: s3://{S3_BUCKET}/{log_key}")
        s3.upload_file(local_json, S3_BUCKET, log_key)

        for temp_file in [local_in, local_out, local_json]:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                print(f"Warning: Could not delete {temp_file}: {e}")

        print(f"Silence removal completed for {base} (session: {session_id}, driver: {driver_folder})")

        record_audit_event('SILENCE_REMOVAL_SUCCESS', 'pipeline', session_id, 'system', {
            'chunk_id': base,
            'driver_folder': driver_folder,
            'silence_removed': round(total_silence, 3),
            'output_key': cleaned_key
        })

        return {
            "status": "SUCCESS",
            "session_id": session_id,
            "driver_folder": driver_folder,
            "clean_audio_s3_path": f"s3://{S3_BUCKET}/{cleaned_key}",
            "log_path": f"s3://{S3_BUCKET}/{log_key}"
        }

    except Exception as e:
        print(f"Silence removal failed: {type(e).__name__}: {e}")
        record_audit_event('SILENCE_REMOVAL_FAILED', 'pipeline', session_id, 'system', {
            'chunk_id': base,
            'driver_folder': driver_folder,
            'error': f"{type(e).__name__}: {e}"
        })
        raise


def parse_ffmpeg(log):
    events = []
    last_end = 0.0
    total_silence = 0.0
    total_audio = 0.0

    for line in log.splitlines():
        s = SILENCE_START.search(line)
        e = SILENCE_END.search(line)

        if s:
            start = float(s.group(1))
            audio = max(0, start - last_end)
            events.append({"type": "AUDIO", "start": last_end, "end": start, "duration": audio})
            total_audio += audio

        if e:
            end = float(e.group(1))
            dur = float(e.group(2))
            start = end - dur
            events.append({"type": "SILENCE", "start": start, "end": end, "duration": dur})
            total_silence += dur
            last_end = end

    return events, total_silence, total_audio


def get_leading_silence(events):
    """Seconds of silence removed from the START of the chunk.

    silencedetect reports a SILENCE event beginning at (or within a hair of)
    0.0 when the chunk opens with a gap. That gap is what silenceremove strips,
    so it is exactly the amount every later timestamp must be shifted by.
    Returns 0.0 when the chunk starts with speech.
    """
    for ev in events:
        if ev.get("type") == "SILENCE":
            start = float(ev.get("start", 0) or 0)
            if start <= 0.05:                 # tolerance: silence at the very start
                return float(ev.get("duration", 0) or 0)
            return 0.0                        # first silence is later => starts with speech
        if ev.get("type") == "AUDIO" and float(ev.get("duration", 0) or 0) > 0:
            return 0.0                        # audio first => no leading silence
    return 0.0


def get_duration(path):
    try:
        out = subprocess.check_output(
            [FFPROBE, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
        text=True)
        return float(out.strip())
    except:
        return 0.0