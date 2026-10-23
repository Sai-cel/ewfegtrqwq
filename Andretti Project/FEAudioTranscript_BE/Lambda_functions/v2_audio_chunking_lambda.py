########## 16/07/2026 #########
"""
Audio Chunking Lambda
Splits large audio/video files into smaller chunks for faster processing

Trigger: S3 event when file is uploaded to uploads/ prefix
Output: Multiple chunks to raw-chunks/ (triggers existing pipeline)

This Lambda is specifically for uploaded recordings (not live streams).
Live streams are already chunked by the ECS ingester.

IMPORTANT: Chunk filenames include "_recorded" suffix to distinguish
uploaded recordings from live streams. This is used by opensearch_dynamo_db_lambda
to skip WebSocket push for recorded files (only live sessions should push).

Chunk format: segment-00000-00m00s_recorded.mp3

Author: People Tech Group
Created: December 30, 2024
Updated: January 2025
"""

import os
import json
import uuid
import boto3
import subprocess
import math
from urllib.parse import unquote_plus
from datetime import datetime

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

INPUT_PREFIX = "uploads/"

OUTPUT_PREFIX = "raw-chunks/"

LOG_PREFIX = "logs/chunking/"

FFMPEG = "/opt/bin/ffmpeg"
FFPROBE = "/opt/bin/ffprobe"

CHUNK_DURATION = int(os.environ.get("CHUNK_DURATION", "30"))

SUPPORTED_EXTENSIONS = {'.mp3', '.mp4', '.m4a', '.wav', '.flac', '.ogg', '.webm'}


def lambda_handler(event, context):
    """
    Main handler - splits uploaded file into chunks
    """
    print("EVENT:", json.dumps(event))

    if "Records" not in event:
        raise ValueError("This Lambda must be triggered by S3")

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])

    file_ext = os.path.splitext(key.lower())[1]
    if file_ext not in SUPPORTED_EXTENSIONS:
        print(f"Skipping unsupported format: {key}")
        return {"status": "ignored", "reason": f"Unsupported format: {file_ext}"}

    key_parts = key.replace(INPUT_PREFIX, "").split("/")

    if len(key_parts) >= 3:
        session_id = key_parts[0]
        driver_folder = key_parts[1]
        original_filename = key_parts[2]
    elif len(key_parts) >= 2:
        session_id = key_parts[0]
        driver_folder = "default"
        original_filename = key_parts[1]
    else:
        session_id = datetime.utcnow().strftime('%Y-%m-%d_%H-%M')
        driver_folder = "default"
        original_filename = key_parts[0] if key_parts else "unknown"

    base_name = os.path.splitext(original_filename)[0]

    local_input = f"/tmp/input{file_ext}"

    print(f"Processing: s3://{bucket}/{key}")
    print(f"Session: {session_id}, Driver: {driver_folder}")

    driver_name = driver_folder
    team = "UPLOADED"

    record_audit_event('CHUNKING_START', 'pipeline', session_id, 'system', {
        'driver_folder': driver_folder,
        'original_file': key
    })

    try:
        upload_metadata_key = f"{INPUT_PREFIX}{session_id}/{driver_folder}/upload_metadata.json"
        print(f"Looking for upload metadata: {upload_metadata_key}")
        metadata_obj = s3.get_object(Bucket=bucket, Key=upload_metadata_key)
        upload_metadata = json.loads(metadata_obj["Body"].read().decode("utf-8"))
        driver_name = upload_metadata.get("driver_name", driver_folder)
        team = upload_metadata.get("team", "UPLOADED")
        print(f"Found upload metadata - Driver: {driver_name}, Team: {team}")
    except Exception as e:
        print(f"Could not read upload_metadata.json (using defaults): {e}")

    try:
        s3.download_file(bucket, key, local_input)

        duration = get_duration(local_input)
        print(f"File duration: {duration} seconds")

        if duration <= 0:
            print("Could not determine file duration")
            record_audit_event('CHUNKING_FAILED', 'pipeline', session_id, 'system', {
                'driver_folder': driver_folder,
                'original_file': key,
                'reason': 'Could not determine duration'
            })
            return {"status": "error", "reason": "Could not determine duration"}

        num_chunks = math.ceil(duration / CHUNK_DURATION)
        print(f"Splitting into {num_chunks} chunks of {CHUNK_DURATION}s each")

        chunks_created = []

        metadata = {
            "session_id": session_id,
            "driver_name": driver_name,
            "team": team
        }

        metadata_key = f"{OUTPUT_PREFIX}{session_id}/{driver_folder}/metadata.json"
        print(f"Creating metadata BEFORE chunks: {metadata_key}")
        print(f"Metadata content: {metadata}")
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType="application/json"
        )

        for i in range(num_chunks):
            start_time = i * CHUNK_DURATION
            # Format: segment-00000-00m00s_recorded.mp3
            # Include "_recorded" suffix to identify uploaded recordings vs live streams
            # This is used by opensearch_dynamo_db_lambda to skip WebSocket push for recordings

            # ----------------------------------------------------
            # TIMESTAMP OFFSET LOGIC
            # Starts filenames from -02:00
            #
            # Examples:
            # 0s   -> -02m00s
            # 10s  -> -01m50s
            # 20s  -> -01m40s
            # ...
            # 120s -> 00m00s
            # 130s -> 00m10s
            # ----------------------------------------------------

            OFFSET_SECONDS = 120  # 2 minutes

            display_time = start_time - OFFSET_SECONDS

            # Handle negative timestamps
            is_negative = display_time < 0

            abs_time = abs(display_time)

            minutes = int(abs_time // 60)
            seconds = int(abs_time % 60)

            # Add "-" prefix only for negative values
            time_prefix = "-" if is_negative else ""

            chunk_filename = (
                f"segment-{i:05d}-"
                f"{time_prefix}{minutes:02d}m{seconds:02d}s_recorded.mp3"
            )

            local_chunk = f"/tmp/{chunk_filename}"

            cmd = [
                FFMPEG, "-hide_banner", "-y",
                "-ss", str(start_time),
                "-i", local_input,
                "-t", str(CHUNK_DURATION),
                "-c:a", "libmp3lame",
                "-b:a", "128k",
                "-ar", "16000",
                local_chunk
            ]

            try:
                subprocess.run(cmd, check=True, capture_output=True)

                chunk_key = f"{OUTPUT_PREFIX}{session_id}/{driver_folder}/{chunk_filename}"

                print(f"Uploading chunk {i+1}/{num_chunks}: {chunk_key}")
                s3.upload_file(local_chunk, S3_BUCKET, chunk_key)

                chunks_created.append({
                    "chunk_index": i,
                    "start_time": start_time,
                    "s3_key": chunk_key
                })

                os.remove(local_chunk)

            except subprocess.CalledProcessError as e:
                print(f"Error creating chunk {i}: {e.stderr.decode() if e.stderr else str(e)}")
                continue

        if os.path.exists(local_input):
            os.remove(local_input)


        log_data = {
            "original_file": key,
            "session_id": session_id,
            "driver_folder": driver_folder,
            "duration": duration,
            "chunk_duration": CHUNK_DURATION,
            "num_chunks": len(chunks_created),
            "chunks": chunks_created,
            "processed_at": datetime.utcnow().isoformat()
        }

        log_key = f"{LOG_PREFIX}{session_id}/{driver_folder}/{base_name}_chunking.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=log_key,
            Body=json.dumps(log_data, indent=2),
            ContentType="application/json"
        )

        print(f"Chunking complete: {len(chunks_created)} chunks created")

        record_audit_event('CHUNKING_SUCCESS', 'pipeline', session_id, 'system', {
            'driver_folder': driver_folder,
            'original_file': key,
            'num_chunks': len(chunks_created),
            'duration': duration
        })

        return {
            "status": "SUCCESS",
            "session_id": session_id,
            "driver_folder": driver_folder,
            "original_duration": duration,
            "chunks_created": len(chunks_created),
            "log_path": f"s3://{S3_BUCKET}/{log_key}"
        }

    except Exception as e:
        print(f"Chunking failed: {type(e).__name__}: {e}")
        record_audit_event('CHUNKING_FAILED', 'pipeline', session_id, 'system', {
            'driver_folder': driver_folder,
            'original_file': key,
            'error': f"{type(e).__name__}: {e}"
        })
        raise


def get_duration(path):
    """Get audio/video duration using ffprobe"""
    try:
        result = subprocess.check_output([
            FFPROBE, "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path
        ], text=True)
        return float(result.strip())
    except Exception as e:
        print(f"Error getting duration: {e}")
        return 0.0



