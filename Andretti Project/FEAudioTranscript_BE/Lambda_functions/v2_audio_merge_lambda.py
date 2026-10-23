#################### 16/07/2026 ##############
"""
Audio Merge Lambda
Merges all audio chunks for a session into a single full audio file per driver

Trigger:
- EventBridge rule when session status changes to STOPPED
- Or direct API call: POST /sessions/{session_id}/merge-audio

Output: Full session audio to full-sessions/{session_id}/{driver_id}/full_audio.mp3

Author: People Tech Group
Created: January 2025
"""

import os
import json
import uuid
import boto3
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


S3_BUCKET = os.environ.get("S3_BUCKET", "test-v2-andretti-transcription-system")

INPUT_PREFIX = "silence-removed/"

OUTPUT_PREFIX = "full-sessions/"
UPLOAD_OUTPUT_PREFIX = "merged-audio/"  # NEW: dedicated prefix for upload-flow merged audio, separate from live

FFMPEG = "/opt/bin/ffmpeg"

BATCH_SIZE = 5

s3_client = boto3.client('s3', region_name='us-west-2')
dynamodb = boto3.resource('dynamodb')

ACTIVE_SESSIONS_TABLE = os.environ.get("ACTIVE_SESSIONS_TABLE", "v2_ActiveSessions")
DRIVERS_TABLE = os.environ.get("DRIVERS_TABLE", "v2_DriversConfig")

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
        s3_client.put_object(
            Bucket=AUDIT_S3_BUCKET,
            Key=f"{AUDIT_S3_PREFIX}{date_path}/{suffix}",
            Body=body,
            ContentType='application/json'
        )
        if entity in SESSION_SCOPED_ENTITIES and record['entity_id']:
            s3_client.put_object(
                Bucket=AUDIT_S3_BUCKET,
                Key=f"{SESSION_LOGS_PREFIX}{record['entity_id']}/{suffix}",
                Body=body,
                ContentType='application/json'
            )
        else:
            s3_client.put_object(
                Bucket=AUDIT_S3_BUCKET,
                Key=f"{SYSTEM_LOGS_PREFIX}{date_path}/{suffix}",
                Body=body,
                ContentType='application/json'
            )
    except Exception as e:
        print(f"[AUDIT] Failed to record {action} for {entity}:{entity_id}: {e}")



def lambda_handler(event, context):
    """
    Main handler for audio merge requests

    Supports two trigger types:
    1. EventBridge: {"detail": {"session_id": "...", "status": "STOPPED"}}
    2. API Gateway: {"pathParameters": {"session_id": "..."}, "body": {"driver_ids": [...]}}
    """
    print("=" * 60)
    print("AUDIO MERGE LAMBDA STARTED")
    print("=" * 60)
    print(f"Event: {json.dumps(event, indent=2)}")

    try:
        if "detail" in event:
            return handle_eventbridge_trigger(event)
        elif "pathParameters" in event:
            return handle_api_trigger(event)
        else:
            session_id = event.get("session_id")
            driver_ids = event.get("driver_ids")

            if not session_id:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "session_id is required"})
                }

            # NEW: uploads pass "flow": "upload" explicitly. Live's payload never
            # includes this field, so live always falls through unchanged below.
            if event.get("flow") == "upload":
                return merge_session_audio_upload(session_id, driver_ids)
            return merge_session_audio(session_id, driver_ids)

    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def handle_eventbridge_trigger(event):
    """Handle EventBridge trigger when session status changes to STOPPED"""
    detail = event.get("detail", {})
    session_id = detail.get("session_id")
    status = detail.get("status")

    print(f"[EVENTBRIDGE] Session: {session_id}, Status: {status}")

    if status != "STOPPED":
        print(f"[EVENTBRIDGE] Ignoring non-STOPPED status: {status}")
        return {"statusCode": 200, "body": "Ignored - not a STOPPED event"}

    if not session_id:
        return {"statusCode": 400, "body": json.dumps({"error": "session_id missing in event detail"})}

    return merge_session_audio(session_id)


def handle_api_trigger(event):
    """Handle API Gateway trigger for manual merge request"""
    path_params = event.get("pathParameters", {})
    session_id = path_params.get("session_id")

    if not session_id:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "session_id is required"})
        }

    driver_ids = None
    if event.get("body"):
        try:
            body = json.loads(event["body"])
            driver_ids = body.get("driver_ids")
        except json.JSONDecodeError:
            pass

    result = merge_session_audio(session_id, driver_ids)

    return {
        "statusCode": result.get("statusCode", 200),
        "headers": {"Content-Type": "application/json"},
        "body": result.get("body", json.dumps(result))
    }



def get_session_name(session_id):
    """
    Get the session name from ActiveSessions table
    Returns session_name or session_id as fallback
    """
    try:
        table = dynamodb.Table(ACTIVE_SESSIONS_TABLE)
        result = table.get_item(Key={'SessionId': session_id})
        if 'Item' in result:
            return result['Item'].get('session_name', session_id)
        return session_id
    except Exception as e:
        print(f"[HELPER] Error getting session name: {e}")
        return session_id


def get_driver_name(driver_id):
    """
    Get the driver name from DriversConfig table
    Returns driver_name or driver_id as fallback
    """
    try:
        table = dynamodb.Table(DRIVERS_TABLE)
        result = table.get_item(Key={'driver_id': driver_id})
        if 'Item' in result:
            return result['Item'].get('driver_name', driver_id)
        return driver_id
    except Exception as e:
        print(f"[HELPER] Error getting driver name: {e}")
        return driver_id


def sanitize_filename(name):
    """
    Sanitize a string for use in filename
    Replaces spaces with underscores and removes special characters
    """
    name = name.replace(' ', '_')
    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for char in invalid_chars:
        name = name.replace(char, '')
    return name



def merge_session_audio(session_id, driver_ids=None):
    """
    Merge all audio chunks for a session into full audio files using batch processing

    Args:
        session_id: Session ID to merge
        driver_ids: Optional list of specific driver IDs to merge (None = all drivers)

    Returns:
        Result dict with merge status per driver
    """
    print(f"[MERGE] Starting merge for session: {session_id}")

    record_audit_event('AUDIO_MERGE_START', 'pipeline', session_id, 'system', {
        'requested_driver_ids': driver_ids
    })

    try:
        session_name = get_session_name(session_id)
        print(f"[MERGE] Session name: {session_name}")

        if driver_ids:
            drivers_to_merge = driver_ids
        else:
            drivers_to_merge = get_drivers_in_session(session_id)

        if not drivers_to_merge:
            print(f"[MERGE] No drivers found for session {session_id}")
            record_audit_event('AUDIO_MERGE_SUCCESS', 'pipeline', session_id, 'system', {
                'total_drivers': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'reason': 'No drivers found'
            })
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No drivers found for this session",
                    "session_id": session_id,
                    "merged": []
                })
            }

        print(f"[MERGE] Found {len(drivers_to_merge)} drivers to merge: {drivers_to_merge}")
        print(f"[MERGE] Using batch processing with BATCH_SIZE={BATCH_SIZE}")

        results = []

        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            future_to_driver = {
                executor.submit(merge_driver_audio, session_id, driver_id, session_name): driver_id
                for driver_id in drivers_to_merge
            }

            for future in as_completed(future_to_driver):
                driver_id = future_to_driver[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"[MERGE] Completed driver: {driver_id} - Status: {result.get('status')}")
                except Exception as e:
                    print(f"[MERGE] Exception for driver {driver_id}: {e}")
                    results.append({
                        "driver_id": driver_id,
                        "status": "error",
                        "error": str(e)
                    })

        successful = [r for r in results if r.get("status") == "success"]
        failed = [r for r in results if r.get("status") == "error"]
        skipped = [r for r in results if r.get("status") == "skipped"]

        print(f"[MERGE] Complete - Success: {len(successful)}, Failed: {len(failed)}, Skipped: {len(skipped)}")

        record_audit_event('AUDIO_MERGE_SUCCESS', 'pipeline', session_id, 'system', {
            'total_drivers': len(drivers_to_merge),
            'successful': len(successful),
            'failed': len(failed),
            'skipped': len(skipped)
        })

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Audio merge complete for session {session_id}",
                "session_id": session_id,
                "total_drivers": len(drivers_to_merge),
                "successful": len(successful),
                "failed": len(failed),
                "skipped": len(skipped),
                "batch_size": BATCH_SIZE,
                "results": results
            })
        }

    except Exception as e:
        print(f"[MERGE] Session-level merge failed: {type(e).__name__}: {e}")
        record_audit_event('AUDIO_MERGE_FAILED', 'pipeline', session_id, 'system', {
            'error': f"{type(e).__name__}: {e}"
        })
        raise


def merge_session_audio_upload(session_id, driver_ids=None):
    """
    UPLOAD-FLOW ONLY. Identical to merge_session_audio, except it calls
    merge_driver_audio_upload (writes to UPLOAD_OUTPUT_PREFIX = "merged-audio/"
    instead of OUTPUT_PREFIX = "full-sessions/"). Live flow never calls this
    function - see lambda_handler's "flow" == "upload" check.
    """
    print(f"[MERGE_UPLOAD] Starting upload merge for session: {session_id}")
    record_audit_event('AUDIO_MERGE_START', 'pipeline', session_id, 'system', {
        'requested_driver_ids': driver_ids,
        'flow': 'upload'
    })
    try:
        session_name = get_session_name(session_id)
        print(f"[MERGE_UPLOAD] Session name: {session_name}")
        if driver_ids:
            drivers_to_merge = driver_ids
        else:
            drivers_to_merge = get_drivers_in_session(session_id)
        if not drivers_to_merge:
            print(f"[MERGE_UPLOAD] No drivers found for session {session_id}")
            record_audit_event('AUDIO_MERGE_SUCCESS', 'pipeline', session_id, 'system', {
                'total_drivers': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'reason': 'No drivers found',
                'flow': 'upload'
            })
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No drivers found for this session",
                    "session_id": session_id,
                    "merged": []
                })
            }
        print(f"[MERGE_UPLOAD] Found {len(drivers_to_merge)} drivers to merge: {drivers_to_merge}")
        print(f"[MERGE_UPLOAD] Using batch processing with BATCH_SIZE={BATCH_SIZE}")
        results = []
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            future_to_driver = {
                executor.submit(merge_driver_audio_upload, session_id, driver_id, session_name): driver_id
                for driver_id in drivers_to_merge
            }
            for future in as_completed(future_to_driver):
                driver_id = future_to_driver[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"[MERGE_UPLOAD] Completed driver: {driver_id} - Status: {result.get('status')}")
                except Exception as e:
                    print(f"[MERGE_UPLOAD] Exception for driver {driver_id}: {e}")
                    results.append({
                        "driver_id": driver_id,
                        "status": "error",
                        "error": str(e)
                    })
        successful = [r for r in results if r.get("status") == "success"]
        failed = [r for r in results if r.get("status") == "error"]
        skipped = [r for r in results if r.get("status") == "skipped"]
        print(f"[MERGE_UPLOAD] Complete - Success: {len(successful)}, Failed: {len(failed)}, Skipped: {len(skipped)}")
        record_audit_event('AUDIO_MERGE_SUCCESS', 'pipeline', session_id, 'system', {
            'total_drivers': len(drivers_to_merge),
            'successful': len(successful),
            'failed': len(failed),
            'skipped': len(skipped),
            'flow': 'upload'
        })
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Upload audio merge complete for session {session_id}",
                "session_id": session_id,
                "total_drivers": len(drivers_to_merge),
                "successful": len(successful),
                "failed": len(failed),
                "skipped": len(skipped),
                "batch_size": BATCH_SIZE,
                "results": results
            })
        }
    except Exception as e:
        print(f"[MERGE_UPLOAD] Session-level merge failed: {type(e).__name__}: {e}")
        record_audit_event('AUDIO_MERGE_FAILED', 'pipeline', session_id, 'system', {
            'error': f"{type(e).__name__}: {e}",
            'flow': 'upload'
        })
        raise


def merge_driver_audio_upload(session_id, driver_id, session_name=None):
    """
    UPLOAD-FLOW ONLY. Identical to merge_driver_audio, except the merged file
    is written under UPLOAD_OUTPUT_PREFIX ("merged-audio/") instead of
    OUTPUT_PREFIX ("full-sessions/"). Live flow never calls this function.
    """
    print(f"[MERGE_DRIVER_UPLOAD] Processing driver: {driver_id}")
    driver_name = get_driver_name(driver_id)
    print(f"[MERGE_DRIVER_UPLOAD] Driver name: {driver_name}")
    if not session_name:
        session_name = session_id
    prefix = f"{INPUT_PREFIX}{session_id}/{driver_id}/"
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )
        objects = response.get('Contents', [])
        mp3_files = [
            obj for obj in objects
            if obj['Key'].lower().endswith('.mp3') and obj.get('Size', 0) > 100
        ]
        if not mp3_files:
            print(f"[MERGE_DRIVER_UPLOAD] No MP3 files found for driver {driver_id}")
            return {
                "driver_id": driver_id,
                "status": "skipped",
                "reason": "No MP3 files found"
            }
        mp3_files.sort(key=lambda x: x['Key'])
        print(f"[MERGE_DRIVER_UPLOAD] Found {len(mp3_files)} MP3 files to process")
        local_files = []
        skipped_files = []
        for i, obj in enumerate(mp3_files):
            key = obj['Key']
            filename = os.path.basename(key)
            local_path = f"/tmp/{driver_id}_{filename}"
            if (i + 1) % 50 == 0 or i == 0:
                print(f"[MERGE_DRIVER_UPLOAD] Downloading {i+1}/{len(mp3_files)}: {filename}")
            s3_client.download_file(S3_BUCKET, key, local_path)
            if is_valid_mp3(local_path):
                local_files.append(local_path)
            else:
                print(f"[MERGE_DRIVER_UPLOAD] Skipping invalid/corrupted file: {filename}")
                skipped_files.append(filename)
                try:
                    os.remove(local_path)
                except:
                    pass
        if not local_files:
            print(f"[MERGE_DRIVER_UPLOAD] No valid MP3 files after validation for driver {driver_id}")
            return {
                "driver_id": driver_id,
                "status": "error",
                "reason": "All MP3 files are corrupted or invalid",
                "total_files": len(mp3_files),
                "skipped_files": len(skipped_files)
            }
        print(f"[MERGE_DRIVER_UPLOAD] Valid files: {len(local_files)}, Skipped: {len(skipped_files)}")
        concat_list_path = f"/tmp/{driver_id}_concat_list.txt"
        with open(concat_list_path, 'w') as f:
            for local_file in local_files:
                f.write(f"file '{local_file}'\n")
        output_local = f"/tmp/{driver_id}_full_audio.mp3"
        print(f"[MERGE_DRIVER_UPLOAD] Running FFmpeg concat for {len(local_files)} valid files...")
        ffmpeg_cmd = [
            FFMPEG,
            "-hide_banner",
            "-loglevel", "warning",
            "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_list_path,
            "-c", "copy",
            output_local
        ]
        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"[MERGE_DRIVER_UPLOAD] FFmpeg error: {result.stderr}")
            cleanup_temp_files(local_files + [concat_list_path])
            raise Exception(f"FFmpeg failed: {result.stderr}")
        print(f"[MERGE_DRIVER_UPLOAD] FFmpeg concat successful")
        output_size = os.path.getsize(output_local)
        print(f"[MERGE_DRIVER_UPLOAD] Output file size: {output_size / (1024*1024):.2f} MB")
        safe_session_name = sanitize_filename(session_name)
        safe_driver_name = sanitize_filename(driver_name)
        output_filename = f"{safe_session_name}_{safe_driver_name}_full_audio.mp3"
        output_key = f"{UPLOAD_OUTPUT_PREFIX}{session_id}/{driver_id}/{output_filename}"
        print(f"[MERGE_DRIVER_UPLOAD] Uploading to: s3://{S3_BUCKET}/{output_key}")
        s3_client.upload_file(output_local, S3_BUCKET, output_key)
        cleanup_temp_files(local_files + [concat_list_path, output_local])
        print(f"[MERGE_DRIVER_UPLOAD] Successfully merged audio for driver {driver_id}")
        return {
            "driver_id": driver_id,
            "status": "success",
            "chunks_merged": len(local_files),
            "chunks_skipped": len(skipped_files),
            "output_size_mb": round(output_size / (1024*1024), 2),
            "output_path": f"s3://{S3_BUCKET}/{output_key}"
        }
    except Exception as e:
        print(f"[MERGE_DRIVER_UPLOAD] Error merging driver {driver_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "driver_id": driver_id,
            "status": "error",
            "error": str(e)
        }


def get_drivers_in_session(session_id):
    """Get list of driver IDs that have audio chunks in this session"""
    prefix = f"{INPUT_PREFIX}{session_id}/"

    print(f"[DRIVERS] Listing drivers in: s3://{S3_BUCKET}/{prefix}")

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix,
            Delimiter='/'
        )

        drivers = []
        for prefix_info in response.get('CommonPrefixes', []):
            driver_prefix = prefix_info['Prefix']
            driver_id = driver_prefix.replace(prefix, '').rstrip('/')
            if driver_id:
                drivers.append(driver_id)

        print(f"[DRIVERS] Found drivers: {drivers}")
        return drivers

    except Exception as e:
        print(f"[DRIVERS] Error listing drivers: {e}")
        return []


def is_valid_mp3(file_path):
    """
    Check if a file is a valid MP3 using FFmpeg probe
    This is more reliable than header checks because it verifies actual audio frames

    Args:
        file_path: Path to the MP3 file

    Returns:
        True if valid, False otherwise
    """
    try:
        if not os.path.exists(file_path):
            return False

        file_size = os.path.getsize(file_path)
        if file_size < 100:
            print(f"[VALIDATE] File too small ({file_size} bytes): {os.path.basename(file_path)}")
            return False


        ffprobe_cmd = [
            FFMPEG,
            "-v", "error",
            "-i", file_path,
            "-f", "null",
            "-t", "0.1",
            "-"
        ]

        result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, timeout=5)

        if result.returncode != 0:
            print(f"[VALIDATE] FFmpeg probe failed for {os.path.basename(file_path)}: {result.stderr[:100]}")
            return False

        if "misdetection possible" in result.stderr or "Failed to find" in result.stderr:
            print(f"[VALIDATE] FFmpeg detected issues with {os.path.basename(file_path)}")
            return False

        return True

    except subprocess.TimeoutExpired:
        print(f"[VALIDATE] FFmpeg probe timeout for {os.path.basename(file_path)}")
        return False
    except Exception as e:
        print(f"[VALIDATE] Error checking file {os.path.basename(file_path)}: {e}")
        return False


def merge_driver_audio(session_id, driver_id, session_name=None):
    """
    Merge all audio chunks for a specific driver into a single file
    Validates MP3 files and skips corrupted ones

    Args:
        session_id: Session ID
        driver_id: Driver ID
        session_name: Optional session name for filename (defaults to session_id)

    Returns:
        Result dict with status and output path
    """
    print(f"[MERGE_DRIVER] Processing driver: {driver_id}")

    driver_name = get_driver_name(driver_id)
    print(f"[MERGE_DRIVER] Driver name: {driver_name}")

    if not session_name:
        session_name = session_id

    prefix = f"{INPUT_PREFIX}{session_id}/{driver_id}/"

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )

        objects = response.get('Contents', [])

        mp3_files = [
            obj for obj in objects
            if obj['Key'].lower().endswith('.mp3') and obj.get('Size', 0) > 100
        ]

        if not mp3_files:
            print(f"[MERGE_DRIVER] No MP3 files found for driver {driver_id}")
            return {
                "driver_id": driver_id,
                "status": "skipped",
                "reason": "No MP3 files found"
            }

        mp3_files.sort(key=lambda x: x['Key'])

        print(f"[MERGE_DRIVER] Found {len(mp3_files)} MP3 files to process")

        local_files = []
        skipped_files = []

        for i, obj in enumerate(mp3_files):
            key = obj['Key']
            filename = os.path.basename(key)
            local_path = f"/tmp/{driver_id}_{filename}"

            if (i + 1) % 50 == 0 or i == 0:
                print(f"[MERGE_DRIVER] Downloading {i+1}/{len(mp3_files)}: {filename}")

            s3_client.download_file(S3_BUCKET, key, local_path)

            if is_valid_mp3(local_path):
                local_files.append(local_path)
            else:
                print(f"[MERGE_DRIVER] Skipping invalid/corrupted file: {filename}")
                skipped_files.append(filename)
                try:
                    os.remove(local_path)
                except:
                    pass

        if not local_files:
            print(f"[MERGE_DRIVER] No valid MP3 files after validation for driver {driver_id}")
            return {
                "driver_id": driver_id,
                "status": "error",
                "reason": "All MP3 files are corrupted or invalid",
                "total_files": len(mp3_files),
                "skipped_files": len(skipped_files)
            }

        print(f"[MERGE_DRIVER] Valid files: {len(local_files)}, Skipped: {len(skipped_files)}")

        concat_list_path = f"/tmp/{driver_id}_concat_list.txt"
        with open(concat_list_path, 'w') as f:
            for local_file in local_files:
                f.write(f"file '{local_file}'\n")

        output_local = f"/tmp/{driver_id}_full_audio.mp3"

        print(f"[MERGE_DRIVER] Running FFmpeg concat for {len(local_files)} valid files...")

        ffmpeg_cmd = [
            FFMPEG,
            "-hide_banner",
            "-loglevel", "warning",
            "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_list_path,
            "-c", "copy",
            output_local
        ]

        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"[MERGE_DRIVER] FFmpeg error: {result.stderr}")
            cleanup_temp_files(local_files + [concat_list_path])
            raise Exception(f"FFmpeg failed: {result.stderr}")

        print(f"[MERGE_DRIVER] FFmpeg concat successful")

        output_size = os.path.getsize(output_local)
        print(f"[MERGE_DRIVER] Output file size: {output_size / (1024*1024):.2f} MB")

        safe_session_name = sanitize_filename(session_name)
        safe_driver_name = sanitize_filename(driver_name)
        output_filename = f"{safe_session_name}_{safe_driver_name}_full_audio.mp3"
        output_key = f"{OUTPUT_PREFIX}{session_id}/{driver_id}/{output_filename}"

        print(f"[MERGE_DRIVER] Uploading to: s3://{S3_BUCKET}/{output_key}")
        s3_client.upload_file(output_local, S3_BUCKET, output_key)

        cleanup_temp_files(local_files + [concat_list_path, output_local])

        print(f"[MERGE_DRIVER] Successfully merged audio for driver {driver_id}")

        return {
            "driver_id": driver_id,
            "status": "success",
            "chunks_merged": len(local_files),
            "chunks_skipped": len(skipped_files),
            "output_size_mb": round(output_size / (1024*1024), 2),
            "output_path": f"s3://{S3_BUCKET}/{output_key}"
        }

    except Exception as e:
        print(f"[MERGE_DRIVER] Error merging driver {driver_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "driver_id": driver_id,
            "status": "error",
            "error": str(e)
        }


def cleanup_temp_files(file_paths):
    """Clean up temporary files"""
    for path in file_paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            print(f"[CLEANUP] Warning: Could not delete {path}: {e}")



def get_full_audio_url(session_id, driver_id):
    """
    Generate presigned URL for downloading full session audio

    Args:
        session_id: Session ID
        driver_id: Driver ID

    Returns:
        Dict with url, filename or None if file doesn't exist
    """
    prefix = f"{OUTPUT_PREFIX}{session_id}/{driver_id}/"

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )

        objects = response.get('Contents', [])

        full_audio_files = [
            obj for obj in objects
            if obj['Key'].endswith('_full_audio.mp3')
        ]

        if not full_audio_files:
            old_key = f"{OUTPUT_PREFIX}{session_id}/{driver_id}/full_audio.mp3"
            try:
                s3_client.head_object(Bucket=S3_BUCKET, Key=old_key)
                full_audio_files = [{'Key': old_key}]
            except:
                return None

        output_key = full_audio_files[0]['Key']
        filename = os.path.basename(output_key)

        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": output_key},
            ExpiresIn=43200
        )

        return {
            "url": url,
            "filename": filename,
            "s3_key": output_key
        }

    except Exception as e:
        print(f"Error getting full audio URL: {e}")
        return None


def list_available_full_audios(session_id):
    """
    List all drivers that have full audio available for a session

    Args:
        session_id: Session ID

    Returns:
        List of driver IDs with full audio available
    """
    prefix = f"{OUTPUT_PREFIX}{session_id}/"

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix,
            Delimiter='/'
        )

        drivers = []
        for prefix_info in response.get('CommonPrefixes', []):
            driver_prefix = prefix_info['Prefix']
            driver_id = driver_prefix.replace(prefix, '').rstrip('/')
            if driver_id:
                drivers.append(driver_id)

        return drivers

    except Exception as e:
        print(f"Error listing full audios: {e}")
        return []





