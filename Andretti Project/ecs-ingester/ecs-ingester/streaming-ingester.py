"""
Streaming Audio Ingester for 2-Second Latency

This ingester uses Amazon Transcribe Streaming API for real-time transcription,
then calls the existing Lambda functions for storage and WebSocket push.

Architecture:
1. FFmpeg extracts audio from HLS stream as PCM (for Transcribe) + MP3 (for S3)
2. Transcribe Streaming API processes PCM in real-time (~1-2 sec latency)
3. Results are formatted and sent to existing opensearch_dynamo_db_lambda_v2
4. Lambda handles DynamoDB/OpenSearch storage and WebSocket push

This approach:
- Reuses existing Lambda functions (no code duplication)
- Only adds streaming-specific FFmpeg + Transcribe Streaming code
- Maintains compatibility with existing frontend and data pipeline

Author: People Tech Group
Updated: January 2025
"""

import os
import re
import boto3
import subprocess
import multiprocessing
import time
import json
import asyncio
import signal
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

# Amazon Transcribe Streaming SDK
from amazon_transcribe.client import TranscribeStreamingClient
from amazon_transcribe.handlers import TranscriptResultStreamHandler
from amazon_transcribe.model import TranscriptEvent


# =====================================================
# CONFIGURATION (same as ecs-ingester/ingester.py)
# =====================================================

AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET = os.getenv("S3_BUCKET", "test-v2-andretti-transcription-system")
S3_PREFIX = os.getenv("S3_PREFIX", "raw-chunks")

# Segment configuration
SEGMENT_DURATION = int(os.getenv("SEGMENT_DURATION", "10"))
OUTPUT_DIR = "/tmp/audio_output"

# Streaming configuration
STREAMING_ENABLED = os.getenv("STREAMING_ENABLED", "true").lower() == "true"
STREAMING_SAMPLE_RATE = int(os.getenv("STREAMING_SAMPLE_RATE", "16000"))
STREAMING_LANGUAGE_CODE = os.getenv("STREAMING_LANGUAGE_CODE", "en-US")
CUSTOM_VOCABULARY_NAME = os.getenv("CUSTOM_VOCABULARY_NAME", "")

# Lambda invocation (for storage and WebSocket)
OPENSEARCH_LAMBDA_NAME = os.getenv("OPENSEARCH_LAMBDA_NAME", "v2-opensearch-dynamodb-lambda")
USE_DIRECT_LAMBDA_INVOKE = os.getenv("USE_DIRECT_LAMBDA_INVOKE", "true").lower() == "true"

# DynamoDB tables
ACTIVE_SESSIONS_TABLE = os.environ.get("ACTIVE_SESSIONS_TABLE", "v2_ActiveSessions")
DRIVER_CONFIG_TABLE = os.environ.get("DRIVER_CONFIG_TABLE", "v2_DriverConfig")
SESSION_CONFIG_TABLE = os.environ.get("SESSION_CONFIG_TABLE", "v2_SessionConfig")

# Use DynamoDB config (same logic as existing ingester)
ENV_SESSION_ID = os.environ.get("SESSION_ID")
ENV_DRIVER_URL = os.environ.get("DRIVER_01_URL")

if ENV_SESSION_ID:
    SESSION_ID = ENV_SESSION_ID
    print(f"[SESSION] Using SESSION_ID from environment: {SESSION_ID}")
else:
    TASK_START_TIME = datetime.now(timezone.utc)
    SESSION_ID = TASK_START_TIME.strftime("%Y-%m-%d_%H-%M")
    print(f"[SESSION] Generated SESSION_ID: {SESSION_ID}")

if ENV_DRIVER_URL:
    USE_DYNAMODB_CONFIG = False
else:
    USE_DYNAMODB_CONFIG = os.environ.get("USE_DYNAMODB_CONFIG", "true").lower() == "true"


# =====================================================
# HELPER FUNCTIONS (same as existing ingester)
# =====================================================

def sanitize_for_path(name):
    """Sanitize a name for use in S3 paths and local directories."""
    return re.sub(r'[^0-9a-zA-Z._-]', '_', name)


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

def log_system(message, level="INFO"):
    print(
        f"[{level}] [SYSTEM] "
        f"{datetime.now(timezone.utc).isoformat()} :: {message}",
        flush=True,
    )


# =====================================================
# DRIVER CONFIGURATION (same as existing ingester)
# =====================================================

def get_driver_config_from_dynamodb():
    """Get driver configuration from DynamoDB (frontend config)."""
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(DRIVER_CONFIG_TABLE)

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

        print(f"[CONFIG] Loaded {len(config)} drivers from DynamoDB")
        return config

    except Exception as e:
        print(f"[CONFIG] Error reading DynamoDB: {e}")
        return None


def get_driver_config_from_env():
    """Build driver config from environment variables (fallback)."""
    config = {}

    for driver_num in range(1, 23):
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

    return config


def build_driver_config():
    """Build driver config - tries DynamoDB first, falls back to ENV vars."""
    config = None

    if USE_DYNAMODB_CONFIG:
        print("[CONFIG] Checking DynamoDB for driver configuration...")
        config = get_driver_config_from_dynamodb()

    if not config:
        print("[CONFIG] Using environment variables for driver configuration...")
        config = get_driver_config_from_env()

    if not config:
        print("[WARNING] No driver URLs found!")

    return config


# Build driver config
DRIVER_CONFIG = build_driver_config()


# =====================================================
# SESSION STATUS (same as existing ingester)
# =====================================================

def update_session_status(session_id, status):
    """Update the ActiveSessions DynamoDB table with ECS session status."""
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(ACTIVE_SESSIONS_TABLE)

        now_ts = int(time.time())

        if status == 'ACTIVE':
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

        log_system(f"Updated session {session_id} to {status}")
    except Exception as e:
        log_system(f"Error updating session status: {e}", "ERROR")


# =====================================================
# AWS CLIENTS
# =====================================================

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def get_lambda_client():
    return boto3.client("lambda", region_name=AWS_REGION)


# =====================================================
# TRANSCRIBE STREAMING HANDLER
# =====================================================

class StreamingTranscriptHandler(TranscriptResultStreamHandler):
    """
    Handles streaming transcription results from AWS Transcribe.

    When a final transcript is received, it formats the result and
    invokes the existing opensearch_dynamo_db_lambda_v2 for storage
    and WebSocket push.
    """

    def __init__(self, stream, driver_id, driver_name, driver_folder, team, session_id):
        super().__init__(stream)
        self.driver_id = driver_id
        self.driver_name = driver_name
        self.driver_folder = driver_folder
        self.team = team
        self.session_id = session_id
        self.segment_count = 0
        self.lambda_client = get_lambda_client()
        self.s3_client = get_s3_client()

    async def handle_transcript_event(self, transcript_event: TranscriptEvent):
        """Process incoming transcript events with speaker diarization support"""
        results = transcript_event.transcript.results

        for result in results:
            if result.is_partial:
                # Skip partial results, wait for final
                continue

            if not result.alternatives:
                continue

            alternative = result.alternatives[0]
            transcript_text = alternative.transcript

            if not transcript_text or not transcript_text.strip():
                continue

            self.segment_count += 1

            # Extract speaker label from items if available (diarization)
            speaker_label = "spk_0"  # Default
            if hasattr(alternative, 'items') and alternative.items:
                for item in alternative.items:
                    if hasattr(item, 'speaker') and item.speaker:
                        speaker_label = item.speaker
                        break

            # Calculate timing information
            start_time = float(result.start_time) if hasattr(result, 'start_time') else 0
            end_time = float(result.end_time) if hasattr(result, 'end_time') else start_time

            # Calculate duration
            duration = end_time - start_time

            # Session time display
            total_seconds = int(start_time)
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            session_time = f"{minutes:02d}:{seconds:02d}"

            # Calculate confidence score
            confidence = self._calculate_confidence(alternative)

            log_info(self.driver_id, f"[STREAMING] [{speaker_label}] (conf: {confidence:.2f}, dur: {duration:.1f}s) {transcript_text[:50]}...")

            # Build transcription result in same format as Transcribe Batch API
            transcription_result = self._build_transcription_result(
                transcript_text,
                result,
                session_time,
                speaker_label,
                confidence,
                duration
            )

            # Invoke existing Lambda for storage and WebSocket push
            await self._invoke_storage_lambda(transcription_result)

    def _calculate_confidence(self, alternative):
        """Calculate average confidence score from transcript items"""
        if not hasattr(alternative, 'items') or not alternative.items:
            return 0.0

        confidences = []
        for item in alternative.items:
            if hasattr(item, 'confidence') and item.confidence is not None:
                try:
                    confidences.append(float(item.confidence))
                except (ValueError, TypeError):
                    pass

        return sum(confidences) / len(confidences) if confidences else 0.0

    def _build_transcription_result(self, transcript_text, result, session_time, speaker_label, confidence, duration):
        """
        Build transcription result in the same format as Transcribe Batch API output.

        This ensures compatibility with the existing opensearch_dynamo_db_lambda_v2.
        """
        alternative = result.alternatives[0]

        # Build items (words with timestamps, confidence, and speaker labels)
        items = []
        if hasattr(alternative, 'items') and alternative.items:
            for item in alternative.items:
                item_data = {
                    "type": getattr(item, 'type', 'pronunciation'),
                    "start_time": str(getattr(item, 'start_time', 0)),
                    "end_time": str(getattr(item, 'end_time', 0)),
                    "alternatives": [{
                        "content": getattr(item, 'content', ''),
                        "confidence": str(getattr(item, 'confidence', 0.0))
                    }]
                }
                # Include speaker label for each word (diarization)
                if hasattr(item, 'speaker') and item.speaker:
                    item_data["speaker_label"] = item.speaker
                items.append(item_data)

        # Build speaker segments with proper diarization
        start_time_val = float(result.start_time) if hasattr(result, 'start_time') else 0
        end_time_val = float(result.end_time) if hasattr(result, 'end_time') else 0

        segments = [{
            "speaker_label": speaker_label,
            "start_time": str(start_time_val),
            "end_time": str(end_time_val),
            "items": [{"start_time": i.get('start_time', '0'), "speaker_label": i.get('speaker_label', speaker_label)} for i in items]
        }]

        return {
            "status": "COMPLETED",
            "jobName": f"stream_{self.driver_folder}_{self.segment_count}",
            "results": {
                "transcripts": [{"transcript": transcript_text}],
                "items": items,
                "speaker_labels": {
                    "speakers": 2,  # Expected number of speakers (driver + engineer)
                    "segments": segments
                }
            },
            # Metadata for Lambda processing with enhanced quality info
            "_streaming_metadata": {
                "session_id": self.session_id,
                "driver_folder": self.driver_folder,
                "driver_name": self.driver_name,
                "team": self.team,
                "session_time": session_time,
                "is_streaming": True,
                "speaker_label": speaker_label,
                "confidence": confidence,
                "duration": duration,
                "start_time": start_time_val,
                "end_time": end_time_val
            }
        }

    async def _invoke_storage_lambda(self, transcription_result):
        """
        Invoke the existing opensearch_dynamo_db_lambda_v2 for storage.

        The Lambda handles:
        - DynamoDB storage
        - OpenSearch indexing
        - WebSocket push to clients
        """
        if not USE_DIRECT_LAMBDA_INVOKE:
            log_info(self.driver_id, "[STREAMING] Direct Lambda invoke disabled")
            return

        try:
            # Create a mock S3 event that the Lambda expects
            # The Lambda is triggered by S3 events, so we simulate that
            mock_s3_key = f"transcriptions/{self.session_id}/{self.driver_folder}/stream-{self.segment_count}.json"

            # First, write the transcription result to S3 (Lambda reads from S3)
            self.s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=mock_s3_key,
                Body=json.dumps(transcription_result),
                ContentType="application/json"
            )

            log_info(self.driver_id, f"[STREAMING] Wrote transcription to S3: {mock_s3_key}")

            # The S3 event notification will automatically trigger the Lambda
            # No need to invoke Lambda directly - the existing S3 trigger handles it

        except Exception as e:
            log_error(self.driver_id, f"[STREAMING] Error writing to S3: {e}")


# =====================================================
# STREAMING WORKER
# =====================================================

async def streaming_worker_async(driver_id, m3u8_url, driver_name, driver_folder, team):
    """
    Async worker that streams audio to Transcribe Streaming API.

    Uses FFmpeg to extract PCM audio from HLS stream and pipe it
    directly to Transcribe Streaming API.
    """
    log_info(driver_id, "=" * 50)
    log_info(driver_id, "STREAMING WORKER STARTED")
    log_info(driver_id, f"Driver: {driver_name}, Team: {team}")
    log_info(driver_id, f"Stream URL: {m3u8_url[:50]}...")
    log_info(driver_id, "=" * 50)

    # FFmpeg command to extract PCM audio for Transcribe
    # Optimized for better transcription quality
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        # Input settings
        "-reconnect", "1",  # Auto-reconnect on connection loss
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", m3u8_url,
        "-vn",  # No video
        # Audio processing for cleaner output
        # REMOVED (risky): volume=1.5, afftdn=nf=-25 - may not work on all FFmpeg builds
        "-af", "highpass=f=80,lowpass=f=8000",  # Filter noise, focus on speech frequencies
        # Output settings
        "-acodec", "pcm_s16le",  # 16-bit PCM
        "-ar", str(STREAMING_SAMPLE_RATE),  # Sample rate for Transcribe
        "-ac", "1",  # Mono
        "-f", "s16le",  # Raw PCM format
        "-"  # Output to stdout
    ]

    log_info(driver_id, f"Starting FFmpeg process...")

    try:
        # Start FFmpeg process
        ffmpeg_process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Create Transcribe Streaming client
        transcribe_client = TranscribeStreamingClient(region=AWS_REGION)

        # Build transcription parameters with improved quality settings
        transcribe_params = {
            "language_code": STREAMING_LANGUAGE_CODE,
            "media_sample_rate_hz": STREAMING_SAMPLE_RATE,
            "media_encoding": "pcm",
            # Enable partial results stabilization for smoother output
            "enable_partial_results_stabilization": True,
            "partial_results_stability": "high",
            # Enable speaker diarization (identifies different speakers)
            "show_speaker_label": True,
            "number_of_channels": 1,
            "enable_channel_identification": False,
        }

        # Add custom vocabulary if configured (improves racing terminology accuracy)
        if CUSTOM_VOCABULARY_NAME:
            transcribe_params["vocabulary_name"] = CUSTOM_VOCABULARY_NAME
            log_info(driver_id, f"Using custom vocabulary: {CUSTOM_VOCABULARY_NAME}")

        # Add vocabulary filter if configured (reduces noise/profanity)
        vocabulary_filter = os.getenv("VOCABULARY_FILTER_NAME", "")
        if vocabulary_filter:
            transcribe_params["vocabulary_filter_name"] = vocabulary_filter
            transcribe_params["vocabulary_filter_method"] = "mask"
            log_info(driver_id, f"Using vocabulary filter: {vocabulary_filter}")

        log_info(driver_id, f"Starting Transcribe streaming session...")

        # Start streaming transcription
        stream = await transcribe_client.start_stream_transcription(**transcribe_params)

        # Create handler for transcript events
        handler = StreamingTranscriptHandler(
            stream.output_stream,
            driver_id,
            driver_name,
            driver_folder,
            team,
            SESSION_ID
        )

        # Stream audio chunks to Transcribe
        # Larger chunks (200ms) improve transcription quality by providing more context
        async def stream_audio():
            # 6400 bytes = 200ms of audio at 16kHz 16-bit mono
            # Larger chunks provide better context for transcription accuracy
            chunk_size = 6400
            audio_buffer = bytearray()

            while True:
                try:
                    # Read raw audio data from FFmpeg
                    raw_chunk = await asyncio.wait_for(
                        ffmpeg_process.stdout.read(chunk_size),
                        timeout=10.0  # Increased timeout for HLS buffering
                    )

                    if not raw_chunk:
                        # End of stream - send any remaining buffered audio
                        if audio_buffer:
                            await stream.input_stream.send_audio_event(audio_chunk=bytes(audio_buffer))
                        break

                    # Add to buffer
                    audio_buffer.extend(raw_chunk)

                    # Send when we have enough data (ensures consistent chunk sizes)
                    while len(audio_buffer) >= chunk_size:
                        chunk_to_send = bytes(audio_buffer[:chunk_size])
                        audio_buffer = audio_buffer[chunk_size:]
                        await stream.input_stream.send_audio_event(audio_chunk=chunk_to_send)

                except asyncio.TimeoutError:
                    log_info(driver_id, "Audio read timeout, continuing...")
                    continue
                except Exception as e:
                    log_error(driver_id, f"Audio streaming error: {e}")
                    break

            await stream.input_stream.end_stream()

        # Run audio streaming and event handling concurrently
        await asyncio.gather(
            stream_audio(),
            handler.handle_events(),
        )

        log_info(driver_id, f"Streaming session ended. Total segments: {handler.segment_count}")

    except Exception as e:
        log_error(driver_id, f"Streaming worker error: {e}")
        raise
    finally:
        if 'ffmpeg_process' in locals():
            ffmpeg_process.terminate()


def streaming_worker(driver_id):
    """
    Main worker function for each driver (streaming mode).
    Wraps the async worker.
    """
    try:
        config = DRIVER_CONFIG[driver_id]
        m3u8_url = config["m3u8_url"]
        driver_name = config.get("driver_name", f"driver{driver_id:02d}")
        team = config.get("team", "UNKNOWN")
        driver_folder = sanitize_for_path(driver_name)

        # Run async worker
        asyncio.run(streaming_worker_async(
            driver_id, m3u8_url, driver_name, driver_folder, team
        ))

    except KeyError:
        log_error(driver_id, f"Driver {driver_id} not found in DRIVER_CONFIG")
    except Exception as e:
        log_error(driver_id, f"Worker failed: {e}")


# =====================================================
# GRACEFUL SHUTDOWN
# =====================================================

shutdown_flag = multiprocessing.Event()


def signal_handler(signum, _frame):
    log_system(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag.set()
    update_session_status(SESSION_ID, 'STOPPED')


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    main_start_time = time.time()

    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    log_system("=" * 60)
    log_system("STREAMING AUDIO INGESTER STARTED (2-Second Latency)")
    log_system("=" * 60)

    log_system(f"Session ID: {SESSION_ID}")
    log_system(f"Streaming Enabled: {STREAMING_ENABLED}")
    log_system(f"Language: {STREAMING_LANGUAGE_CODE}")
    log_system(f"Sample Rate: {STREAMING_SAMPLE_RATE}")
    log_system(f"Total drivers: {len(DRIVER_CONFIG)}")
    log_system(f"AWS Region: {AWS_REGION}")
    log_system(f"S3 Bucket: {S3_BUCKET}")

    # Mark session as ACTIVE
    update_session_status(SESSION_ID, 'ACTIVE')

    processes = []

    log_system("-" * 60)
    log_system("Spawning streaming worker processes...")
    log_system("-" * 60)

    for driver_id in DRIVER_CONFIG.keys():
        log_system(f"Starting streaming worker for Driver {driver_id:02d}...")
        p = multiprocessing.Process(target=streaming_worker, args=(driver_id,))
        p.start()
        log_system(f"Process started for Driver {driver_id:02d} (PID: {p.pid})")
        processes.append(p)

    log_system(f"All {len(processes)} streaming workers started")
    log_system("-" * 60)

    try:
        for p in processes:
            p.join()
            log_system(f"Process PID {p.pid} completed with exit code: {p.exitcode}")
    except KeyboardInterrupt:
        log_system("Keyboard interrupt received, stopping workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=10)
            if p.is_alive():
                p.kill()

    update_session_status(SESSION_ID, 'STOPPED')

    main_elapsed_time = time.time() - main_start_time

    log_system("=" * 60)
    log_system("ALL STREAMING WORKERS STOPPED")
    log_system(f"Total execution time: {main_elapsed_time:.2f} seconds")
    log_system("=" * 60)
