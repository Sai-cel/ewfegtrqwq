"""Central configuration. All values overridable via environment variables.

Defaults reflect the test environment (account 838254633873, us-west-2).
For prod, set AWS_REGION, S3_BUCKET, table names, and the scheduler/merge
Lambda names accordingly.
"""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

    # --- AWS ---
    aws_region: str = "us-west-2"

    # --- S3 ---
    s3_bucket: str = "test-v2-andretti-transcription-system"
    full_sessions_prefix: str = "full-sessions/"
    raw_chunks_prefix: str = "raw-chunks/"
    silence_removed_prefix: str = "silence-removed/"
    transcriptions_prefix: str = "transcriptions/"
    uploads_prefix: str = "uploads/"

    # --- DynamoDB ---
    session_table: str = "v2_SessionConfig"
    driver_table: str = "v2_DriverConfig"
    active_sessions_table: str = "v2_ActiveSessions"

    # --- Lambda (writes are proxied to the scheduler to reuse its logic) ---
    scheduler_lambda: str = "v2_session_scheduler_lambda"
    audio_merge_lambda: str = "v2-audio-merge-lambda"

    # --- OpenSearch ---
    opensearch_endpoint: str = ""          # e.g. https://search-xxx.us-west-2.es.amazonaws.com
    # The index the pipeline actually writes to. This used to default to
    # "transcripts", which is a REAL but nearly-empty index (14 docs) - the code
    # happened to hardcode "v2_transcriptions" everywhere, so the wrong default
    # was never noticed. It is now the single source of truth and the service
    # reads it, so a wrong value here would be obvious immediately rather than
    # silently returning zeros.
    opensearch_index: str = "v2_transcriptions"

    # --- CloudWatch ---
    pipeline_lambdas: list[str] = [
        "v2_audio_chunking_lambda",
        "v2_silence_removal",
        "v2_transcriptions",
        "v2_opensearch_dynamodb_storing",
        "v2_session_scheduler_lambda",
    ]
    ecs_cluster: str = "andretti-fe-dev-container-audio-chunks-computation"

    # --- API ---
    cors_origins: list[str] = ["*"]
    api_title: str = "Andretti FE Platform API"
    api_version: str = "1.0.0"

    # Pipeline stage order, used by the pipeline status endpoint.
    @property
    def pipeline_stages(self) -> list[tuple[str, str]]:
        return [
            ("raw_chunks", self.raw_chunks_prefix),
            ("silence_removed", self.silence_removed_prefix),
            ("transcriptions", self.transcriptions_prefix),
            ("full_sessions", self.full_sessions_prefix),
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
