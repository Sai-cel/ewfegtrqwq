"""S3 access — fast stage counts using list_objects_v2 with max 100 keys."""
from __future__ import annotations
import boto3
from config import settings

AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".wav", ".mp4")

# What counts as a real artefact for each pipeline stage.
# WHY: stage_counts_for_session() used to add page["KeyCount"], i.e. EVERY object
# under the prefix. raw-chunks/{session}/{driver}/ also holds one metadata.json
# per driver, so the RAW column was always inflated by exactly the number of
# drivers (measured: +5 on 5-driver races, +8 on 8-driver races, every time).
# That made RAW look larger than SILENCE and TRANSCRIBE look larger than SILENCE,
# which is impossible and made the board mistrusted.
STAGE_FILTERS = {
    "raw_chunks":      lambda k: k.lower().endswith(AUDIO_EXTS),
    "silence_removed": lambda k: k.lower().endswith(AUDIO_EXTS),
    "transcriptions":  lambda k: k.lower().endswith(".json") and "/transcribe-" in k,
    "full_sessions":   lambda k: k.lower().endswith(AUDIO_EXTS),
}

class S3Service:
    def __init__(self) -> None:
        self._s3 = boto3.client("s3", region_name=settings.aws_region)
        self.bucket = settings.s3_bucket

    def _common_prefixes(self, prefix: str) -> list[str]:
        prefix = prefix if prefix.endswith("/") else prefix + "/"
        out = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                folder = cp["Prefix"][len(prefix):].rstrip("/")
                if folder:
                    out.append(folder)
        return sorted(out)

    def list_sessions(self) -> list[str]:
        return sorted(self._common_prefixes(settings.full_sessions_prefix), reverse=True)

    def list_drivers_with_audio(self, session: str) -> list[dict]:
        base = f"{settings.full_sessions_prefix.rstrip('/')}/{session}/"
        agg: dict[str, dict] = {}
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=base):
            for obj in page.get("Contents", []):
                rest = obj["Key"][len(base):]
                if "/" not in rest:
                    continue
                d = agg.setdefault(rest.split("/")[0], {"code": rest.split("/")[0],
                                                         "has_audio": False, "bytes": 0})
                if obj["Key"].lower().endswith(AUDIO_EXTS):
                    d["has_audio"] = True
                    d["bytes"] += obj["Size"]
        return [agg[k] for k in sorted(agg)]

    def count_objects(self, prefix: str) -> tuple[int, str | None]:
        count, last = 0, None
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                count += 1
                lm = obj["LastModified"]
                if last is None or lm > last:
                    last = lm
        return count, (last.isoformat() if last else None)

    def stage_counts_for_session(self, session_id: str) -> dict[str, int]:
        """Full, uncapped count per stage — pages through all objects instead
        of stopping at 1000 (the old MaxKeys=1000 cap silently under-reported
        any session with more than 1000 files per stage).

        Counts only the artefact each stage actually produces, so the columns
        are comparable to each other. Previously this summed KeyCount, which
        included metadata.json and any other bookkeeping file in the prefix."""
        counts = {name: 0 for name, _ in settings.pipeline_stages}
        for name, prefix in settings.pipeline_stages:
            keep = STAGE_FILTERS.get(name, lambda k: True)
            try:
                total = 0
                for page in self._s3.get_paginator("list_objects_v2").paginate(
                    Bucket=self.bucket,
                    Prefix=f"{prefix.rstrip('/')}/{session_id}/",
                ):
                    for obj in page.get("Contents", []):
                        if keep(obj["Key"]):
                            total += 1
                counts[name] = total
            except Exception:
                counts[name] = 0
        return counts

    def presign(self, key: str, expires: int = 3600) -> str:
        return self._s3.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires
        )

    # ---- per-driver drill down (used by /pipeline/session/{id}/drivers) ----
    def driver_stage_range(self, prefix: str, session_id: str, driver: str,
                            exts: tuple[str, ...] = AUDIO_EXTS) -> dict:
        """Count + first/last upload time for one driver's files in one stage."""
        key_prefix = f"{prefix.rstrip('/')}/{session_id}/{driver}/"
        count = 0
        first = last = None
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=key_prefix
        ):
            for obj in page.get("Contents", []):
                if not obj["Key"].lower().endswith(exts):
                    continue
                count += 1
                lm = obj["LastModified"]
                if first is None or lm < first:
                    first = lm
                if last is None or lm > last:
                    last = lm
        return {
            "count": count,
            "first": first.isoformat() if first else None,
            "last": last.isoformat() if last else None,
        }

    def driver_stage_chunks(self, prefix: str, session_id: str, driver: str,
                             exts: tuple[str, ...] = AUDIO_EXTS) -> list[dict]:
        """Full per-file list for one driver's files in one stage (expand-on-click only)."""
        key_prefix = f"{prefix.rstrip('/')}/{session_id}/{driver}/"
        out = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=key_prefix
        ):
            for obj in page.get("Contents", []):
                if not obj["Key"].lower().endswith(exts):
                    continue
                out.append({
                    "file": obj["Key"].rsplit("/", 1)[-1],
                    "last_modified": obj["LastModified"].isoformat(),
                })
        out.sort(key=lambda c: c["file"])
        return out

s3_service = S3Service()
