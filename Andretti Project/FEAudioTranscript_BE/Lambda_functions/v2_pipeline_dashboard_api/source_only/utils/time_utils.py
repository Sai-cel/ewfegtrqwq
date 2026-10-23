"""Time helpers shared across routers/services.

Mirrors the scheduler Lambda's timezone handling so the API layer computes
race clocks the same way: timezone-aware parsing, UTC conversion, and
relative display-time formatting.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo, available_timezones
except ImportError:  # pragma: no cover
    ZoneInfo = None
    available_timezones = None


def _parse_utc_offset(tz_name: str | None):
    """Convert 'UTC+05:30' / 'GMT-08:00' / '+05:30' to a tzinfo, or None."""
    if tz_name is None:
        return None
    s = str(tz_name).strip().upper()
    for prefix in ("UTC", "GMT"):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
            break
    if s == "":
        return timezone.utc
    sign = 1
    if s and s[0] in "+-":
        sign = -1 if s[0] == "-" else 1
        s = s[1:]
    if ":" in s:
        hh, mm = s.split(":", 1)
    elif len(s) > 2:
        hh, mm = s[:-2], s[-2:]
    else:
        hh, mm = s, "0"
    try:
        hours, minutes = int(hh), int(mm)
    except ValueError:
        return None
    if hours > 14 or minutes >= 60:
        return None
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def parse_session_datetime(dt_str: str, tz_name: str | None = None) -> datetime:
    """Parse an ISO datetime into a tz-aware datetime.

    Priority: offset in the string -> tz_name as UTC offset -> tz_name as IANA
    -> default UTC. Identical resolution to the scheduler Lambda.
    """
    dt = datetime.fromisoformat(str(dt_str).strip().replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        return dt
    if tz_name:
        off = _parse_utc_offset(tz_name)
        if off is not None:
            return dt.replace(tzinfo=off)
        if ZoneInfo is not None:
            try:
                return dt.replace(tzinfo=ZoneInfo(tz_name))
            except Exception:
                pass
    return dt.replace(tzinfo=timezone.utc)


def to_utc_iso(dt_str: str, tz_name: str | None = None) -> str:
    """Parse then return a canonical UTC ISO string ('...Z')."""
    return parse_session_datetime(dt_str, tz_name).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def to_epoch(dt_str: str, tz_name: str | None = None) -> int:
    return int(parse_session_datetime(dt_str, tz_name).timestamp())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _format_offset(minutes: int) -> str:
    sign = "+" if minutes >= 0 else "-"
    a = abs(int(minutes))
    return f"UTC{sign}{a // 60:02d}:{a % 60:02d}"


def list_timezones() -> list[dict]:
    """Full IANA list (each labelled with current offset), or a UTC fallback."""
    if ZoneInfo is not None and available_timezones is not None:
        now = datetime.now(timezone.utc)
        zones = []
        for name in available_timezones():
            try:
                off = now.astimezone(ZoneInfo(name)).utcoffset()
                mins = int(off.total_seconds() // 60) if off else 0
            except Exception:
                continue
            zones.append({"value": name, "label": f"({_format_offset(mins)}) {name}",
                          "offset_minutes": mins})
        if zones:
            if not any(z["value"] == "UTC" for z in zones):
                zones.append({"value": "UTC", "label": "(UTC+00:00) UTC", "offset_minutes": 0})
            zones.sort(key=lambda z: (z["offset_minutes"], z["value"]))
            return zones
    return [{"value": "UTC", "label": "(UTC+00:00) UTC", "offset_minutes": 0}]


def display_time(started_at: int | None, offset_seconds: int | None, race_start_ts: int | None) -> str | None:
    """Format a transcript's race-relative time as [-]MM:SS or [-]HH:MM:SS."""
    if started_at is None or offset_seconds is None or race_start_ts is None:
        return None
    delta = (started_at + offset_seconds) - race_start_ts
    sign = "-" if delta < 0 else ""
    a = abs(delta)
    h, m, s = a // 3600, (a % 3600) // 60, a % 60
    return f"{sign}{h:02d}:{m:02d}:{s:02d}" if h else f"{sign}{m:02d}:{s:02d}"
