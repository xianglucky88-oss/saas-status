"""ISO-8601 timestamp helpers.

All timestamps persisted by the new storage are normalized to UTC and rendered
with a trailing ``Z`` so that lexicographic ordering matches chronological
ordering.  The legacy pipeline keeps storing the original strings untouched.
"""
from __future__ import annotations

from datetime import datetime, timezone


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (``Z`` or explicit offset) into aware UTC."""
    text = str(value).strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso_z(value: str) -> str:
    """Normalize any supported ISO-8601 input to ``YYYY-MM-DDTHH:MM:SSZ``."""
    dt = parse_iso(value)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def is_at_or_before(left: str, right: str) -> bool:
    return parse_iso(left) <= parse_iso(right)


def is_in_effect(observed_at: str, effective_from: str, effective_until: str | None) -> bool:
    """Half-open effective interval: ``effective_from <= T < effective_until``."""
    at = parse_iso(observed_at)
    if at < parse_iso(effective_from):
        return False
    if effective_until is not None and at >= parse_iso(effective_until):
        return False
    return True


def now_iso_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
