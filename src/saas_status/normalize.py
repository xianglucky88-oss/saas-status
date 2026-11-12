from __future__ import annotations
from .models import StatusEvent

_STATES = {"investigating": "investigating", "identified": "identified", "monitoring": "monitoring", "resolved": "resolved"}
_IMPACTS = {"none": "none", "minor": "minor", "major": "major", "critical": "critical"}


def normalize_event(raw: dict) -> StatusEvent:
    return StatusEvent(
        service=str(raw["service"]).strip().lower(),
        component=str(raw["component"]).strip().lower(),
        event_id=str(raw["event_id"]).strip(),
        started_at=str(raw["started_at"]).strip(),
        updated_at=str(raw["updated_at"]).strip(),
        state=_STATES.get(str(raw.get("state", "investigating")).lower(), "unknown"),
        impact=_IMPACTS.get(str(raw.get("impact", "none")).lower(), "unknown"),
        summary=" ".join(str(raw.get("summary", "")).split()),
        source_ref=str(raw["source_ref"]).strip(),
    )
