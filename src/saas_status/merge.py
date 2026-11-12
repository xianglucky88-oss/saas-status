from __future__ import annotations
from .models import StatusEvent


_STATE_PRIORITY = {"investigating": 1, "identified": 2, "monitoring": 3, "resolved": 4}
_IMPACT_PRIORITY = {"none": 0, "minor": 1, "major": 2, "critical": 3}


def merge_events(events: list[StatusEvent]) -> list[StatusEvent]:
    merged: dict[str, StatusEvent] = {}
    for event in events:
        previous = merged.get(event.event_key)
        if previous is None:
            merged[event.event_key] = event
            continue
        state = event.state if _STATE_PRIORITY.get(event.state, 0) >= _STATE_PRIORITY.get(previous.state, 0) else previous.state
        impact = event.impact if _IMPACT_PRIORITY.get(event.impact, 0) >= _IMPACT_PRIORITY.get(previous.impact, 0) else previous.impact
        merged[event.event_key] = StatusEvent(event.service, event.component, event.event_id, event.started_at, max(previous.updated_at, event.updated_at), state, impact, event.summary or previous.summary, ",".join(sorted(set((previous.source_ref + "," + event.source_ref).split(",")))))
    return sorted(merged.values(), key=lambda item: (item.service, item.updated_at))
