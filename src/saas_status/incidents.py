"""Incident reconstruction from individual event observations.

The legacy pipeline stores one merged row per event key.  For impact analysis
we instead retain *every* observation (an immutable record of what the source
reported at ``updated_at``) and reconstruct an incident's active interval from
the complete observation set.  A late-arriving observation therefore only
trims/extends/corrects that incident's window: other incidents are untouched.

Window reconstruction (observations ordered by ``updated_at``):

* an interval opens at ``started_at`` when the first non-resolved observation
  appears;
* it closes at the ``updated_at`` of the observation whose state is
  ``resolved``;
* a later non-resolved observation re-opens a new interval
  (flapping / follow-up issue);
* an incident with no resolved observation is open-ended.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .clock import is_at_or_before
from .topology import REASON_UNKNOWN_STATE

_RESOLVED_STATE = "resolved"
_IMPACT_RANK = {"none": 0, "minor": 1, "major": 2, "critical": 3, "unknown": -1}


@dataclass(frozen=True)
class Observation:
    service_name: str
    component_name: str
    event_id: str
    started_at: str
    updated_at: str
    state: str
    impact: str
    summary: str
    source_ref: str
    raw_hash: str

    @property
    def event_key(self) -> str:
        return "|".join((self.service_name, self.component_name, self.event_id))


@dataclass(frozen=True)
class IncidentWindow:
    started_at: str
    ended_at: str | None  # None means still open given current evidence


@dataclass
class Incident:
    event_id: str
    service_name: str
    component_name: str
    windows: list[IncidentWindow] = field(default_factory=list)
    max_impact: str = "unknown"
    latest_state: str = "unknown"
    latest_updated_at: str = ""
    observations: list[Observation] = field(default_factory=list)
    unknown_states: list[str] = field(default_factory=list)

    def active_at(self, as_of: str) -> bool:
        for window in self.windows:
            if not is_at_or_before(window.started_at, as_of):
                continue
            if window.ended_at is None or is_at_or_before(as_of, window.ended_at):
                return True
        return False

    def label_at(self, as_of: str) -> dict:
        """Best-evidence label known for this incident at instant ``as_of``.

        ``as_of_label_uses_late_evidence`` is true when the reconstruction
        relies on at least one observation whose ``updated_at`` is later than
        the queried instant: either no observation had arrived yet (backfilled
        incident) or later corrections changed the picture.  Per-observation
        late flags are attached separately by the engine.
        """
        visible = [obs for obs in self.observations if is_at_or_before(obs.updated_at, as_of)]
        any_late = len(visible) < len(self.observations)
        if not visible:
            visible = list(self.observations)
        latest = max(visible, key=lambda obs: obs.updated_at)
        impact = max(
            (obs.impact for obs in visible if obs.impact in _IMPACT_RANK and _IMPACT_RANK[obs.impact] >= 0),
            key=lambda value: _IMPACT_RANK[value],
            default="unknown",
        )
        return {
            "state": latest.state,
            "impact": impact,
            "summary": latest.summary,
            "as_of_label_uses_late_evidence": any_late,
        }


def reconstruct_incidents(observations: list[Observation]) -> list[Incident]:
    grouped: dict[str, list[Observation]] = {}
    for obs in observations:
        grouped.setdefault(obs.event_id, []).append(obs)

    incidents: list[Incident] = []
    for event_id, group in grouped.items():
        ordered = sorted(group, key=lambda obs: (obs.updated_at, obs.source_ref))
        first = ordered[0]
        incident = Incident(
            event_id=event_id,
            service_name=first.service_name,
            component_name=first.component_name,
            latest_state=ordered[-1].state,
            latest_updated_at=ordered[-1].updated_at,
            observations=ordered,
        )

        windows: list[IncidentWindow] = []
        open_start: str | None = None
        for obs in ordered:
            if obs.state not in ("investigating", "identified", "monitoring", "resolved"):
                incident.unknown_states.append(obs.state)
            if obs.state != _RESOLVED_STATE:
                if open_start is None:
                    open_start = obs.started_at
            elif open_start is not None:
                windows.append(IncidentWindow(open_start, obs.updated_at))
                open_start = None
        if open_start is not None:
            windows.append(IncidentWindow(open_start, None))
        incident.windows = windows

        known_impacts = [obs.impact for obs in ordered if obs.impact in _IMPACT_RANK and _IMPACT_RANK[obs.impact] >= 0]
        incident.max_impact = max(known_impacts, key=lambda value: _IMPACT_RANK[value], default="unknown")
        incidents.append(incident)
    return sorted(incidents, key=lambda item: (item.service_name, item.event_id))


def unknown_state_reason(incident: Incident) -> dict | None:
    if not incident.unknown_states:
        return None
    return {
        "code": REASON_UNKNOWN_STATE,
        "detail": f"incident {incident.event_id!r} carries unrecognized state(s): {sorted(set(incident.unknown_states))}",
        "event_id": incident.event_id,
    }
