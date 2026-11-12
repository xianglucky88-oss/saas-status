"""Pure impact computation engine.

Combines a topology snapshot valid at the observation instant with incidents
reconstructed from all currently known observations.  The function never
touches the database, which makes it reusable for both on-the-fly queries and
resumable batch runs.
"""
from __future__ import annotations

from .clock import is_at_or_before
from .incidents import Incident, unknown_state_reason
from .topology import (
    REASON_CYCLE_DEPENDENCY,
    REASON_DANGLING_REFERENCE,
    REASON_NO_TOPOLOGY,
    REASON_UNRESOLVED_NAME,
    TopologyGraph,
    TopologySnapshot,
)

_IMPACT_RANK = {"none": 0, "unknown": 0, "minor": 1, "major": 2, "critical": 3}
_STATUS_BY_IMPACT = {
    "none": "operational",
    "minor": "degraded",
    "major": "degraded",
    "critical": "critical",
    "unknown": "degraded",
}


def _node_refs(graph: TopologyGraph, path: list[str]) -> list[dict]:
    snapshot = graph.snapshot
    return [
        {
            "node_id": node_id,
            "name": snapshot.name_of(node_id) or node_id,
            "kind": snapshot.kind_of(node_id) or "unknown",
        }
        for node_id in path
    ]


def _undetermined(service_name: str, as_of: str, reasons: list[dict],
                  snapshot: TopologySnapshot | None = None,
                  contributions: list[dict] | None = None) -> dict:
    return {
        "service_name": service_name,
        "as_of": as_of,
        "status": "undetermined",
        "impact_level": "unknown",
        "topology_generation_id": snapshot.generation_id if snapshot else None,
        "contributing_incidents": contributions or [],
        "undetermined_reasons": reasons,
    }


def compute_service_status(service_name: str, as_of: str,
                           snapshot: TopologySnapshot | None,
                           incidents: list[Incident]) -> dict:
    """Answer: which components/incidents affect ``service_name`` at ``as_of``."""
    wanted = service_name.strip().lower()
    if snapshot is None:
        return _undetermined(wanted, as_of, [
            {"code": REASON_NO_TOPOLOGY, "detail": "no topology generation is effective at the requested time"}
        ])

    graph = TopologyGraph(snapshot)
    service_resolution = graph.resolve_name(wanted, "service")
    if service_resolution.reason is not None or not service_resolution.node_ids:
        return _undetermined(wanted, as_of, [
            {
                "code": REASON_UNRESOLVED_NAME,
                "detail": service_resolution.detail or f"service {service_name!r} cannot be located",
            }
        ], snapshot)
    service_id = next(iter(service_resolution.node_ids))

    reasons: list[dict] = []

    for cycle in graph.services_touching_cycles().get(service_id, []):
        reasons.append({
            "code": REASON_CYCLE_DEPENDENCY,
            "detail": f"dependency cycle reachable from service {service_id!r}: {cycle}",
            "cycle_node_ids": cycle,
        })
    reasons.extend(graph.dangling_reasons_for_service(service_id))

    contributions: list[dict] = []
    active_impacts: list[str] = []

    for incident in incidents:
        if not incident.active_at(as_of):
            continue
        reported_for_this_service = incident.service_name.strip().lower() == wanted
        component_resolution = graph.resolve_name(incident.component_name, "component")
        if component_resolution.reason is not None or not component_resolution.node_ids:
            # An unresolvable component is only reportable against the service
            # named on the event itself; topology-based propagation is impossible.
            if reported_for_this_service:
                reasons.append({
                    "code": REASON_UNRESOLVED_NAME,
                    "detail": (component_resolution.detail or
                               f"component {incident.component_name!r} cannot be located"),
                    "event_id": incident.event_id,
                })
            continue
        component_id = next(iter(component_resolution.node_ids))

        affected = graph.affected_services(component_id)
        if service_id not in affected and not reported_for_this_service:
            continue

        raw_paths = graph.impact_paths(component_id, service_id)
        if not raw_paths:
            # The source reported the event directly against this service but
            # the topology only knows an ownership attribution.
            raw_paths = [[component_id, service_id]]
        paths = [_node_refs(graph, path) for path in raw_paths]

        label = incident.label_at(as_of)
        active_window = next(window for window in incident.windows
                             if is_at_or_before(window.started_at, as_of)
                             and (window.ended_at is None or is_at_or_before(as_of, window.ended_at)))
        evidence = [
            {
                "updated_at": obs.updated_at,
                "state": obs.state,
                "impact": obs.impact,
                "source_ref": obs.source_ref,
                "raw_hash": obs.raw_hash,
                "late_evidence": not is_at_or_before(obs.updated_at, as_of),
            }
            for obs in incident.observations
        ]
        contributions.append({
            "event_id": incident.event_id,
            "component_id": component_id,
            "component_name": snapshot.name_of(component_id) or incident.component_name,
            "component_name_at_event": incident.component_name,
            "name_rewritten_by_generation": (
                snapshot.name_of(component_id) is not None
                and snapshot.name_of(component_id).strip().lower() != incident.component_name.strip().lower()
            ),
            "state": label["state"],
            "impact": label["impact"],
            "summary": label["summary"],
            "label_uses_late_evidence": label["as_of_label_uses_late_evidence"],
            "window": {"started_at": active_window.started_at, "ended_at": active_window.ended_at},
            "impact_paths": paths,
            "evidence_observations": evidence,
        })
        if label["impact"] in _IMPACT_RANK and label["impact"] != "unknown":
            active_impacts.append(label["impact"])

        state_reason = unknown_state_reason(incident)
        if state_reason is not None:
            reasons.append(state_reason)

    if reasons:
        return _undetermined(wanted, as_of, reasons, snapshot, contributions)

    if not contributions:
        impact_level = "none"
    else:
        impact_level = max(active_impacts, key=lambda value: _IMPACT_RANK[value], default="unknown")
    return {
        "service_name": wanted,
        "service_id": service_id,
        "as_of": as_of,
        "status": _STATUS_BY_IMPACT[impact_level],
        "impact_level": impact_level,
        "topology_generation_id": snapshot.generation_id,
        "contributing_incidents": contributions,
        "undetermined_reasons": [],
    }
