"""Service-status query facade and resumable compute runs.

Two entry points share the same engine:

* :func:`query_service` evaluates a service at an observation instant directly
  from stored topology + observations (no pre-computation needed);
* :func:`run_compute_batch` materializes the same results into run/task/result
  tables, one committed task at a time, so an interrupted process resumes by
  skipping finished tasks and re-claiming failed/pending ones.
"""
from __future__ import annotations

import uuid
from collections.abc import Callable

from .clock import now_iso_z, to_iso_z
from .engine import compute_service_status
from .impact_storage import ImpactStore, canonical_hash
from .incidents import Observation, reconstruct_incidents
from .normalize import normalize_event
from .topology import TopologyDocument

_TaskHook = Callable[[str, str, str], None]


def observation_from_raw(raw: dict) -> Observation:
    """Reuse the legacy normalizer so old and new inputs share one mapping."""
    event = normalize_event(raw)
    payload = {
        "service": event.service, "component": event.component, "event_id": event.event_id,
        "started_at": event.started_at, "updated_at": event.updated_at, "state": event.state,
        "impact": event.impact, "summary": event.summary, "source_ref": event.source_ref,
    }
    return Observation(
        event.service, event.component, event.event_id,
        to_iso_z(event.started_at), to_iso_z(event.updated_at),
        event.state, event.impact, event.summary, event.source_ref,
        canonical_hash(payload),
    )


def ingest_events(store: ImpactStore, raw_events: list[dict]) -> dict:
    return store.save_observations([observation_from_raw(item) for item in raw_events])


def ingest_topology(store: ImpactStore, raw_document: dict) -> dict:
    document = TopologyDocument.from_raw(raw_document, canonical_hash(raw_document))
    return store.save_topology(document)


def _build_context(store: ImpactStore, as_of: str):
    moment = to_iso_z(as_of)
    snapshot, generation_ids = store.load_snapshot(moment)
    incidents = reconstruct_incidents(store.load_observations())
    return snapshot, incidents, generation_ids


def query_service(store: ImpactStore, service_name: str, as_of: str) -> dict:
    snapshot, incidents, _ = _build_context(store, as_of)
    result = compute_service_status(service_name, to_iso_z(as_of), snapshot, incidents)
    result["queried_at"] = now_iso_z()
    return result


def query_all_services(store: ImpactStore, as_of: str) -> list[dict]:
    moment = to_iso_z(as_of)
    snapshot, incidents, _ = _build_context(store, moment)
    if snapshot is None:
        return [compute_service_status("", moment, None, incidents)]
    service_names = sorted({node.name for node in snapshot.nodes.values() if node.kind == "service"})
    return [compute_service_status(name, moment, snapshot, incidents) for name in service_names]


def run_compute_batch(store: ImpactStore, services: list[str], as_of: str,
                      run_id: str | None = None,
                      before_task: _TaskHook | None = None) -> dict:
    """Compute (service, as_of) results with per-task commit and resume support."""
    moment = to_iso_z(as_of)
    run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
    store.create_run(run_id, {"services": [s.strip().lower() for s in services], "as_of": moment})

    snapshot, incidents, _ = _build_context(store, moment)
    while True:
        task = store.claim_pending_task(run_id)
        if task is None:
            break
        service_name = task["service_name"]
        if before_task is not None:
            before_task(run_id, service_name, moment)
        try:
            result = compute_service_status(service_name, moment, snapshot, incidents)
            result["run_id"] = run_id
            store.complete_task(run_id, service_name, moment, result)
        except Exception as exc:  # persisted as a failed task, never kills the run
            store.fail_task(run_id, service_name, moment, f"{type(exc).__name__}: {exc}")
    summary = store.finish_run(run_id)
    summary["results"] = [
        store.load_result(run_id, service_name, moment)
        for service_name in sorted(s.strip().lower() for s in services)
    ]
    return summary


def resume_run(store: ImpactStore, run_id: str, before_task: _TaskHook | None = None) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise KeyError(f"unknown run {run_id!r}")
    return run_compute_batch(
        store,
        [task["service_name"] for task in run["tasks"]],
        run["request"]["as_of"],
        run_id=run_id,
        before_task=before_task,
    )
