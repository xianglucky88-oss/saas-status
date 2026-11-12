#!/usr/bin/env python3
"""Offline end-to-end demonstration.

Scenario timeline (all times UTC):

* Topology v1 effective 2026-11-09: Web Shop / Checkout / Customer Portal, with
  API Gateway shared by all three services and Billing shared by Web Shop and
  Checkout.
* 2026-11-10 02:00-08:00  INC-100 major incident on Billing (recovered 08:00).
* 2026-11-10 04:00 ->       INC-200 critical incident on API Gateway, still open
                           when the first batch is collected: the two incidents
                           overlap between 04:00 and 08:00.
* A batch compute run is interrupted mid-way and then resumed.
* Topology v2 effective 2026-11-11 renames components (Billing -> Settlement,
  API Gateway -> Edge Gateway). Historical queries must keep v1 names.
* A late feeder backfills INC-300 on Auth (started 11-10 01:30) and a night-ops
  report closes INC-200 on 11-11 10:00. Windows are corrected, other incidents
  are not erased.

Run with:  PYTHONPATH=src python3 scripts/demo.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from saas_status.impact import (  # noqa: E402
    ingest_events,
    ingest_topology,
    query_all_services,
    query_service,
    resume_run,
    run_compute_batch,
)
from saas_status.impact_storage import ImpactStore  # noqa: E402
from saas_status.pipeline import StatusPipeline  # noqa: E402
from saas_status.sources import JsonStatusSource  # noqa: E402
from saas_status.storage import StatusStore  # noqa: E402

FIXTURES = ROOT / "fixtures"
DEMO = FIXTURES / "demo"
SERVICES = ["Web Shop", "Checkout", "Customer Portal"]


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def show(result: dict) -> None:
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=False))


def compact(result: dict) -> dict:
    return {
        "service": result["service_name"],
        "status": result["status"],
        "impact_level": result["impact_level"],
        "generation": result.get("topology_generation_id"),
        "incidents": [
            {
                "event_id": item["event_id"],
                "component": item["component_name"],
                "state": item["state"],
                "impact": item["impact"],
                "path": " -> ".join(node["name"] for node in item["impact_paths"][0])
                if item["impact_paths"] else None,
            }
            for item in result["contributing_incidents"]
        ],
        "undetermined_reasons": [reason["code"] for reason in result.get("undetermined_reasons", [])],
    }


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="saas-demo-"))
    database = workdir / "demo.sqlite3"
    print(f"demo database: {database}")
    store = ImpactStore(database)

    banner("1. Legacy pipeline still works (old fixture, old merged-event table)")
    legacy_store = StatusStore(database)
    legacy_events = StatusPipeline(JsonStatusSource(FIXTURES / "status.json"), legacy_store).run()
    legacy_store.close()
    print(f"legacy merged events stored: {len(legacy_events)}")

    banner("2. Ingest topology v1 and the first event batch")
    print(ingest_topology(store, json.loads((DEMO / "topology_v1.json").read_text())))
    print(ingest_events(store, json.loads((DEMO / "events_initial.json").read_text())))

    banner("3. Historical query at 2026-11-10T06:00 — two overlapping incidents")
    for result in query_all_services(store, "2026-11-10T06:00:00Z"):
        print(json.dumps(compact(result), ensure_ascii=False))

    banner("4. Partial recovery at 09:00 — Billing healed, Gateway still down")
    show(compact(query_service(store, "Checkout", "2026-11-10T09:00:00Z")))

    banner("5. Batch run interrupted after the first task, then resumed")
    attempts = {"n": 0}

    def crash_after_first(run_id: str, service_name: str, as_of: str) -> None:
        attempts["n"] += 1
        if attempts["n"] == 2:
            print(f"[simulated process killed while computing {service_name}]")
            raise RuntimeError("simulated process kill")

    try:
        run_compute_batch(store, SERVICES, "2026-11-10T06:00:00Z",
                          run_id="run-incident-day", before_task=crash_after_first)
    except RuntimeError as exc:
        print(f"batch aborted: {exc}")
    print("task ledger at crash time:")
    print(json.dumps(store.get_run("run-incident-day")["tasks"], ensure_ascii=False, indent=2))
    summary = resume_run(store, "run-incident-day")
    print(f"resumed run status: {summary['status']}, task counts: {summary['tasks']}")

    banner("6. Topology v2 takes over on 2026-11-11 (component renames)")
    print(ingest_topology(store, json.loads((DEMO / "topology_v2.json").read_text())))
    historical = query_service(store, "Checkout", "2026-11-10T03:00:00Z")
    current = query_service(store, "Web Shop", "2026-11-11T03:00:00Z")
    print("historical (v1 names):", json.dumps(compact(historical), ensure_ascii=False))
    print("current   (v2 names):", json.dumps(compact(current), ensure_ascii=False))

    banner("7. Late messages arrive: backfilled INC-300 and late INC-200 recovery")
    print(ingest_events(store, json.loads((DEMO / "events_late.json").read_text())))
    before_recovery = query_service(store, "Web Shop", "2026-11-11T09:00:00Z")
    after_recovery = query_service(store, "Web Shop", "2026-11-11T11:00:00Z")
    print("just before the late recovery timestamp:", json.dumps(compact(before_recovery), ensure_ascii=False))
    print("after the recovery window closes:       ", json.dumps(compact(after_recovery), ensure_ascii=False))

    banner("8. Evidence trail for the backfilled INC-300 at a historical instant")
    portal_history = query_service(store, "Customer Portal", "2026-11-10T02:00:00Z")
    incident_300 = next(
        item for item in portal_history["contributing_incidents"] if item["event_id"] == "INC-300"
    )
    show(incident_300)

    banner("9. Undetermined reasons: dangling reference, cycle, unknown state, bad name")
    ingest_topology(store, json.loads((DEMO / "problems" / "topology.json").read_text()))
    ingest_events(store, json.loads((DEMO / "problems" / "events.json").read_text()))
    for service in ("Service X", "Service Y"):
        print(json.dumps(compact(query_service(store, service, "2026-11-05T02:00:00Z")),
                         ensure_ascii=False, indent=2))

    store.close()
    banner(f"done — inspect {database} with the query / run-status subcommands")


if __name__ == "__main__":
    main()
