import sqlite3
import tempfile
import unittest
from pathlib import Path

from saas_status.cli import main
from saas_status.engine import compute_service_status
from saas_status.impact import (
    ingest_events,
    ingest_topology,
    query_all_services,
    query_service,
    resume_run,
    run_compute_batch,
)
from saas_status.impact_storage import ImpactStore
from saas_status.incidents import reconstruct_incidents
from saas_status.topology import (
    REASON_CYCLE_DEPENDENCY,
    REASON_DANGLING_REFERENCE,
    REASON_UNKNOWN_STATE,
    REASON_UNRESOLVED_NAME,
    TopologyDocument,
    TopologyGraph,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "demo"


def load_json(path: Path):
    import json

    return json.loads(path.read_text(encoding="utf-8"))


class ImpactStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "impact.sqlite3"
        self.store = ImpactStore(self.db)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def seed(self) -> None:
        ingest_topology(self.store, load_json(FIXTURES / "topology_v1.json"))
        ingest_events(self.store, load_json(FIXTURES / "events_initial.json"))

    def test_ingestion_is_idempotent(self):
        self.seed()
        again_topology = ingest_topology(self.store, load_json(FIXTURES / "topology_v1.json"))
        again_events = ingest_events(self.store, load_json(FIXTURES / "events_initial.json"))
        self.assertFalse(again_topology["changed"])
        self.assertEqual(again_events["inserted"], 0)
        self.assertEqual(again_events["duplicated"], 6)

    def test_new_generation_closes_old_rows_but_keeps_history(self):
        self.seed()
        ingest_topology(self.store, load_json(FIXTURES / "topology_v2.json"))
        rows = self.store.connection.execute(
            "SELECT generation_id, name, effective_until FROM topology_nodes "
            "WHERE node_id = 'comp-billing' ORDER BY generation_id"
        ).fetchall()
        self.assertEqual([dict(r)["generation_id"] for r in rows], ["topo-2026w45", "topo-2026w46"])
        self.assertIsNotNone(rows[0]["effective_until"])
        self.assertIsNone(rows[1]["effective_until"])
        # Historical name survives the rename.
        self.assertEqual(rows[0]["name"], "Billing")
        self.assertEqual(rows[1]["name"], "Settlement")

    def test_overlapping_incidents_and_partial_recovery(self):
        self.seed()
        # 06:00: both incidents active -> Checkout impacted via both gateway and billing.
        result = query_service(self.store, "Checkout", "2026-11-10T06:00:00Z")
        self.assertEqual(result["status"], "critical")
        event_ids = sorted(item["event_id"] for item in result["contributing_incidents"])
        self.assertEqual(event_ids, ["INC-100", "INC-200"])
        for item in result["contributing_incidents"]:
            self.assertTrue(item["impact_paths"], "every contribution must carry a component path")

        # 09:00: billing recovered but gateway still down -> service must NOT look healthy.
        result_after = query_service(self.store, "Checkout", "2026-11-10T09:00:00Z")
        self.assertEqual(result_after["status"], "critical")
        self.assertEqual([i["event_id"] for i in result_after["contributing_incidents"]], ["INC-200"])

        healthy = query_service(self.store, "Checkout", "2026-11-09T00:00:00Z")
        self.assertEqual(healthy["status"], "operational")
        self.assertEqual(healthy["contributing_incidents"], [])

    def test_shared_component_hits_all_dependent_services(self):
        self.seed()
        webshop = query_service(self.store, "Web Shop", "2026-11-10T06:00:00Z")
        checkout = query_service(self.store, "Checkout", "2026-11-10T06:00:00Z")
        portal = query_service(self.store, "Customer Portal", "2026-11-10T06:00:00Z")
        for result in (webshop, checkout, portal):
            self.assertIn(
                "INC-200",
                [item["event_id"] for item in result["contributing_incidents"]],
            )
        # Portal has no billing dependency: INC-100 must not propagate there.
        self.assertNotIn(
            "INC-100",
            [item["event_id"] for item in portal["contributing_incidents"]],
        )

    def test_topology_generation_switch_changes_names_not_history(self):
        self.seed()
        ingest_topology(self.store, load_json(FIXTURES / "topology_v2.json"))
        # Historical query resolves names through the generation effective then.
        historical = query_service(self.store, "Checkout", "2026-11-10T03:00:00Z")
        self.assertEqual(historical["topology_generation_id"], "topo-2026w45")
        historical_names = {
            item["component_name"] for item in historical["contributing_incidents"]
        }
        self.assertIn("Billing", historical_names)

        # Same component id under the new generation reports its new name; the
        # v1 row is preserved untouched and still drives historical queries.
        current = query_service(self.store, "Web Shop", "2026-11-11T03:00:00Z")
        self.assertEqual(current["topology_generation_id"], "topo-2026w46")
        gateway = next(
            item for item in current["contributing_incidents"] if item["event_id"] == "INC-200"
        )
        self.assertEqual(gateway["component_name"], "Edge Gateway")
        self.assertEqual(gateway["component_id"], "comp-api-gateway")

        rows = self.store.connection.execute(
            "SELECT name FROM topology_nodes WHERE node_id = 'comp-api-gateway' ORDER BY generation_id"
        ).fetchall()
        self.assertEqual([row["name"] for row in rows], ["API Gateway", "Edge Gateway"])

    def test_historical_event_name_resolves_across_rename(self):
        self.seed()
        ingest_topology(self.store, load_json(FIXTURES / "topology_v2.json"))
        # Under v2 the event's old component name "api gateway" must still bind
        # to the same stable component id instead of becoming undetermined.
        current = query_service(self.store, "Web Shop", "2026-11-11T03:00:00Z")
        self.assertEqual(current["undetermined_reasons"], [])
        gateway = next(
            item for item in current["contributing_incidents"] if item["event_id"] == "INC-200"
        )
        self.assertTrue(gateway["name_rewritten_by_generation"])
        self.assertEqual(gateway["component_name_at_event"], "api gateway")
        self.assertEqual(gateway["component_id"], "comp-api-gateway")

    def test_late_messages_correct_windows_without_erasing_others(self):
        self.seed()
        ingest_events(self.store, load_json(FIXTURES / "events_late.json"))
        # INC-300 started at 01:30 but was only reported later; the backfilled
        # window is active at 02:00, where none of its observations had arrived.
        portal = query_service(self.store, "Customer Portal", "2026-11-10T02:00:00Z")
        self.assertIn(
            "INC-300",
            [item["event_id"] for item in portal["contributing_incidents"]],
        )
        backfilled = next(
            item for item in portal["contributing_incidents"] if item["event_id"] == "INC-300"
        )
        # Reconstruction at 02:00 already depends on later evidence (the 09:00
        # monitoring observation), even though one earlier observation had arrived.
        self.assertTrue(backfilled["label_uses_late_evidence"])
        per_observation = {obs["updated_at"]: obs["late_evidence"]
                           for obs in backfilled["evidence_observations"]}
        self.assertEqual(per_observation["2026-11-10T01:30:00Z"], False)
        self.assertEqual(per_observation["2026-11-10T09:00:00Z"], True)
        # At 10:00 the 01:30/09:00 observations had arrived, but nothing newer:
        # the contribution is no longer flagged late at that instant.
        later_portal = query_service(self.store, "Customer Portal", "2026-11-10T10:00:00Z")
        later_incident = next(
            item for item in later_portal["contributing_incidents"] if item["event_id"] == "INC-300"
        )
        self.assertFalse(later_incident["label_uses_late_evidence"])

        # Late INC-200 recovery closes only its own window at 11-11 10:00.
        still_degraded = query_service(self.store, "Web Shop", "2026-11-11T09:00:00Z")
        recovered = query_service(self.store, "Web Shop", "2026-11-11T11:00:00Z")
        self.assertEqual(still_degraded["status"], "critical")
        self.assertEqual(recovered["status"], "operational")
        # At 11-10 06:00 the recovery message had not arrived: flag and window
        # reconstruction still show the incident active there.
        history = query_service(self.store, "Checkout", "2026-11-10T06:00:00Z")
        gateway = next(item for item in history["contributing_incidents"] if item["event_id"] == "INC-200")
        self.assertTrue(
            any(obs["late_evidence"] and obs["state"] == "resolved"
                for obs in gateway["evidence_observations"])
        )
        self.assertEqual(
            sorted(item["event_id"] for item in history["contributing_incidents"]),
            ["INC-100", "INC-200"],
        )

    def test_undetermined_reasons_are_identifiable(self):
        ingest_topology(self.store, load_json(FIXTURES / "problems" / "topology.json"))
        ingest_events(self.store, load_json(FIXTURES / "problems" / "events.json"))

        service_x = query_service(self.store, "Service X", "2026-11-05T02:00:00Z")
        self.assertEqual(service_x["status"], "undetermined")
        codes = {reason["code"] for reason in service_x["undetermined_reasons"]}
        self.assertIn(REASON_DANGLING_REFERENCE, codes)
        self.assertIn(REASON_UNRESOLVED_NAME, codes)

        service_y = query_service(self.store, "Service Y", "2026-11-05T02:00:00Z")
        y_codes = {reason["code"] for reason in service_y["undetermined_reasons"]}
        self.assertIn(REASON_CYCLE_DEPENDENCY, y_codes)
        self.assertIn(REASON_UNKNOWN_STATE, y_codes)

        unknown_service = query_service(self.store, "Ghost Service", "2026-11-05T02:00:00Z")
        self.assertEqual(unknown_service["status"], "undetermined")
        self.assertEqual(unknown_service["undetermined_reasons"][0]["code"], REASON_UNRESOLVED_NAME)

    def test_query_without_topology_is_undetermined(self):
        ingest_events(self.store, load_json(FIXTURES / "events_initial.json"))
        result = query_service(self.store, "Checkout", "2026-11-10T06:00:00Z")
        self.assertEqual(result["status"], "undetermined")
        self.assertEqual(result["undetermined_reasons"][0]["code"], "no_topology")

    def test_resumable_batch_run_skips_finished_tasks(self):
        self.seed()
        services = ["Web Shop", "Checkout", "Customer Portal"]
        as_of = "2026-11-10T06:00:00Z"
        crashed = {"count": 0}

        def crash_once(run_id, service_name, moment):
            crashed["count"] += 1
            if crashed["count"] == 2:
                raise RuntimeError("simulated crash mid-batch")

        with self.assertRaises(RuntimeError):
            run_compute_batch(self.store, services, as_of, run_id="run-demo", before_task=crash_once)

        run_view = self.store.get_run("run-demo")
        statuses = {task["service_name"]: task["status"] for task in run_view["tasks"]}
        self.assertIn("running", statuses.values())
        self.assertEqual(sum(1 for value in statuses.values() if value == "done"), 1)

        # Resume: done tasks are skipped, the failed one is retried and completed.
        summary = resume_run(self.store, "run-demo")
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(len(summary["results"]), 3)
        self.assertTrue(all(result is not None for result in summary["results"]))

        # Stored result equals an on-the-fly query for the same instant.
        stored = self.store.load_result("run-demo", "checkout", as_of)
        live = query_service(self.store, "Checkout", as_of)
        self.assertEqual(
            stored["contributing_incidents"], live["contributing_incidents"]
        )

    def test_query_all_services_returns_full_surface(self):
        self.seed()
        results = query_all_services(self.store, "2026-11-10T06:00:00Z")
        self.assertEqual(len(results), 3)
        self.assertTrue(all("impact_paths" in incident for result in results
                            for incident in result["contributing_incidents"]))


class GraphEngineUnitTest(unittest.TestCase):
    def test_cycle_detection_and_path_enumeration(self):
        raw = load_json(FIXTURES / "problems" / "topology.json")
        document = TopologyDocument.from_raw(raw, "hash")
        from saas_status.topology import TopologySnapshot

        snapshot = TopologySnapshot(
            "2026-11-05T00:00:00Z", document.generation_id,
            {node.node_id: node for node in document.nodes}, document.edges,
        )
        graph = TopologyGraph(snapshot)
        cycles = graph.cyclic_components()
        self.assertEqual(cycles, [["comp-a", "comp-b"]])
        self.assertTrue(graph.dangling_edges)

    def test_ambiguous_name_across_generations_stays_undetermined(self):
        raw_v1 = {
            "generation_id": "g1", "effective_from": "2026-11-01T00:00:00Z",
            "nodes": [
                {"id": "svc-1", "kind": "service", "name": "Core"},
                {"id": "comp-1", "kind": "component", "name": "Shared Name"},
            ],
            "edges": [{"kind": "owns", "source": "svc-1", "target": "comp-1"}],
        }
        raw_v2 = {
            "generation_id": "g2", "effective_from": "2026-11-05T00:00:00Z",
            "nodes": [
                {"id": "svc-1", "kind": "service", "name": "Core"},
                {"id": "comp-1", "kind": "component", "name": "Shared Name"},
                {"id": "comp-2", "kind": "component", "name": "Shared Name"},
            ],
            "edges": [
                {"kind": "owns", "source": "svc-1", "target": "comp-1"},
                {"kind": "owns", "source": "svc-1", "target": "comp-2"},
            ],
        }
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = ImpactStore(Path(tmp.name) / "amb.sqlite3")
        ingest_topology(store, raw_v1)
        ingest_topology(store, raw_v2)
        events = [{
            "service": "Core", "component": "Shared Name", "event_id": "INC-1",
            "started_at": "2026-11-06T00:00:00Z", "updated_at": "2026-11-06T00:30:00Z",
            "state": "investigating", "impact": "minor", "summary": "ambiguous", "source_ref": "s",
        }]
        ingest_events(store, events)
        result = query_service(store, "Core", "2026-11-06T01:00:00Z")
        self.assertEqual(result["status"], "undetermined")
        self.assertTrue(
            any(reason["code"] == REASON_UNRESOLVED_NAME for reason in result["undetermined_reasons"])
        )
        store.close()

    def test_reconstruct_incidents_reopens_after_resolution(self):
        store_db = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        store_db.close()
        store = ImpactStore(store_db.name)
        try:
            ingest_events(store, load_json(FIXTURES / "events_initial.json"))
            incidents = {item.event_id: item for item in reconstruct_incidents(store.load_observations())}
        finally:
            store.close()
        self.assertEqual(len(incidents["INC-100"].windows), 1)
        self.assertIsNotNone(incidents["INC-100"].windows[0].ended_at)
        self.assertEqual(len(incidents["INC-200"].windows), 1)
        self.assertIsNone(incidents["INC-200"].windows[0].ended_at)
        self.assertTrue(incidents["INC-200"].active_at("2026-11-12T00:00:00Z"))

    def test_resolved_then_reopened_incident_has_two_windows(self):
        flapping = [
            {"service": "Checkout", "component": "Billing", "event_id": "INC-F",
             "started_at": "2026-11-10T00:00:00Z", "updated_at": "2026-11-10T01:00:00Z",
             "state": "investigating", "impact": "minor", "summary": "first", "source_ref": "s"},
            {"service": "Checkout", "component": "Billing", "event_id": "INC-F",
             "started_at": "2026-11-10T00:00:00Z", "updated_at": "2026-11-10T02:00:00Z",
             "state": "resolved", "impact": "minor", "summary": "closed", "source_ref": "s"},
            {"service": "Checkout", "component": "Billing", "event_id": "INC-F",
             "started_at": "2026-11-10T05:00:00Z", "updated_at": "2026-11-10T05:30:00Z",
             "state": "investigating", "impact": "major", "summary": "regression", "source_ref": "s"},
        ]
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = ImpactStore(Path(tmp.name) / "flap.sqlite3")
        ingest_events(store, flapping)
        try:
            incidents = reconstruct_incidents(store.load_observations())
        finally:
            store.close()
        incident = next(item for item in incidents if item.event_id == "INC-F")
        self.assertEqual(len(incident.windows), 2)
        self.assertIsNone(incident.windows[1].ended_at)
        self.assertFalse(incident.active_at("2026-11-10T03:00:00Z"))
        self.assertTrue(incident.active_at("2026-11-10T06:00:00Z"))
        self.assertEqual(incident.max_impact, "major")


class LegacyCompatibilityTest(unittest.TestCase):
    def test_legacy_cli_and_event_query_still_work(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = str(Path(tmp.name) / "legacy.sqlite3")
        main(["--fixture", str(Path(__file__).resolve().parent.parent / "fixtures" / "status.json"),
              "--database", db])
        connection = sqlite3.connect(db)
        try:
            count = connection.execute("SELECT COUNT(*) FROM status_events").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 1)
        # New tables coexist in the same database file.
        impact = ImpactStore(db)
        try:
            self.assertEqual(impact.load_observations(), [])
        finally:
            impact.close()


if __name__ == "__main__":
    unittest.main()
