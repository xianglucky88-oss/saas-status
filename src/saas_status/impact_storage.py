"""Persistence for versioned topology, raw event observations and impact runs.

The legacy :class:`~saas_status.storage.StatusStore` table (``status_events``)
is left untouched and continues to power the existing event query.  Everything
added here is append-only or upserted by a stable natural key, so:

* re-ingesting the same topology document / observation is a no-op;
* a compute run persists one task per (run, service, as_of) and skips finished
  tasks on resume;
* every stored result references the observation hashes and topology generation
  that produced it, keeping the evidence trail queryable.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from .clock import now_iso_z, to_iso_z
from .incidents import Observation
from .topology import EdgeVersion, NodeVersion, TopologyDocument, TopologySnapshot

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topology_generations (
    generation_id TEXT PRIMARY KEY,
    effective_from TEXT NOT NULL,
    effective_until TEXT,
    raw_hash TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS topology_nodes (
    generation_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_until TEXT,
    PRIMARY KEY (generation_id, node_id)
);
CREATE TABLE IF NOT EXISTS topology_edges (
    generation_id TEXT NOT NULL,
    edge_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_until TEXT,
    PRIMARY KEY (generation_id, edge_id)
);
CREATE TABLE IF NOT EXISTS event_observations (
    event_key TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    service_name TEXT NOT NULL,
    component_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    state TEXT NOT NULL,
    impact TEXT NOT NULL,
    summary TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    PRIMARY KEY (event_key, updated_at, source_ref)
);
CREATE TABLE IF NOT EXISTS compute_runs (
    run_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS compute_tasks (
    run_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    as_of TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    PRIMARY KEY (run_id, service_name, as_of)
);
CREATE TABLE IF NOT EXISTS service_status_results (
    run_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    as_of TEXT NOT NULL,
    result_json TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    topology_generation_id TEXT,
    PRIMARY KEY (run_id, service_name, as_of)
);
CREATE INDEX IF NOT EXISTS idx_observations_event ON event_observations(event_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON topology_edges(generation_id, target_id);
CREATE INDEX IF NOT EXISTS idx_results_lookup ON service_status_results(service_name, as_of);
"""


def canonical_hash(payload: object) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class ImpactStore:
    def __init__(self, database: str | Path) -> None:
        self.connection = sqlite3.connect(database)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(_SCHEMA)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    # ------------------------------------------------------------ topology
    def save_topology(self, document: TopologyDocument) -> dict:
        """Idempotently persist one generation, closing superseded open rows."""
        conn = self.connection
        existing = conn.execute(
            "SELECT generation_id FROM topology_generations WHERE generation_id = ?",
            (document.generation_id,),
        ).fetchone()
        if existing is not None:
            return {"generation_id": document.generation_id, "changed": False}

        with conn:
            conn.execute(
                "INSERT INTO topology_generations VALUES (?, ?, ?, ?, ?)",
                (
                    document.generation_id,
                    to_iso_z(document.effective_from),
                    to_iso_z(document.effective_until) if document.effective_until else None,
                    document.raw_hash,
                    now_iso_z(),
                ),
            )
            conn.executemany(
                "INSERT INTO topology_nodes VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        n.generation_id, n.node_id, n.kind, n.name,
                        to_iso_z(n.effective_from),
                        to_iso_z(n.effective_until) if n.effective_until else None,
                    )
                    for n in document.nodes
                ],
            )
            conn.executemany(
                "INSERT INTO topology_edges VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        e.generation_id, e.edge_id, e.kind, e.source_id, e.target_id,
                        to_iso_z(e.effective_from),
                        to_iso_z(e.effective_until) if e.effective_until else None,
                    )
                    for e in document.edges
                ],
            )
            # A new open generation supersedes previous open generations: close
            # their rows at the new effective_from. Historical rows stay intact.
            if document.effective_until is None:
                cutoff = to_iso_z(document.effective_from)
                conn.execute(
                    "UPDATE topology_generations SET effective_until = ? "
                    "WHERE effective_until IS NULL AND generation_id <> ?",
                    (cutoff, document.generation_id),
                )
                conn.execute(
                    "UPDATE topology_nodes SET effective_until = ? "
                    "WHERE effective_until IS NULL AND generation_id <> ?",
                    (cutoff, document.generation_id),
                )
                conn.execute(
                    "UPDATE topology_edges SET effective_until = ? "
                    "WHERE effective_until IS NULL AND generation_id <> ?",
                    (cutoff, document.generation_id),
                )
        return {"generation_id": document.generation_id, "changed": True}

    def load_snapshot(self, as_of: str) -> tuple[TopologySnapshot | None, list[str]]:
        """Materialize every node/edge effective at ``as_of``.

        Returns the snapshot and the list of generation ids that contributed.
        Multiple generations can contribute when older generations were given
        explicit ``effective_until`` windows that overlap differently from the
        currently open one.
        """
        moment = to_iso_z(as_of)
        generation_rows = self.connection.execute(
            "SELECT generation_id FROM topology_generations "
            "WHERE effective_from <= ? AND (effective_until IS NULL OR effective_until > ?) "
            "ORDER BY effective_from, generation_id",
            (moment, moment),
        ).fetchall()
        if not generation_rows:
            return None, []
        generation_ids = [row["generation_id"] for row in generation_rows]
        placeholders = ",".join("?" for _ in generation_ids)

        nodes: dict[str, NodeVersion] = {}
        node_rows = self.connection.execute(
            f"SELECT * FROM topology_nodes WHERE generation_id IN ({placeholders}) "
            f"AND effective_from <= ? AND (effective_until IS NULL OR effective_until > ?)",
            (*generation_ids, moment, moment),
        ).fetchall()
        # Newest generation wins for a shared node id; ids that only existed in
        # an older generation remain visible for the same instant.
        for row in sorted(node_rows, key=lambda r: (r["effective_from"], r["generation_id"]), reverse=True):
            if row["node_id"] not in nodes:
                nodes[row["node_id"]] = NodeVersion(
                    row["node_id"], row["kind"], row["name"], row["generation_id"],
                    row["effective_from"], row["effective_until"],
                )

        edges: list[EdgeVersion] = []
        edge_rows = self.connection.execute(
            f"SELECT * FROM topology_edges WHERE generation_id IN ({placeholders}) "
            f"AND effective_from <= ? AND (effective_until IS NULL OR effective_until > ?)",
            (*generation_ids, moment, moment),
        ).fetchall()
        for row in edge_rows:
            edges.append(
                EdgeVersion(
                    row["edge_id"], row["kind"], row["source_id"], row["target_id"],
                    row["generation_id"], row["effective_from"], row["effective_until"],
                )
            )
        open_generation = self.connection.execute(
            "SELECT generation_id FROM topology_generations "
            "WHERE effective_from <= ? AND effective_until IS NULL ORDER BY effective_from DESC LIMIT 1",
            (moment,),
        ).fetchone()
        label_generation = open_generation["generation_id"] if open_generation else generation_ids[-1]

        # Full version history per stable id: historical event names resolve
        # through renames while the effective rows above keep time-scoped names.
        alias_rows = self.connection.execute(
            "SELECT * FROM topology_nodes ORDER BY node_id, effective_from, generation_id"
        ).fetchall()
        aliases: dict[str, list[NodeVersion]] = {}
        for row in alias_rows:
            aliases.setdefault(row["node_id"], []).append(
                NodeVersion(
                    row["node_id"], row["kind"], row["name"], row["generation_id"],
                    row["effective_from"], row["effective_until"],
                )
            )
        node_aliases = {node_id: tuple(versions) for node_id, versions in aliases.items()}
        return TopologySnapshot(moment, label_generation, nodes, edges, node_aliases), generation_ids

    # -------------------------------------------------------- observations
    def save_observation(self, obs: Observation) -> bool:
        key = (obs.event_key, to_iso_z(obs.updated_at), obs.source_ref)
        with self.connection:
            existing = self.connection.execute(
                "SELECT 1 FROM event_observations WHERE event_key = ? AND updated_at = ? AND source_ref = ?",
                key,
            ).fetchone()
            if existing is not None:
                return False
            self.connection.execute(
                "INSERT INTO event_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    obs.event_key, to_iso_z(obs.updated_at), obs.service_name, obs.component_name,
                    obs.event_id, to_iso_z(obs.started_at), obs.state, obs.impact, obs.summary,
                    obs.source_ref, obs.raw_hash,
                ),
            )
        return True

    def save_observations(self, observations: list[Observation]) -> dict:
        inserted = 0
        for obs in observations:
            if self.save_observation(obs):
                inserted += 1
        return {"received": len(observations), "inserted": inserted, "duplicated": len(observations) - inserted}

    def load_observations(self) -> list[Observation]:
        rows = self.connection.execute(
            "SELECT * FROM event_observations ORDER BY updated_at, event_key, source_ref"
        ).fetchall()
        return [
            Observation(
                row["service_name"], row["component_name"], row["event_id"], row["started_at"],
                row["updated_at"], row["state"], row["impact"], row["summary"], row["source_ref"],
                row["raw_hash"],
            )
            for row in rows
        ]

    # --------------------------------------------------------- compute run
    def create_run(self, run_id: str, request: dict) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO compute_runs VALUES (?, ?, ?, ?)",
                (run_id, now_iso_z(), "pending", json.dumps(request, ensure_ascii=False)),
            )
            self.connection.execute(
                "UPDATE compute_runs SET status = 'running' WHERE run_id = ?", (run_id,)
            )
            for service_name in request["services"]:
                self.connection.execute(
                    "INSERT OR IGNORE INTO compute_tasks(run_id, service_name, as_of, status) "
                    "VALUES (?, ?, ?, 'pending')",
                    (run_id, service_name, to_iso_z(request["as_of"])),
                )

    def claim_pending_task(self, run_id: str) -> sqlite3.Row | None:
        with self.connection:
            # 'running' is also reclaimable: a process killed mid-task leaves no
            # other signal, so a resumed run retries it.
            row = self.connection.execute(
                "SELECT * FROM compute_tasks WHERE run_id = ? AND status IN ('pending', 'failed', 'running') "
                "ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, service_name LIMIT 1",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE compute_tasks SET status = 'running', started_at = ?, attempts = attempts + 1 "
                "WHERE run_id = ? AND service_name = ? AND as_of = ?",
                (now_iso_z(), run_id, row["service_name"], row["as_of"]),
            )
        return row

    def complete_task(self, run_id: str, service_name: str, as_of: str, result: dict) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO service_status_results VALUES (?, ?, ?, ?, ?, ?)",
                (
                    run_id, service_name, to_iso_z(as_of),
                    json.dumps(result, ensure_ascii=False, sort_keys=True),
                    now_iso_z(), result.get("topology_generation_id"),
                ),
            )
            self.connection.execute(
                "UPDATE compute_tasks SET status = 'done', finished_at = ?, last_error = NULL "
                "WHERE run_id = ? AND service_name = ? AND as_of = ?",
                (now_iso_z(), run_id, service_name, to_iso_z(as_of)),
            )

    def fail_task(self, run_id: str, service_name: str, as_of: str, error: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE compute_tasks SET status = 'failed', last_error = ? "
                "WHERE run_id = ? AND service_name = ? AND as_of = ?",
                (error[:500], run_id, service_name, to_iso_z(as_of)),
            )

    def finish_run(self, run_id: str) -> dict:
        rows = self.connection.execute(
            "SELECT status, COUNT(*) AS n FROM compute_tasks WHERE run_id = ? GROUP BY status",
            (run_id,),
        ).fetchall()
        counts = {row["status"]: row["n"] for row in rows}
        final_status = "completed" if not counts.get("pending") and not counts.get("failed") and not counts.get("running") else "failed"
        with self.connection:
            self.connection.execute(
                "UPDATE compute_runs SET status = ? WHERE run_id = ?", (final_status, run_id)
            )
        return {"run_id": run_id, "status": final_status, "tasks": counts}

    def get_run(self, run_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT * FROM compute_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        tasks = self.connection.execute(
            "SELECT service_name, as_of, status, attempts, last_error FROM compute_tasks WHERE run_id = ? ORDER BY service_name",
            (run_id,),
        ).fetchall()
        return {
            "run_id": run_id,
            "status": row["status"],
            "created_at": row["created_at"],
            "request": json.loads(row["request_json"]),
            "tasks": [dict(task) for task in tasks],
        }

    def load_result(self, run_id: str, service_name: str, as_of: str) -> dict | None:
        row = self.connection.execute(
            "SELECT result_json FROM service_status_results WHERE run_id = ? AND service_name = ? AND as_of = ?",
            (run_id, service_name, to_iso_z(as_of)),
        ).fetchone()
        return json.loads(row["result_json"]) if row else None

    def latest_result(self, service_name: str, as_of: str) -> dict | None:
        row = self.connection.execute(
            "SELECT result_json FROM service_status_results WHERE service_name = ? AND as_of = ? "
            "ORDER BY computed_at DESC LIMIT 1",
            (service_name, to_iso_z(as_of)),
        ).fetchone()
        return json.loads(row["result_json"]) if row else None
