from __future__ import annotations

import argparse
import json

from .impact import (
    ingest_events,
    ingest_topology,
    query_all_services,
    query_service,
    resume_run,
    run_compute_batch,
)
from .impact_storage import ImpactStore
from .pipeline import StatusPipeline
from .sources import JsonStatusSource
from .storage import StatusStore


def _print(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _read_json(path: str) -> object:
    return JsonStatusSource(path).read()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="saas-status")
    sub = parser.add_subparsers(dest="command")

    legacy = sub.add_parser("ingest-legacy", help="legacy merged-event pipeline (original behavior)")
    legacy.add_argument("--fixture", required=True)
    legacy.add_argument("--database", default="saas-status.sqlite3")

    events = sub.add_parser("ingest-events", help="store raw event observations (idempotent)")
    events.add_argument("--input", required=True)
    events.add_argument("--database", default="saas-status.sqlite3")

    topo = sub.add_parser("ingest-topology", help="store a versioned topology generation")
    topo.add_argument("--input", required=True)
    topo.add_argument("--database", default="saas-status.sqlite3")

    query = sub.add_parser("query", help="impact status of one service at an observation instant")
    query.add_argument("--service", required=True)
    query.add_argument("--as-of", required=True)
    query.add_argument("--database", default="saas-status.sqlite3")

    query_all = sub.add_parser("query-all", help="impact status of every known service at one instant")
    query_all.add_argument("--as-of", required=True)
    query_all.add_argument("--database", default="saas-status.sqlite3")

    run = sub.add_parser("run", help="resumable batch computation for several services")
    run.add_argument("--services", required=True, help="comma-separated service names")
    run.add_argument("--as-of", required=True)
    run.add_argument("--run-id")
    run.add_argument("--database", default="saas-status.sqlite3")

    resume = sub.add_parser("resume", help="resume an interrupted compute run")
    resume.add_argument("--run-id", required=True)
    resume.add_argument("--database", default="saas-status.sqlite3")

    run_status = sub.add_parser("run-status", help="inspect tasks and results of a compute run")
    run_status.add_argument("--run-id", required=True)
    run_status.add_argument("--database", default="saas-status.sqlite3")

    legacy_query = sub.add_parser("events", help="legacy event query over merged status_events rows")
    legacy_query.add_argument("--database", default="saas-status.sqlite3")

    # Legacy top-level flags (original CLI invocation stays valid).
    parser.add_argument("--fixture")
    parser.add_argument("--database", default="saas-status.sqlite3")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        if args.fixture:
            store = StatusStore(args.database)
            try:
                events = StatusPipeline(JsonStatusSource(args.fixture), store).run()
                print({"events": len(events), "database": args.database})
            finally:
                store.close()
            return
        parser.print_help()
        return

    if args.command == "ingest-legacy":
        store = StatusStore(args.database)
        try:
            events = StatusPipeline(JsonStatusSource(args.fixture), store).run()
            _print({"events": len(events), "database": args.database})
        finally:
            store.close()
        return

    if args.command == "events":
        import sqlite3

        connection = sqlite3.connect(args.database)
        connection.row_factory = sqlite3.Row
        try:
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM status_events ORDER BY service, updated_at"
            )]
        except sqlite3.OperationalError:
            rows = []
        finally:
            connection.close()
        _print({"count": len(rows), "events": rows})
        return

    store = ImpactStore(args.database)
    try:
        if args.command == "ingest-events":
            _print(ingest_events(store, _read_json(args.input)))
        elif args.command == "ingest-topology":
            _print(ingest_topology(store, _read_json(args.input)))
        elif args.command == "query":
            _print(query_service(store, args.service, args.as_of))
        elif args.command == "query-all":
            _print(query_all_services(store, args.as_of))
        elif args.command == "run":
            services = [item.strip() for item in args.services.split(",") if item.strip()]
            _print(run_compute_batch(store, services, args.as_of, run_id=args.run_id))
        elif args.command == "resume":
            _print(resume_run(store, args.run_id))
        elif args.command == "run-status":
            _print(store.get_run(args.run_id))
    finally:
        store.close()


if __name__ == "__main__":
    main()
