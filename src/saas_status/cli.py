from __future__ import annotations
import argparse
from .pipeline import StatusPipeline
from .sources import JsonStatusSource
from .storage import StatusStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--database", default="saas-status.sqlite3")
    args = parser.parse_args()
    store = StatusStore(args.database)
    try:
        events = StatusPipeline(JsonStatusSource(args.fixture), store).run()
        print({"events": len(events), "database": args.database})
    finally:
        store.close()


if __name__ == "__main__":
    main()
