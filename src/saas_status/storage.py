from __future__ import annotations
import hashlib
import sqlite3
from pathlib import Path
from .models import StatusEvent


class StatusStore:
    def __init__(self, database: str | Path) -> None:
        self.connection = sqlite3.connect(database)
        self.connection.execute("""CREATE TABLE IF NOT EXISTS status_events (
            event_key TEXT PRIMARY KEY, service TEXT NOT NULL, component TEXT NOT NULL,
            event_id TEXT NOT NULL, started_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            state TEXT NOT NULL, impact TEXT NOT NULL, summary TEXT NOT NULL,
            source_refs TEXT NOT NULL, content_hash TEXT NOT NULL
        )""")
        self.connection.commit()

    def save_many(self, events: list[StatusEvent]) -> None:
        rows = []
        for event in events:
            canonical = "|".join((event.event_key, event.updated_at, event.state, event.impact, event.summary))
            digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            rows.append((event.event_key, event.service, event.component, event.event_id, event.started_at, event.updated_at, event.state, event.impact, event.summary, event.source_ref, digest))
        self.connection.executemany("INSERT OR REPLACE INTO status_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
