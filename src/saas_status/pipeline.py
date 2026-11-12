from __future__ import annotations
from .merge import merge_events
from .models import StatusEvent
from .normalize import normalize_event
from .sources import StatusSource
from .storage import StatusStore


class StatusPipeline:
    def __init__(self, source: StatusSource, store: StatusStore) -> None:
        self.source = source
        self.store = store

    def run(self) -> list[StatusEvent]:
        events = merge_events([normalize_event(item) for item in self.source.read()])
        self.store.save_many(events)
        return events
