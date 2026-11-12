from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class StatusEvent:
    service: str
    component: str
    event_id: str
    started_at: str
    updated_at: str
    state: str
    impact: str
    summary: str
    source_ref: str

    @property
    def event_key(self) -> str:
        return "|".join((self.service, self.component, self.event_id))
