from __future__ import annotations
import json
from pathlib import Path


class StatusSource:
    def read(self) -> list[dict]:
        raise NotImplementedError


class JsonStatusSource(StatusSource):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read(self) -> list[dict]:
        return json.loads(self.path.read_text(encoding="utf-8"))
