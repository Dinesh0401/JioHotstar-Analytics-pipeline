"""Structured, serialisable reasoning trace for the AI agent runtime."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Callable


class StepKind(str, Enum):
    """The kind of a single step in the reasoning trace."""

    THOUGHT = "thought"
    TOOL_CALL = "tool_call"
    OBSERVATION = "observation"
    FALLBACK = "fallback"
    SUMMARY = "summary"
    FINAL = "final"
    ERROR = "error"


@dataclass
class TraceEvent:
    """One observable step in the reasoning loop."""

    kind: StepKind
    title: str
    detail: str = ""
    tool_name: str = ""
    tool_args: dict = field(default_factory=dict)
    sql: str = ""
    duration_ms: int = 0
    status: str = "ok"  # ok | error | fallback
    step_index: int = 0


class Trace:
    """Collects TraceEvents and (optionally) streams them to a callback."""

    def __init__(self, on_event: Callable[[TraceEvent], None] | None = None) -> None:
        self.events: list[TraceEvent] = []
        self._on_event = on_event

    def add(self, event: TraceEvent) -> None:
        self.events.append(event)
        if self._on_event is not None:
            self._on_event(event)

    def to_json(self) -> str:
        payload = []
        for event in self.events:
            data = asdict(event)
            data["kind"] = event.kind.value
            payload.append(data)
        return json.dumps(payload, indent=2)

    @classmethod
    def from_json(cls, data: str) -> "Trace":
        trace = cls()
        for raw in json.loads(data):
            raw = dict(raw)
            raw["kind"] = StepKind(raw["kind"])
            trace.events.append(TraceEvent(**raw))
        return trace
