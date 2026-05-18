"""Tests for the reasoning trace: events, callback, serialization."""

from __future__ import annotations

from ai_agent.trace import StepKind, Trace, TraceEvent


def test_add_appends_and_fires_callback():
    seen = []
    trace = Trace(on_event=seen.append)
    event = TraceEvent(kind=StepKind.THOUGHT, title="thinking", step_index=1)
    trace.add(event)
    assert trace.events == [event]
    assert seen == [event]


def test_add_without_callback_still_records():
    trace = Trace()
    trace.add(TraceEvent(kind=StepKind.FINAL, title="done"))
    assert len(trace.events) == 1


def test_event_defaults():
    event = TraceEvent(kind=StepKind.TOOL_CALL, title="t")
    assert event.detail == ""
    assert event.tool_args == {}
    assert event.duration_ms == 0
    assert event.status == "ok"


def test_to_json_from_json_round_trip():
    trace = Trace()
    trace.add(TraceEvent(kind=StepKind.THOUGHT, title="think", duration_ms=12, step_index=1))
    trace.add(
        TraceEvent(
            kind=StepKind.TOOL_CALL,
            title="churn_risk",
            tool_name="churn_risk",
            tool_args={"plan_id": "PREMIUM"},
            sql="SELECT 1",
            duration_ms=40,
            step_index=2,
        )
    )
    restored = Trace.from_json(trace.to_json())
    assert restored.events == trace.events


def test_latency_fields_are_non_negative():
    trace = Trace()
    trace.add(TraceEvent(kind=StepKind.OBSERVATION, title="obs", duration_ms=5))
    for event in trace.events:
        assert event.duration_ms >= 0
