"""Tests for the demo-trace builder."""

from __future__ import annotations

from ai_agent.build_demo_traces import build_demo_trace
from ai_agent.trace import StepKind, Trace


def test_build_demo_trace_produces_serialisable_trace():
    trace = build_demo_trace("compare churn risk across subscription plans")
    assert isinstance(trace, Trace)
    assert len(trace.events) >= 2
    # Round-trips cleanly.
    restored = Trace.from_json(trace.to_json())
    assert restored.events == trace.events
    # Multi-step question chained at least one tool call.
    assert any(e.kind == StepKind.TOOL_CALL for e in trace.events)
