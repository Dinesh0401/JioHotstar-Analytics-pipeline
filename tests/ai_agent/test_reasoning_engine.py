"""Orchestration tests for ReasoningEngine, using a scripted FakeBrain."""

from __future__ import annotations

from ai_agent.reasoning import AgentState, EngineResult, FinalAnswer, ReasoningEngine, ToolCall
from ai_agent.tools import Tool, ToolRegistry, ToolResult
from ai_agent.trace import StepKind


class FakeBrain:
    """Returns a pre-scripted sequence of actions, one per next_action call."""

    def __init__(self, actions, raise_on_call=None, name="rule"):
        self.name = name
        self._actions = list(actions)
        self._raise_on_call = raise_on_call
        self._calls = 0

    def next_action(self, state, tools):
        self._calls += 1
        if self._raise_on_call == self._calls:
            raise RuntimeError("brain exploded")
        return self._actions.pop(0)

    def summarize(self, state):
        return "forced summary"


def _registry(tools):
    reg = ToolRegistry()
    for tool in tools:
        reg.register(tool)
    return reg


def _ok_tool(name, text="ok"):
    return Tool(name=name, description="d", parameters={},
                fn=lambda p, s: ToolResult(text=text, source="fixed_sql"))


def _failing_tool(name):
    def boom(p, s):
        raise RuntimeError("tool down")
    return Tool(name=name, description="d", parameters={}, fn=boom)


def test_engine_runs_single_tool_then_final():
    reg = _registry([_ok_tool("a", "result A")])
    brain = FakeBrain([ToolCall(tool_name="a"), FinalAnswer(text="all done")])
    engine = ReasoningEngine(reg, rule_brain=brain)
    result = engine.run("question")
    assert isinstance(result, EngineResult)
    assert result.answer == "all done"
    kinds = [e.kind for e in result.trace.events]
    assert StepKind.TOOL_CALL in kinds
    assert StepKind.OBSERVATION in kinds
    assert kinds[-1] == StepKind.FINAL


def test_engine_accumulates_observations():
    reg = _registry([_ok_tool("a", "A"), _ok_tool("b", "B")])
    brain = FakeBrain([ToolCall(tool_name="a"), ToolCall(tool_name="b"),
                       FinalAnswer(text="done")])
    result = ReasoningEngine(reg, rule_brain=brain).run("q")
    assert len(result.state.observations) == 2


def test_engine_caps_at_max_steps_and_forces_final():
    reg = _registry([_ok_tool("a")])
    # Brain always asks for another tool call -> must be capped.
    brain = FakeBrain([ToolCall(tool_name="a", args={"n": i}) for i in range(20)])
    result = ReasoningEngine(reg, rule_brain=brain).run("q")
    assert result.answer == "forced summary"
    assert result.state.step_count <= 5


def test_engine_loop_guard_on_repeated_call():
    reg = _registry([_ok_tool("a")])
    # Same tool + same args twice -> loop guard forces a FINAL.
    brain = FakeBrain([ToolCall(tool_name="a", args={"x": 1}),
                       ToolCall(tool_name="a", args={"x": 1})])
    result = ReasoningEngine(reg, rule_brain=brain).run("q")
    assert any(e.kind == StepKind.FINAL for e in result.trace.events)


def test_engine_partial_failure_continuation():
    """Tool A ok, Tool B fails, Tool C still runs, FINAL still produced."""
    reg = _registry([_ok_tool("a", "A"), _failing_tool("b"), _ok_tool("c", "C")])
    brain = FakeBrain([ToolCall(tool_name="a"), ToolCall(tool_name="b"),
                       ToolCall(tool_name="c"), FinalAnswer(text="finished")])
    result = ReasoningEngine(reg, rule_brain=brain).run("q")
    assert result.answer == "finished"
    assert len(result.state.observations) == 3
    assert any(o.error for o in result.state.observations)
    obs = [e for e in result.trace.events if e.kind == StepKind.OBSERVATION]
    assert any(e.status == "error" for e in obs)


def test_engine_falls_back_when_primary_brain_raises():
    reg = _registry([_ok_tool("a", "A")])
    primary = FakeBrain([], raise_on_call=1, name="bedrock")  # raises immediately
    rule = FakeBrain([ToolCall(tool_name="a"), FinalAnswer(text="rule answer")])
    engine = ReasoningEngine(reg, bedrock_brain=primary, rule_brain=rule)
    result = engine.run("q")
    assert result.answer == "rule answer"
    assert result.state.active_brain == "rule"
    assert any(e.kind == StepKind.FALLBACK for e in result.trace.events)


def test_engine_trace_latency_fields_non_negative():
    reg = _registry([_ok_tool("a")])
    brain = FakeBrain([ToolCall(tool_name="a"), FinalAnswer(text="done")])
    result = ReasoningEngine(reg, rule_brain=brain).run("q")
    for event in result.trace.events:
        assert event.duration_ms >= 0
