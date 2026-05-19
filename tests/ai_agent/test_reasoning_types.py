"""Tests for runtime state and brain-action types."""

from __future__ import annotations

from ai_agent.reasoning import AgentState, FinalAnswer, ToolCall


def test_agent_state_defaults():
    state = AgentState(conversation_id="c1", user_query="hi")
    assert state.step_count == 0
    assert state.observations == []
    assert state.tool_history == []
    assert state.intermediate_results == {}
    assert state.memory == {}
    assert state.active_brain == "rule"
    assert state.started_at is not None


def test_tool_call_holds_name_args_thought():
    call = ToolCall(tool_name="churn_risk", args={"plan_id": "PREMIUM"}, thought="check churn")
    assert call.tool_name == "churn_risk"
    assert call.args == {"plan_id": "PREMIUM"}
    assert call.thought == "check churn"


def test_final_answer_holds_text():
    answer = FinalAnswer(text="done", thought="wrapping up")
    assert answer.text == "done"
