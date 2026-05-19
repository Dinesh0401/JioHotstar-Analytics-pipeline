"""Tests for the deterministic RuleBrain planner."""

from __future__ import annotations

from ai_agent.analytics_tools import build_default_registry
from ai_agent.brains import RuleBrain
from ai_agent.reasoning import AgentState, FinalAnswer, ToolCall

_TOOLS = build_default_registry().all()


def _state(query: str) -> AgentState:
    return AgentState(conversation_id="t", user_query=query)


def test_rule_brain_routes_top_content():
    action = RuleBrain().next_action(_state("what are the top trending shows?"), _TOOLS)
    assert isinstance(action, ToolCall)
    assert action.tool_name == "top_content"


def test_rule_brain_routes_churn():
    action = RuleBrain().next_action(_state("which users will churn?"), _TOOLS)
    assert isinstance(action, ToolCall)
    assert action.tool_name == "churn_risk"


def test_rule_brain_uses_query_analytics_when_nothing_matches():
    action = RuleBrain().next_action(_state("xyzzy random gibberish"), _TOOLS)
    assert isinstance(action, ToolCall)
    assert action.tool_name == "query_analytics"


def test_rule_brain_finishes_after_observations():
    state = _state("top shows")
    state.observations.append(type("R", (), {"text": "some result", "error": ""})())
    state.tool_history.append("top_content({})")
    action = RuleBrain().next_action(state, _TOOLS)
    assert isinstance(action, FinalAnswer)


def test_rule_brain_chains_compare_plan_question():
    """The displayed multi-step sample must chain: subscriptions then churn_risk."""
    brain = RuleBrain()
    state = _state("compare churn risk across subscription plans")
    first = brain.next_action(state, _TOOLS)
    assert isinstance(first, ToolCall) and first.tool_name == "subscriptions"
    # Simulate the first observation, then ask again.
    state.observations.append(type("R", (), {"text": "plans", "error": ""})())
    state.tool_history.append("subscriptions({})")
    second = brain.next_action(state, _TOOLS)
    assert isinstance(second, ToolCall) and second.tool_name == "churn_risk"


def test_rule_brain_summarize_joins_observations():
    state = _state("q")
    state.observations.append(type("R", (), {"text": "obs one", "error": ""})())
    summary = RuleBrain().summarize(state)
    assert "obs one" in summary
