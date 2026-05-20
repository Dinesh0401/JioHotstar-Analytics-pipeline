"""Tests for the ConversationSession shim over ReasoningEngine."""

from __future__ import annotations

from ai_agent.langgraph_agent import ConversationSession, build_engine
from ai_agent import datasources


def test_build_engine_returns_runnable_engine():
    engine = build_engine()
    assert engine.rule_brain is not None


def test_conversation_session_ask_returns_text(monkeypatch):
    monkeypatch.setattr(
        datasources, "run_athena_sql",
        lambda sql: [{"genre": "Sports", "total_views": "900", "unique_viewers": "700"}],
    )
    session = ConversationSession()
    answer = session.ask("which genre is most popular?")
    assert isinstance(answer, str)
    assert answer.strip() != ""


def test_conversation_session_reset_clears_history():
    session = ConversationSession()
    session.history.append("x")
    session.reset()
    assert session.history == []
