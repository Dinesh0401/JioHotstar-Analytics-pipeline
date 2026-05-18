"""Tests for the tool framework: ToolResult, Tool, ToolRegistry, validate_sql."""

from __future__ import annotations

import pandas as pd
import pytest

from ai_agent.tools import Tool, ToolResult


def test_tool_result_defaults():
    result = ToolResult(text="hello")
    assert result.text == "hello"
    assert result.dataframe is None
    assert result.sql == ""
    assert result.error == ""
    assert result.source == ""


def test_tool_result_carries_dataframe_and_source():
    df = pd.DataFrame({"a": [1]})
    result = ToolResult(text="t", dataframe=df, sql="SELECT 1", source="fixed_sql")
    assert result.dataframe is df
    assert result.source == "fixed_sql"


def test_tool_is_frozen_spec():
    tool = Tool(name="x", description="d", parameters={"type": "object"}, fn=lambda p, s: ToolResult(text="ok"))
    assert tool.name == "x"
    with pytest.raises(Exception):
        tool.name = "y"  # frozen dataclass


from ai_agent.tools import ToolRegistry


def _echo_tool(name="echo"):
    return Tool(
        name=name,
        description="echoes its params",
        parameters={"type": "object", "properties": {}},
        fn=lambda params, state: ToolResult(text=f"got {params}", source="fixed_sql"),
    )


def test_registry_register_and_get():
    reg = ToolRegistry()
    reg.register(_echo_tool())
    assert reg.get("echo").name == "echo"
    assert reg.get("missing") is None
    assert reg.names() == ["echo"]


def test_registry_rejects_duplicate_names():
    reg = ToolRegistry()
    reg.register(_echo_tool())
    with pytest.raises(ValueError):
        reg.register(_echo_tool())


def test_registry_execute_runs_tool():
    reg = ToolRegistry()
    reg.register(_echo_tool())
    result = reg.execute("echo", {"k": 1}, state=None)
    assert "got {'k': 1}" in result.text


def test_registry_execute_unknown_tool_returns_error_result():
    reg = ToolRegistry()
    result = reg.execute("nope", {}, state=None)
    assert result.error != ""
    assert "nope" in result.error


def test_registry_execute_catches_tool_exception():
    def boom(params, state):
        raise RuntimeError("kaboom")

    reg = ToolRegistry()
    reg.register(Tool(name="boom", description="d", parameters={}, fn=boom))
    result = reg.execute("boom", {}, state=None)
    assert result.error != ""
    assert "kaboom" in result.error
