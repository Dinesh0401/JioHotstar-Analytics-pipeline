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
