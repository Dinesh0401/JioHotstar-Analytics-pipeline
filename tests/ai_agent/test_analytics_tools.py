"""Tests for the analytics tools (DB access faked via datasources monkeypatch)."""

from __future__ import annotations

from ai_agent import analytics_tools, datasources
from ai_agent.tools import Tool, ToolResult


def test_make_sql_tool_builds_result(monkeypatch):
    monkeypatch.setattr(
        datasources,
        "run_athena_sql",
        lambda sql: [{"genre": "Sports", "total_views": "900"}],
    )
    tool = analytics_tools.make_sql_tool(
        name="demo",
        description="demo tool",
        parameters={"type": "object", "properties": {}},
        build_sql=lambda params: "SELECT genre, total_views FROM genre_popularity",
        title="Demo",
    )
    assert isinstance(tool, Tool)
    result = tool.fn({}, None)
    assert isinstance(result, ToolResult)
    assert result.source == "fixed_sql"
    assert result.sql.startswith("SELECT")
    assert result.dataframe is not None
    assert list(result.dataframe["genre"]) == ["Sports"]


def test_make_sql_tool_handles_datasource_error(monkeypatch):
    def boom(sql):
        raise datasources.DataSourceError("athena down")

    monkeypatch.setattr(datasources, "run_athena_sql", boom)
    tool = analytics_tools.make_sql_tool(
        name="demo",
        description="d",
        parameters={"type": "object", "properties": {}},
        build_sql=lambda params: "SELECT 1 FROM genre_popularity",
        title="Demo",
    )
    result = tool.fn({}, None)
    assert result.error != ""
    assert "athena down" in result.error


def test_top_content_tool_runs(monkeypatch):
    monkeypatch.setattr(
        datasources,
        "run_athena_sql",
        lambda sql: [{"title": "World Cup", "content_type": "Sports",
                       "total_views": "900", "unique_viewers": "700"}],
    )
    registry = analytics_tools.build_default_registry()
    result = registry.execute("top_content", {"limit": 5}, state=None)
    assert "World Cup" in result.text
    assert result.dataframe is not None
