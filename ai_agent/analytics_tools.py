"""Concrete analytics tools for the AI agent runtime.

Single-query tools are built declaratively with make_sql_tool. Branching tools
and the escape-hatch query tool are added alongside. build_default_registry()
assembles the full ToolRegistry used by the engine.
"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from ai_agent import datasources
from ai_agent.tools import Tool, ToolRegistry, ToolResult

_OBJECT = "object"


def _rows_to_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def make_sql_tool(
    name: str,
    description: str,
    parameters: dict,
    build_sql: Callable[[dict], str],
    title: str,
    runner: Callable[[str], list[dict]] | None = None,
    source: str = "fixed_sql",
) -> Tool:
    """Build a Tool that runs one templated query and returns a ToolResult."""

    def fn(params: dict, state) -> ToolResult:
        sql = build_sql(params or {})
        run = runner or datasources.run_athena_sql
        try:
            rows = run(sql)
        except datasources.DataSourceError as exc:
            return ToolResult(text="", sql=sql, error=str(exc), source=source)
        df = _rows_to_df(rows)
        if df.empty:
            return ToolResult(text=f"**{title}**: no matching rows.", sql=sql,
                              dataframe=df, source=source)
        preview = df.head(15).to_markdown(index=False)
        return ToolResult(
            text=f"**{title}** ({len(df)} rows)\n\n{preview}",
            dataframe=df,
            sql=sql,
            source=source,
        )

    return Tool(name=name, description=description, parameters=parameters, fn=fn)


def _clamp_limit(params: dict, default: int = 10, cap: int = 50) -> int:
    try:
        return min(int(params.get("limit", default)), cap)
    except (TypeError, ValueError):
        return default


# ── Single-query (fixed-SQL) tools ───────────────────────────────────────────

TOP_CONTENT = make_sql_tool(
    name="top_content",
    description="Most-watched content on JioHotstar. Use for trending / popular / "
                "most-viewed shows and movies.",
    parameters={"type": _OBJECT, "properties": {
        "limit": {"type": "integer", "description": "How many rows (max 50)."}}},
    build_sql=lambda p: (
        "SELECT title, content_type, total_views, unique_viewers "
        f"FROM content_watch_metrics ORDER BY total_views DESC LIMIT {_clamp_limit(p)}"),
    title="Most Watched Content",
)

GENRE_POPULARITY = make_sql_tool(
    name="genre_popularity",
    description="Genre popularity rankings by total views.",
    parameters={"type": _OBJECT, "properties": {}},
    build_sql=lambda p: (
        "SELECT genre, total_views, unique_viewers "
        "FROM genre_popularity ORDER BY total_views DESC LIMIT 10"),
    title="Genre Popularity",
)

CONTENT_RATINGS = make_sql_tool(
    name="content_ratings",
    description="Top-rated content by average rating.",
    parameters={"type": _OBJECT, "properties": {}},
    build_sql=lambda p: (
        "SELECT title, content_type, avg_rating, rating_count "
        "FROM content_ratings_summary WHERE rating_count >= 5 "
        "ORDER BY avg_rating DESC, rating_count DESC LIMIT 10"),
    title="Top Rated Content",
)

SUBSCRIPTIONS = make_sql_tool(
    name="subscriptions",
    description="Subscription plan metrics: totals, active, cancelled, top cancel reason. "
                "Use for questions about plans and cancellations.",
    parameters={"type": _OBJECT, "properties": {}},
    build_sql=lambda p: (
        "SELECT plan_id, total_subscriptions, active_subscriptions, "
        "cancelled_subscriptions, top_cancel_reason "
        "FROM subscription_metrics ORDER BY total_subscriptions DESC"),
    title="Subscription Metrics by Plan",
)

DAU = make_sql_tool(
    name="dau",
    description="Daily active user trends over recent dates.",
    parameters={"type": _OBJECT, "properties": {}},
    build_sql=lambda p: (
        "SELECT event_date, daily_active_users, total_views "
        "FROM daily_active_users ORDER BY event_date DESC LIMIT 15"),
    title="Daily Active Users",
)

POPULARITY_PREDICTIONS = make_sql_tool(
    name="popularity_predictions",
    description="ML content popularity predictions (which titles are predicted popular).",
    parameters={"type": _OBJECT, "properties": {}},
    build_sql=lambda p: (
        "SELECT title, primary_genre, total_views, popularity_probability, prediction "
        "FROM content_popularity_prediction ORDER BY popularity_probability DESC LIMIT 10"),
    title="Content Popularity Predictions",
)


def _recommendations_sql(params: dict) -> str:
    user_id = str(params.get("user_id") or "USR-1000001").replace("'", "")
    return (
        "SELECT recommended_content, content_type, recommendation_rank, score "
        f"FROM user_recommendations WHERE user_id = '{user_id}' "
        "ORDER BY recommendation_rank ASC LIMIT 10")


RECOMMENDATIONS = make_sql_tool(
    name="recommendations",
    description="Personalised content recommendations for a specific user_id.",
    parameters={"type": _OBJECT, "properties": {
        "user_id": {"type": "string", "description": "User id, e.g. USR-1000001."}}},
    build_sql=_recommendations_sql,
    title="Personalised Recommendations",
)


_FIXED_TOOLS = [
    TOP_CONTENT, GENRE_POPULARITY, CONTENT_RATINGS, SUBSCRIPTIONS,
    DAU, POPULARITY_PREDICTIONS, RECOMMENDATIONS,
]


def build_default_registry() -> ToolRegistry:
    """Assemble the runtime's tool registry. Extended with branching tools in Task 9."""
    registry = ToolRegistry()
    for tool in _FIXED_TOOLS:
        registry.register(tool)
    return registry
