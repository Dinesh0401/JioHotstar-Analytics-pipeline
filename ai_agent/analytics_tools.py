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


# ── Branching tools ──────────────────────────────────────────────────────────

def _churn_risk_fn(params: dict, state) -> ToolResult:
    user_id = params.get("user_id")
    by_plan = str(params.get("by", "")).lower() == "plan"
    if user_id:
        safe = str(user_id).replace("'", "")
        sql = ("SELECT user_id, plan_id, subscription_duration_days, "
               "churn_probability, prediction "
               f"FROM user_churn_prediction WHERE user_id = '{safe}' LIMIT 1")
        title = f"Churn Analysis — {safe}"
    elif by_plan:
        sql = ("SELECT plan_id, COUNT(*) AS total_users, "
               "SUM(CASE WHEN prediction = 1 THEN 1 ELSE 0 END) AS predicted_churners, "
               "ROUND(AVG(churn_probability), 4) AS avg_churn_prob "
               "FROM user_churn_prediction GROUP BY plan_id ORDER BY avg_churn_prob DESC")
        title = "Churn Risk by Plan"
    else:
        sql = ("SELECT user_id, plan_id, churn_probability, prediction "
               "FROM user_churn_prediction ORDER BY churn_probability DESC LIMIT 15")
        title = "Top Churn-Risk Users"
    try:
        rows = datasources.run_athena_sql(sql)
    except datasources.DataSourceError as exc:
        return ToolResult(text="", sql=sql, error=str(exc), source="fixed_sql")
    df = _rows_to_df(rows)
    if df.empty:
        return ToolResult(text=f"**{title}**: no matching rows.", sql=sql,
                          dataframe=df, source="fixed_sql")
    return ToolResult(text=f"**{title}** ({len(df)} rows)\n\n{df.head(15).to_markdown(index=False)}",
                      dataframe=df, sql=sql, source="fixed_sql")


CHURN_RISK = Tool(
    name="churn_risk",
    description="Churn risk analysis. Pass user_id for one user, by='plan' to group "
                "by subscription plan, or no args for the highest-risk users.",
    parameters={"type": _OBJECT, "properties": {
        "user_id": {"type": "string", "description": "Optional specific user id."},
        "by": {"type": "string", "enum": ["plan"],
               "description": "Set to 'plan' to group churn by plan."}}},
    fn=_churn_risk_fn,
)


def _streaming_traffic_fn(params: dict, state) -> ToolResult:
    dimension = str(params.get("dimension", "")).lower()
    if dimension == "genre":
        sql = ("SELECT genre, COUNT(*) AS events, COUNT(DISTINCT user_id) AS users "
               "FROM streaming_events WHERE event_ts > NOW() - INTERVAL '30 minutes' "
               "GROUP BY genre ORDER BY events DESC LIMIT 10")
        title = "Live Streaming by Genre (30 min)"
    elif dimension == "city":
        sql = ("SELECT city, COUNT(*) AS events, COUNT(DISTINCT user_id) AS users "
               "FROM streaming_events WHERE event_ts > NOW() - INTERVAL '30 minutes' "
               "GROUP BY city ORDER BY events DESC LIMIT 10")
        title = "Live Streaming by City (30 min)"
    else:
        sql = ("SELECT COUNT(*) AS total_events, COUNT(DISTINCT user_id) AS unique_users, "
               "COUNT(*) FILTER (WHERE event_ts > NOW() - INTERVAL '1 minute') AS events_last_min "
               "FROM streaming_events WHERE event_ts > NOW() - INTERVAL '30 minutes'")
        title = "Real-Time Streaming Status"
    try:
        rows = datasources.run_pg_sql(sql)
    except datasources.DataSourceError as exc:
        return ToolResult(text="", sql=sql, error=str(exc), source="streaming")
    df = _rows_to_df(rows)
    if df.empty:
        return ToolResult(text=f"**{title}**: no recent streaming events.", sql=sql,
                          dataframe=df, source="streaming")
    return ToolResult(text=f"**{title}**\n\n{df.head(15).to_markdown(index=False)}",
                      dataframe=df, sql=sql, source="streaming")


STREAMING_TRAFFIC = Tool(
    name="streaming_traffic",
    description="Real-time streaming traffic from the live PostgreSQL feed. "
                "Optional dimension='genre' or 'city'.",
    parameters={"type": _OBJECT, "properties": {
        "dimension": {"type": "string", "enum": ["genre", "city"],
                      "description": "Optional breakdown dimension."}}},
    fn=_streaming_traffic_fn,
)


def _pipeline_health_fn(params: dict, state) -> ToolResult:
    tables = ["daily_active_users", "content_watch_metrics", "genre_popularity",
              "subscription_metrics", "content_ratings_summary", "user_churn_prediction"]
    rows = []
    for table in tables:
        try:
            count = datasources.run_athena_sql(f"SELECT COUNT(*) AS cnt FROM {table}")
            rows.append({"table": table, "rows": count[0].get("cnt", "0"), "status": "OK"})
        except datasources.DataSourceError:
            rows.append({"table": table, "rows": "0", "status": "ERROR"})
    df = _rows_to_df(rows)
    ok = int((df["status"] == "OK").sum())
    overall = "HEALTHY" if ok == len(tables) else "DEGRADED"
    return ToolResult(
        text=f"**Pipeline Health: {overall}** ({ok}/{len(tables)} tables OK)\n\n"
             f"{df.to_markdown(index=False)}",
        dataframe=df, source="fixed_sql")


PIPELINE_HEALTH = Tool(
    name="pipeline_health",
    description="Data pipeline health check — row counts and status for the Gold/ML tables.",
    parameters={"type": _OBJECT, "properties": {}},
    fn=_pipeline_health_fn,
)


# ── Tier-3 escape hatch: generated SQL, guardrailed ──────────────────────────

def _generate_sql(question: str):
    """Generate a SqlPlan for an open-ended question via the existing generators."""
    from ai_agent.sql_generator import RuleBasedSQLGenerator

    return RuleBasedSQLGenerator(database="jiohotstar_gold").generate(question)


def _query_analytics_fn(params: dict, state) -> ToolResult:
    from ai_agent.catalog import APPROVED_TABLES
    from ai_agent.sql_generator import SqlGenerationError
    from ai_agent.tools import SqlValidationError, validate_sql

    question = str(params.get("question", "")).strip()
    if not question:
        return ToolResult(text="", error="query_analytics requires a 'question'.")
    try:
        plan = _generate_sql(question)
    except SqlGenerationError as exc:
        return ToolResult(text="", error=f"Could not generate SQL: {exc}")
    try:
        validate_sql(plan.sql, set(APPROVED_TABLES.keys()))
    except SqlValidationError as exc:
        return ToolResult(text="", sql=plan.sql, error=f"Generated SQL rejected: {exc}")
    try:
        rows = datasources.run_athena_sql(plan.sql)
    except datasources.DataSourceError as exc:
        return ToolResult(text="", sql=plan.sql, error=str(exc), source="generated_sql")
    df = _rows_to_df(rows)
    body = "no matching rows." if df.empty else df.head(15).to_markdown(index=False)
    return ToolResult(text=f"**{plan.title}**\n\n{body}", dataframe=df,
                      sql=plan.sql, source="generated_sql")


QUERY_ANALYTICS = Tool(
    name="query_analytics",
    description="ESCAPE HATCH — use ONLY when no other tool fits the question. "
                "Answers open-ended analytics questions by generating guardrailed SQL.",
    parameters={"type": _OBJECT, "properties": {
        "question": {"type": "string",
                      "description": "The natural-language analytics question."}}},
    fn=_query_analytics_fn,
)

_ALL_TOOLS = [
    TOP_CONTENT, GENRE_POPULARITY, CONTENT_RATINGS, SUBSCRIPTIONS, DAU,
    POPULARITY_PREDICTIONS, RECOMMENDATIONS, CHURN_RISK, STREAMING_TRAFFIC,
    PIPELINE_HEALTH, QUERY_ANALYTICS,
]


def build_default_registry() -> ToolRegistry:
    """Assemble the full runtime tool registry."""
    registry = ToolRegistry()
    for tool in _ALL_TOOLS:
        registry.register(tool)
    return registry
