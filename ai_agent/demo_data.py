"""Canned datasource for offline / Streamlit Cloud demo mode.

Replaces ``datasources.run_athena_sql`` / ``run_pg_sql`` with deterministic
in-memory results so the runtime works with zero AWS credentials. The engine,
brains, registry, and tools all run for real — only the data is canned, so the
reasoning trace is genuine. Intended for the Streamlit Cloud public demo,
``ai_agent.build_demo_traces``, and any other AWS-free showcase.
"""

from __future__ import annotations

from ai_agent import datasources

# Canned Athena rows, as an ordered list of (sql-substring, rows) pairs.
# First match wins, so put more-specific query patterns (aggregations,
# joins) BEFORE bare table names — otherwise a query like
# "FROM user_churn_prediction GROUP BY plan_id" would match the
# "user_churn_prediction" table key before the more specific "GROUP BY plan_id"
# aggregation key.
_CANNED_ATHENA: list[tuple[str, list[dict]]] = [
    # --- Specific query patterns (checked first) ---
    ("GROUP BY plan_id", [  # churn_risk by='plan' aggregation
        {"plan_id": "PREMIUM", "total_users": "5000",
         "predicted_churners": "1500", "avg_churn_prob": "0.48"},
        {"plan_id": "VIP", "total_users": "3000",
         "predicted_churners": "300", "avg_churn_prob": "0.19"},
        {"plan_id": "BASIC", "total_users": "8000",
         "predicted_churners": "3200", "avg_churn_prob": "0.41"},
    ]),
    # --- Per-table fallbacks ---
    ("subscription_metrics", [
        {"plan_id": "PREMIUM", "total_subscriptions": "5000",
         "active_subscriptions": "3800", "cancelled_subscriptions": "1200",
         "top_cancel_reason": "price"},
        {"plan_id": "VIP", "total_subscriptions": "3000",
         "active_subscriptions": "2600", "cancelled_subscriptions": "400",
         "top_cancel_reason": "content"},
        {"plan_id": "BASIC", "total_subscriptions": "8000",
         "active_subscriptions": "5500", "cancelled_subscriptions": "2500",
         "top_cancel_reason": "price"},
    ]),
    ("user_churn_prediction", [
        # Default churn_risk query returns highest-risk users.
        {"user_id": "USR-1000001", "plan_id": "PREMIUM",
         "churn_probability": "0.88", "prediction": "1"},
        {"user_id": "USR-1000204", "plan_id": "BASIC",
         "churn_probability": "0.82", "prediction": "1"},
        {"user_id": "USR-1000917", "plan_id": "PREMIUM",
         "churn_probability": "0.79", "prediction": "1"},
    ]),
    ("content_watch_metrics", [
        {"title": "World Cup Final", "content_type": "Sports",
         "total_views": "98000", "unique_viewers": "71000"},
        {"title": "Mystery Manor S2", "content_type": "Series",
         "total_views": "62000", "unique_viewers": "48000"},
        {"title": "Cricket Highlights", "content_type": "Sports",
         "total_views": "55000", "unique_viewers": "41000"},
    ]),
    ("genre_popularity", [
        {"genre": "Sports", "total_views": "210000", "unique_viewers": "140000"},
        {"genre": "Drama", "total_views": "145000", "unique_viewers": "110000"},
        {"genre": "Comedy", "total_views": "98000", "unique_viewers": "82000"},
    ]),
    ("content_ratings_summary", [
        {"title": "Mystery Manor S2", "content_type": "Series",
         "avg_rating": "4.7", "rating_count": "1820"},
        {"title": "World Cup Final", "content_type": "Sports",
         "avg_rating": "4.8", "rating_count": "3100"},
    ]),
    ("daily_active_users", [
        {"event_date": "2026-05-18", "daily_active_users": "412300",
         "total_views": "1820000"},
        {"event_date": "2026-05-17", "daily_active_users": "398100",
         "total_views": "1755000"},
        {"event_date": "2026-05-16", "daily_active_users": "402900",
         "total_views": "1780000"},
    ]),
    ("user_recommendations", [
        {"recommended_content": "Mystery Manor S2", "content_type": "Series",
         "recommendation_rank": "1", "score": "0.91"},
        {"recommended_content": "World Cup Final", "content_type": "Sports",
         "recommendation_rank": "2", "score": "0.87"},
    ]),
    ("content_popularity_prediction", [
        {"title": "Cricket Highlights", "primary_genre": "Sports",
         "total_views": "55000", "popularity_probability": "0.92", "prediction": "1"},
        {"title": "Mystery Manor S2", "primary_genre": "Series",
         "total_views": "62000", "popularity_probability": "0.84", "prediction": "1"},
    ]),
]

# Per-table COUNT(*) responses used by pipeline_health's per-table loop.
_CANNED_COUNTS: dict[str, str] = {
    "daily_active_users": "120",
    "content_watch_metrics": "8400",
    "genre_popularity": "24",
    "subscription_metrics": "3",
    "content_ratings_summary": "8400",
    "user_churn_prediction": "16000",
    "user_recommendations": "160000",
    "content_popularity_prediction": "8400",
}


def _canned_athena(sql: str) -> list[dict]:
    """Return canned rows for an Athena SQL string.

    Walks _CANNED_ATHENA in order; first match wins. Specific aggregation
    patterns are listed before bare table names so they take priority.
    """
    upper = sql.upper()
    if "SELECT COUNT(*)" in upper:
        for table, count in _CANNED_COUNTS.items():
            if table in sql:
                return [{"cnt": count}]
        return [{"cnt": "0"}]
    for key, rows in _CANNED_ATHENA:
        if key in sql:
            return rows
    return []


def _canned_pg(sql: str) -> list[dict]:
    """Return canned rows for the live PostgreSQL streaming feed."""
    upper = sql.upper()
    if "GROUP BY GENRE" in upper:
        return [
            {"genre": "Sports", "events": 1820, "users": 540, "avg_watch": 312.4},
            {"genre": "Drama", "events": 980, "users": 410, "avg_watch": 421.0},
        ]
    if "GROUP BY CITY" in upper:
        return [
            {"city": "Mumbai", "events": 1240, "users": 380},
            {"city": "Bengaluru", "events": 970, "users": 312},
        ]
    return [{
        "total_events": 4200, "unique_users": 1180, "unique_content": 72,
        "avg_watch_sec": 285.7, "events_last_min": 68, "events_last_5min": 312,
    }]


_original_athena = None
_original_pg = None


def apply_canned_datasource() -> None:
    """Monkeypatch the datasource module so all DB calls return canned rows.

    Idempotent — calling twice has no further effect. Use restore_datasource()
    to revert (mainly for tests). In a Streamlit Cloud deploy this is called
    once at app startup and the patch lives for the process lifetime.
    """
    global _original_athena, _original_pg
    if _original_athena is None:
        _original_athena = datasources.run_athena_sql
        datasources.run_athena_sql = _canned_athena  # type: ignore[assignment]
    if _original_pg is None:
        _original_pg = datasources.run_pg_sql
        datasources.run_pg_sql = _canned_pg  # type: ignore[assignment]


def restore_datasource() -> None:
    """Revert apply_canned_datasource(). Useful in tests; harmless if unpatched."""
    global _original_athena, _original_pg
    if _original_athena is not None:
        datasources.run_athena_sql = _original_athena  # type: ignore[assignment]
        _original_athena = None
    if _original_pg is not None:
        datasources.run_pg_sql = _original_pg  # type: ignore[assignment]
        _original_pg = None
