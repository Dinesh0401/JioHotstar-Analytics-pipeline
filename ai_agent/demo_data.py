"""Canned datasource for offline / Streamlit Cloud demo mode.

Replaces ``datasources.run_athena_sql`` / ``run_pg_sql`` with deterministic
in-memory results so the runtime works with zero AWS credentials. The engine,
brains, registry, and tools all run for real — only the data is canned, so the
reasoning trace is genuine.

The dataset is intentionally JioHotstar-flavoured: real-style content names,
six subscription plans, ten genres, plausible Indian-OTT scale numbers, with
long-tail distributions rather than flat synthetic rows.
"""

from __future__ import annotations

from ai_agent import datasources


# ── Athena canned rows ─────────────────────────────────────────────────────
# Ordered list of (sql-substring, rows) pairs. First match wins, so put more-
# specific patterns (aggregations, joins) BEFORE bare table names — otherwise
# a query like "FROM user_churn_prediction GROUP BY plan_id" would match the
# table-name key before the more specific aggregation key.

_CANNED_ATHENA: list[tuple[str, list[dict]]] = [

    # ── Aggregations (specific) ───────────────────────────────────────────

    # churn_risk by='plan' aggregation
    ("GROUP BY plan_id", [
        {"plan_id": "FREE",    "total_users": "12480000",
         "predicted_churners": "6864000", "avg_churn_prob": "0.55"},
        {"plan_id": "MOBILE",  "total_users": "8210000",
         "predicted_churners": "2627000", "avg_churn_prob": "0.32"},
        {"plan_id": "BASIC",   "total_users": "6040000",
         "predicted_churners": "1691000", "avg_churn_prob": "0.28"},
        {"plan_id": "FAMILY",  "total_users":  "812000",
         "predicted_churners":  "106000", "avg_churn_prob": "0.13"},
        {"plan_id": "PREMIUM", "total_users": "4530000",
         "predicted_churners":  "815000", "avg_churn_prob": "0.18"},
        {"plan_id": "VIP",     "total_users": "1210000",
         "predicted_churners":  "121000", "avg_churn_prob": "0.10"},
    ]),

    # ── Per-table fallbacks ───────────────────────────────────────────────

    ("subscription_metrics", [
        {"plan_id": "FREE",    "total_subscriptions": "12480000",
         "active_subscriptions": "8210000", "cancelled_subscriptions": "4270000",
         "top_cancel_reason": "ads"},
        {"plan_id": "MOBILE",  "total_subscriptions": "8210000",
         "active_subscriptions": "6540000", "cancelled_subscriptions": "1670000",
         "top_cancel_reason": "price"},
        {"plan_id": "BASIC",   "total_subscriptions": "6040000",
         "active_subscriptions": "4810000", "cancelled_subscriptions": "1230000",
         "top_cancel_reason": "price"},
        {"plan_id": "PREMIUM", "total_subscriptions": "4530000",
         "active_subscriptions": "3920000", "cancelled_subscriptions":  "610000",
         "top_cancel_reason": "content"},
        {"plan_id": "VIP",     "total_subscriptions": "1210000",
         "active_subscriptions": "1100000", "cancelled_subscriptions":  "110000",
         "top_cancel_reason": "quality"},
        {"plan_id": "FAMILY",  "total_subscriptions":  "812000",
         "active_subscriptions":  "725000", "cancelled_subscriptions":   "87000",
         "top_cancel_reason": "features"},
    ]),

    ("user_churn_prediction", [
        # Default churn_risk query returns highest-risk users (LIMIT 15).
        {"user_id": "USR-1000284", "plan_id": "FREE",
         "churn_probability": "0.94", "prediction": "1"},
        {"user_id": "USR-1004921", "plan_id": "FREE",
         "churn_probability": "0.92", "prediction": "1"},
        {"user_id": "USR-1018330", "plan_id": "MOBILE",
         "churn_probability": "0.89", "prediction": "1"},
        {"user_id": "USR-1009742", "plan_id": "FREE",
         "churn_probability": "0.88", "prediction": "1"},
        {"user_id": "USR-1023115", "plan_id": "BASIC",
         "churn_probability": "0.86", "prediction": "1"},
        {"user_id": "USR-1031408", "plan_id": "FREE",
         "churn_probability": "0.85", "prediction": "1"},
        {"user_id": "USR-1041209", "plan_id": "MOBILE",
         "churn_probability": "0.83", "prediction": "1"},
        {"user_id": "USR-1052911", "plan_id": "BASIC",
         "churn_probability": "0.81", "prediction": "1"},
        {"user_id": "USR-1067302", "plan_id": "FREE",
         "churn_probability": "0.80", "prediction": "1"},
        {"user_id": "USR-1078441", "plan_id": "MOBILE",
         "churn_probability": "0.78", "prediction": "1"},
        {"user_id": "USR-1089015", "plan_id": "FREE",
         "churn_probability": "0.77", "prediction": "1"},
        {"user_id": "USR-1093327", "plan_id": "BASIC",
         "churn_probability": "0.75", "prediction": "1"},
        {"user_id": "USR-1102218", "plan_id": "MOBILE",
         "churn_probability": "0.73", "prediction": "1"},
        {"user_id": "USR-1115608", "plan_id": "FREE",
         "churn_probability": "0.72", "prediction": "1"},
        {"user_id": "USR-1128844", "plan_id": "BASIC",
         "churn_probability": "0.70", "prediction": "1"},
    ]),

    ("content_watch_metrics", [
        {"title": "World Cup Final 2024",       "content_type": "Sports",
         "total_views": "12450000", "unique_viewers": "8420000"},
        {"title": "IPL Final 2025",             "content_type": "Sports",
         "total_views":  "9820000", "unique_viewers": "7110000"},
        {"title": "Pathaan",                    "content_type": "Movie",
         "total_views":  "5410000", "unique_viewers": "4790000"},
        {"title": "Mumbai Diaries S2",          "content_type": "Series",
         "total_views":  "4250000", "unique_viewers": "2890000"},
        {"title": "Animal",                     "content_type": "Movie",
         "total_views":  "3820000", "unique_viewers": "3480000"},
        {"title": "The Family Man S3",          "content_type": "Series",
         "total_views":  "3610000", "unique_viewers": "2620000"},
        {"title": "Jawan",                      "content_type": "Movie",
         "total_views":  "3380000", "unique_viewers": "3020000"},
        {"title": "Champions Trophy Highlights","content_type": "Sports",
         "total_views":  "3140000", "unique_viewers": "2410000"},
        {"title": "Panchayat S3",               "content_type": "Series",
         "total_views":  "2840000", "unique_viewers": "1920000"},
        {"title": "Rocky Aur Rani Kii Prem Kahaani", "content_type": "Movie",
         "total_views":  "2510000", "unique_viewers": "2220000"},
    ]),

    ("genre_popularity", [
        {"genre": "Sports",      "total_views": "48200000", "unique_viewers": "18400000"},
        {"genre": "Drama",       "total_views": "32100000", "unique_viewers": "14200000"},
        {"genre": "Action",      "total_views": "28400000", "unique_viewers": "12100000"},
        {"genre": "Comedy",      "total_views": "21300000", "unique_viewers":  "9180000"},
        {"genre": "Romance",     "total_views": "18100000", "unique_viewers":  "8240000"},
        {"genre": "Thriller",    "total_views": "16200000", "unique_viewers":  "7090000"},
        {"genre": "Family",      "total_views": "11400000", "unique_viewers":  "5080000"},
        {"genre": "Kids",        "total_views":  "9180000", "unique_viewers":  "4120000"},
        {"genre": "Documentary", "total_views":  "6240000", "unique_viewers":  "3040000"},
        {"genre": "Reality",     "total_views":  "5110000", "unique_viewers":  "2580000"},
    ]),

    ("content_ratings_summary", [
        {"title": "The Family Man S3",  "content_type": "Series",
         "avg_rating": "4.8", "rating_count": "38420"},
        {"title": "Panchayat S3",       "content_type": "Series",
         "avg_rating": "4.7", "rating_count": "28330"},
        {"title": "Mumbai Diaries S2",  "content_type": "Series",
         "avg_rating": "4.7", "rating_count": "22150"},
        {"title": "World Cup Final 2024","content_type": "Sports",
         "avg_rating": "4.6", "rating_count": "48910"},
        {"title": "Jawan",              "content_type": "Movie",
         "avg_rating": "4.6", "rating_count": "41280"},
        {"title": "Pathaan",            "content_type": "Movie",
         "avg_rating": "4.5", "rating_count": "36810"},
        {"title": "Special Ops 2",      "content_type": "Series",
         "avg_rating": "4.4", "rating_count": "15240"},
        {"title": "Rocky Aur Rani Kii Prem Kahaani", "content_type": "Movie",
         "avg_rating": "4.4", "rating_count": "18420"},
        {"title": "Scoop",              "content_type": "Series",
         "avg_rating": "4.3", "rating_count":  "9820"},
        {"title": "Animal",             "content_type": "Movie",
         "avg_rating": "4.2", "rating_count": "28140"},
    ]),

    ("daily_active_users", [
        {"event_date": "2026-05-20", "daily_active_users": "33820000", "total_views": "148200000"},
        {"event_date": "2026-05-19", "daily_active_users": "32100000", "total_views": "141500000"},
        {"event_date": "2026-05-18", "daily_active_users": "41280000", "total_views": "184600000"},
        {"event_date": "2026-05-17", "daily_active_users": "39940000", "total_views": "179300000"},
        {"event_date": "2026-05-16", "daily_active_users": "31840000", "total_views": "140100000"},
        {"event_date": "2026-05-15", "daily_active_users": "30620000", "total_views": "135800000"},
        {"event_date": "2026-05-14", "daily_active_users": "29980000", "total_views": "132400000"},
        {"event_date": "2026-05-13", "daily_active_users": "31410000", "total_views": "138900000"},
        {"event_date": "2026-05-12", "daily_active_users": "32970000", "total_views": "146100000"},
        {"event_date": "2026-05-11", "daily_active_users": "42850000", "total_views": "192700000"},
        {"event_date": "2026-05-10", "daily_active_users": "41020000", "total_views": "186400000"},
        {"event_date": "2026-05-09", "daily_active_users": "32440000", "total_views": "143200000"},
        {"event_date": "2026-05-08", "daily_active_users": "31180000", "total_views": "137800000"},
        {"event_date": "2026-05-07", "daily_active_users": "30640000", "total_views": "135100000"},
        {"event_date": "2026-05-06", "daily_active_users": "31750000", "total_views": "140200000"},
    ]),

    ("user_recommendations", [
        {"recommended_content": "The Family Man S3",   "content_type": "Series",
         "recommendation_rank": "1",  "score": "0.94"},
        {"recommended_content": "Mumbai Diaries S2",   "content_type": "Series",
         "recommendation_rank": "2",  "score": "0.91"},
        {"recommended_content": "Pathaan",             "content_type": "Movie",
         "recommendation_rank": "3",  "score": "0.89"},
        {"recommended_content": "Special Ops 2",       "content_type": "Series",
         "recommendation_rank": "4",  "score": "0.87"},
        {"recommended_content": "World Cup Final 2024","content_type": "Sports",
         "recommendation_rank": "5",  "score": "0.85"},
        {"recommended_content": "Panchayat S3",        "content_type": "Series",
         "recommendation_rank": "6",  "score": "0.83"},
        {"recommended_content": "Jawan",               "content_type": "Movie",
         "recommendation_rank": "7",  "score": "0.81"},
        {"recommended_content": "Scoop",               "content_type": "Series",
         "recommendation_rank": "8",  "score": "0.78"},
        {"recommended_content": "Animal",              "content_type": "Movie",
         "recommendation_rank": "9",  "score": "0.76"},
        {"recommended_content": "Rocky Aur Rani Kii Prem Kahaani", "content_type": "Movie",
         "recommendation_rank": "10", "score": "0.74"},
    ]),

    ("content_popularity_prediction", [
        {"title": "Pathaan 2",             "primary_genre": "Action",
         "total_views": "8920000", "popularity_probability": "0.96", "prediction": "1"},
        {"title": "IPL 2027 Coverage",     "primary_genre": "Sports",
         "total_views": "7240000", "popularity_probability": "0.94", "prediction": "1"},
        {"title": "The Family Man S4",     "primary_genre": "Thriller",
         "total_views": "5180000", "popularity_probability": "0.92", "prediction": "1"},
        {"title": "Mumbai Diaries S3",     "primary_genre": "Drama",
         "total_views": "4820000", "popularity_probability": "0.90", "prediction": "1"},
        {"title": "Tiger 4",               "primary_genre": "Action",
         "total_views": "4310000", "popularity_probability": "0.88", "prediction": "1"},
        {"title": "Panchayat S4",          "primary_genre": "Comedy",
         "total_views": "3940000", "popularity_probability": "0.86", "prediction": "1"},
        {"title": "Animal Park",           "primary_genre": "Drama",
         "total_views": "3520000", "popularity_probability": "0.84", "prediction": "1"},
        {"title": "Scoop S2",              "primary_genre": "Drama",
         "total_views": "2810000", "popularity_probability": "0.78", "prediction": "1"},
        {"title": "Special Ops 3",         "primary_genre": "Thriller",
         "total_views": "2410000", "popularity_probability": "0.75", "prediction": "1"},
        {"title": "Champions Trophy 2027", "primary_genre": "Sports",
         "total_views": "2180000", "popularity_probability": "0.73", "prediction": "1"},
    ]),
]


# Per-table COUNT(*) responses for the pipeline_health per-table loop.
# Scale chosen to feel like a real Hotstar lakehouse — millions of users,
# tens of thousands of content items, hundreds of millions of recommendations.
_CANNED_COUNTS: dict[str, str] = {
    "daily_active_users":             "380",
    "content_watch_metrics":          "84230",
    "genre_popularity":               "32",
    "subscription_metrics":           "6",
    "content_ratings_summary":        "84230",
    "user_churn_prediction":          "32580000",
    "user_recommendations":           "325800000",
    "content_popularity_prediction":  "84230",
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


# ── PostgreSQL canned rows (live streaming feed) ───────────────────────────

def _canned_pg(sql: str) -> list[dict]:
    """Return canned rows for the live PostgreSQL streaming feed."""
    upper = sql.upper()
    if "GROUP BY GENRE" in upper:
        return [
            {"genre": "Sports",   "events": 92400, "users": 28100, "avg_watch": 412.4},
            {"genre": "Drama",    "events": 58200, "users": 19400, "avg_watch": 384.1},
            {"genre": "Action",   "events": 51800, "users": 17200, "avg_watch": 296.7},
            {"genre": "Comedy",   "events": 38900, "users": 13800, "avg_watch": 271.5},
            {"genre": "Romance",  "events": 31200, "users": 11400, "avg_watch": 322.8},
            {"genre": "Thriller", "events": 27600, "users":  9800, "avg_watch": 358.2},
            {"genre": "Family",   "events": 21400, "users":  8100, "avg_watch": 248.0},
            {"genre": "Kids",     "events": 18900, "users":  7300, "avg_watch": 192.4},
        ]
    if "GROUP BY CITY" in upper:
        return [
            {"city": "Mumbai",    "events": 48200, "users": 14820},
            {"city": "Delhi",     "events": 41100, "users": 12640},
            {"city": "Bengaluru", "events": 37900, "users": 11580},
            {"city": "Hyderabad", "events": 27300, "users":  8420},
            {"city": "Chennai",   "events": 22100, "users":  7100},
            {"city": "Kolkata",   "events": 18800, "users":  6240},
            {"city": "Pune",      "events": 16200, "users":  5280},
            {"city": "Ahmedabad", "events": 12400, "users":  4180},
        ]
    return [{
        "total_events": 285420,
        "unique_users":  84120,
        "unique_content":   412,
        "avg_watch_sec":  285.7,
        "events_last_min":  9180,
        "events_last_5min": 47210,
    }]


# ── Public monkey-patch helpers ────────────────────────────────────────────

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
