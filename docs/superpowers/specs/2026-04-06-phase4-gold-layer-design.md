# Phase 4: Gold Layer Design

**Date:** 2026-04-06
**Status:** Approved
**Depends on:** Phase 3 (Silver layer) — complete (20/20 validation on EC2/S3)

---

## 1. Overview

Phase 4 creates business-ready aggregated Gold tables from the Silver layer. Gold is the analytics tier: GROUP BY, JOIN, explode, and aggregate to produce KPIs and metrics consumed by dashboards, ML models, and AI agents. All Gold tables are written to S3 in Delta format using overwrite mode.

## 2. Architecture

```
Silver (Clean)                         Gold (Analytics)
├── silver_viewing_events        →     gold_daily_active_users
├── silver_viewing_events        →     gold_user_engagement
│   + silver_content_enriched    →     gold_content_watch_metrics
│   + silver_content_enriched    →     gold_genre_popularity
├── silver_subscriptions         →     gold_subscription_metrics
└── silver_ratings_clean         →     gold_content_ratings_summary
    + silver_content_enriched
```

Storage: `s3://jiohotstar-lakehouse-868896905478/gold/`

Write strategy: **overwrite** for all tables. Gold is a full recompute each run — same rationale as Silver (snapshot architecture, not incremental).

## 3. Project Structure (New Files)

```
spark/
├── gold/
│   ├── __init__.py
│   └── transforms/
│       ├── __init__.py
│       ├── gold_daily_active_users.py
│       ├── gold_content_watch_metrics.py
│       ├── gold_genre_popularity.py
│       ├── gold_user_engagement.py
│       ├── gold_subscription_metrics.py
│       └── gold_content_ratings_summary.py
└── jobs/
    ├── run_gold.py              # Orchestrator (shared SparkSession)
    └── validate_gold.py         # Validation checks
```

## 4. Gold Table Specifications

### 4.1 gold_daily_active_users

**Source:** `silver/viewing_events`

**Logic:** `GROUP BY event_date`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| event_date | DATE | PK |
| daily_active_users | LONG | COUNT(DISTINCT user_id) |
| total_views | LONG | COUNT(*) |
| total_watch_time_sec | DOUBLE | SUM(watch_duration_sec) |
| _gold_processed_at | TIMESTAMP | |

### 4.2 gold_content_watch_metrics

**Source:** `silver/viewing_events` INNER JOIN `silver/content_enriched` ON `content_id`

**Logic:** `GROUP BY content_id, title, content_type`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| content_id | STRING | PK |
| title | STRING | from content_enriched |
| content_type | STRING | from content_enriched |
| total_views | LONG | COUNT(*) |
| unique_viewers | LONG | COUNT(DISTINCT user_id) |
| total_watch_time_sec | DOUBLE | SUM(watch_duration_sec) |
| avg_watch_time_sec | DOUBLE | AVG(watch_duration_sec) |
| _gold_processed_at | TIMESTAMP | |

### 4.3 gold_genre_popularity

**Source:** `silver/viewing_events` INNER JOIN `silver/content_enriched` ON `content_id`, then `explode(genre_array)`

**Logic:** Join events to content, explode genre_array into individual rows, `GROUP BY genre`. Each view counts fully toward every genre tagged on that content (industry standard).

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| genre | STRING | PK |
| total_views | LONG | COUNT(*) |
| unique_viewers | LONG | COUNT(DISTINCT user_id) |
| total_watch_time_sec | DOUBLE | SUM(watch_duration_sec) |
| _gold_processed_at | TIMESTAMP | |

### 4.4 gold_user_engagement

**Source:** `silver/viewing_events`

**Logic:** `GROUP BY user_id`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| user_id | STRING | PK |
| total_sessions | LONG | COUNT(DISTINCT session_id) |
| total_views | LONG | COUNT(*) |
| total_watch_time_sec | DOUBLE | SUM(watch_duration_sec) |
| avg_session_watch_sec | DOUBLE | total_watch_time / total_sessions |
| distinct_content_watched | LONG | COUNT(DISTINCT content_id) |
| _gold_processed_at | TIMESTAMP | |

### 4.5 gold_subscription_metrics

**Source:** `silver/subscriptions`

**Logic:** `GROUP BY plan_id`. For `top_cancel_reason`: filter cancelled subs, group by cancel_reason, take most frequent per plan.

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| plan_id | STRING | PK |
| total_subscriptions | LONG | COUNT(*) |
| active_subscriptions | LONG | COUNT where is_active=true |
| cancelled_subscriptions | LONG | COUNT where is_active=false |
| avg_duration_days | DOUBLE | AVG(duration_days) |
| top_cancel_reason | STRING | mode of cancel_reason for cancelled |
| _gold_processed_at | TIMESTAMP | |

### 4.6 gold_content_ratings_summary

**Source:** `silver/ratings_clean` INNER JOIN `silver/content_enriched` ON `content_id`

**Logic:** `GROUP BY content_id, title, content_type`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| content_id | STRING | PK |
| title | STRING | from content_enriched |
| content_type | STRING | from content_enriched |
| avg_rating | DOUBLE | AVG(rating_value) |
| rating_count | LONG | COUNT(*) |
| min_rating | DOUBLE | MIN(rating_value) |
| max_rating | DOUBLE | MAX(rating_value) |
| _gold_processed_at | TIMESTAMP | |

## 5. Execution Strategy

**Pattern:** Hybrid — individual transform modules + single orchestrator with shared SparkSession (same as Silver).

**Execution order:** No inter-Gold dependencies — all read from Silver only. Order is arbitrary.

1. `gold_daily_active_users(spark)`
2. `gold_content_watch_metrics(spark)`
3. `gold_genre_popularity(spark)`
4. `gold_user_engagement(spark)`
5. `gold_subscription_metrics(spark)`
6. `gold_content_ratings_summary(spark)`

**CLI:** `python3 -m spark.jobs.run_gold`

## 6. Validation (`validate_gold.py`)

| # | Check | Logic |
|---|-------|-------|
| 1 | gold_daily_active_users row count > 0 | not empty |
| 2 | gold_daily_active_users no null event_date | 0 nulls |
| 3 | gold_content_watch_metrics row count > 0 | not empty |
| 4 | gold_content_watch_metrics total_views > 0 | MIN(total_views) >= 1 |
| 5 | gold_genre_popularity row count > 0 | not empty |
| 6 | gold_genre_popularity no null genre | 0 nulls |
| 7 | gold_user_engagement row count > 0 | not empty |
| 8 | gold_user_engagement no null user_id | 0 nulls |
| 9 | gold_subscription_metrics row count == distinct plan_ids | matches plan count |
| 10 | gold_content_ratings_summary row count > 0 | not empty |
| 11 | gold_content_ratings_summary avg_rating in 0-5 | range check |
| 12 | All gold tables have _gold_processed_at | schema check |
| 13 | All gold tables _delta_log exists | Delta format check |

**CLI:** `python3 -m spark.jobs.validate_gold`

## 7. Spark Configuration

Reuses existing `spark/conf/spark_settings.py`. New helper:

```python
def get_gold_path(table_name):
    base = get_lakehouse_path()
    return f"{base}/gold/{table_name}"
```

## 8. Dependencies

- **Python:** PySpark 3.5.1, delta-spark 3.1.0 (already installed)
- **Infrastructure:** Same EC2 + S3 from Phase 2-3 (no new AWS resources)
- **Data:** Silver tables must exist and be populated (validated by Phase 3)
