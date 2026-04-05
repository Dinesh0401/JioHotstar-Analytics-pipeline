# Phase 3: Silver Layer Design

**Date:** 2026-04-05
**Status:** Draft
**Depends on:** Phase 2 (Bronze ingestion) — complete (29/29 validation on EC2/S3)

---

## 1. Overview

Phase 3 transforms raw Bronze Delta tables into clean, normalized Silver tables. Silver is where real data engineering happens: type casting, deduplication, joins, schema enforcement, and derived columns. All Silver tables are written to S3 in Delta format using overwrite mode.

## 2. Architecture

```
Bronze (Raw Snapshots)                Silver (Clean + Normalized)
├── users                       →     silver_users
├── subscriptions               →     silver_subscriptions
├── content + content_metadata  →     silver_content_enriched
├── ratings                     →     silver_ratings_clean
├── viewing_events_batch        →     silver_viewing_events
│   + viewing_events (Kafka)
└── campaigns                   →     silver_campaigns
```

Storage: `s3://jiohotstar-lakehouse-868896905478/silver/`

Write strategy: **overwrite** for all tables. Bronze tables are snapshot ingestions, so Silver is a full recompute each run. Production systems would use Delta MERGE INTO for incremental processing.

## 3. Project Structure (New Files)

```
spark/
├── silver/
│   ├── __init__.py
│   └── transforms/
│       ├── __init__.py
│       ├── transform_users.py
│       ├── transform_subscriptions.py
│       ├── transform_content.py
│       ├── transform_ratings.py
│       ├── transform_events.py
│       └── transform_campaigns.py
└── jobs/
    ├── run_silver.py          # Orchestrator (shared SparkSession)
    └── validate_silver.py     # Validation checks
```

## 4. Silver Table Specifications

### 4.1 silver_users

**Source:** `bronze/users`

**Transformations:**
- Extract `device_os` from `user_agent` using regex (iOS, Android, Windows, macOS, Linux, Other)
- Normalize `country` to uppercase via `upper()`
- Add `is_valid_email` boolean: regex check for basic email pattern
- Drop `user_agent` column (raw telemetry, not needed in Silver)
- Drop Bronze audit columns (`_ingested_at`, `_source`)
- Add `_silver_processed_at` timestamp

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| user_id | STRING | PK |
| email | STRING | |
| is_valid_email | BOOLEAN | regex validated |
| full_name | STRING | |
| signup_date | DATE | |
| device_category | STRING | |
| device_os | STRING | extracted from user_agent |
| country | STRING | uppercased |
| created_at | TIMESTAMP | |
| _silver_processed_at | TIMESTAMP | |

### 4.2 silver_subscriptions

**Source:** `bronze/subscriptions`

**Transformations:**
- Compute `is_active`: `end_date IS NULL OR end_date >= current_date()`
- Compute `duration_days`: `datediff(coalesce(end_date, current_date()), start_date)`
- Fill null `cancel_reason` with `"active"`
- Drop Bronze audit columns
- Add `_silver_processed_at`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| subscription_id | STRING | PK |
| user_id | STRING | FK |
| plan_id | STRING | |
| start_date | DATE | |
| end_date | DATE | nullable |
| cancel_reason | STRING | "active" if null |
| is_active | BOOLEAN | computed |
| duration_days | INTEGER | computed |
| created_at | TIMESTAMP | |
| _silver_processed_at | TIMESTAMP | |

### 4.3 silver_content_enriched

**Source:** `bronze/content` LEFT JOIN `bronze/content_metadata` ON `content_id`

**Transformations:**
- Merge `runtime_value` + `runtime_unit` into `runtime_minutes`:
  - `when(runtime_unit == "seconds", runtime_value / 60).otherwise(runtime_value)`
  - Cast to integer
- Split `genre` string into `genre_array` (array<string>) by comma delimiter
- Split `cast_members` string into array<string> by comma delimiter
- Bring in `budget` and `content_rating` from content_metadata
- Drop `runtime_value`, `runtime_unit` (replaced by `runtime_minutes`)
- Drop Bronze audit columns
- Add `_silver_processed_at`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| content_id | STRING | PK |
| title | STRING | |
| content_type | STRING | |
| genre_array | ARRAY<STRING> | split from genre |
| runtime_minutes | INTEGER | computed from value+unit |
| release_year | INTEGER | |
| language | STRING | |
| budget | LONG | from content_metadata |
| cast_members | ARRAY<STRING> | split from string |
| content_rating | STRING | from content_metadata |
| created_at | TIMESTAMP | |
| _silver_processed_at | TIMESTAMP | |

### 4.4 silver_ratings_clean

**Source:** `bronze/ratings`

**Transformations:**
- Dedup by `user_id + content_id`, keeping latest `rated_at`:
  ```
  ROW_NUMBER() OVER (PARTITION BY user_id, content_id ORDER BY rated_at DESC) = 1
  ```
- Validate `rating_value` in range 0.0-5.0, drop rows outside range
- Drop Bronze audit columns
- Add `_silver_processed_at`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| rating_id | INTEGER | |
| user_id | STRING | FK |
| content_id | STRING | FK |
| rating_value | DECIMAL(2,1) | 0.0-5.0 validated |
| rating_source | STRING | kept for ML |
| rated_at | TIMESTAMP | |
| _silver_processed_at | TIMESTAMP | |

### 4.5 silver_viewing_events

**Source:** `bronze/viewing_events_batch` UNION ALL `bronze/viewing_events`

**Transformations:**
- Align schemas before union (streaming table has `_kafka_partition`, `_kafka_offset` — drop these)
- Dedup by `event_id` (keep first occurrence)
- Cast `event_ts` from STRING to TIMESTAMP via `to_timestamp()`
- Cast `session_start_ts` from STRING to TIMESTAMP
- Compute `watch_duration_sec`: `watch_duration_ms / 1000` (double)
- Compute `event_date`: `to_date(event_ts)` for downstream partitioning
- Normalize `event_type` to lowercase
- Drop Bronze audit columns
- Add `_silver_processed_at`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| event_id | STRING | PK |
| user_id | STRING | FK |
| content_id | STRING | FK |
| session_id | STRING | |
| device_category | STRING | |
| event_type | STRING | lowercased |
| watch_duration_ms | LONG | original |
| watch_duration_sec | DOUBLE | computed |
| seek_position_ms | LONG | |
| event_ts | TIMESTAMP | cast from string |
| session_start_ts | TIMESTAMP | cast from string |
| event_date | DATE | derived from event_ts |
| referrer | STRING | |
| _silver_processed_at | TIMESTAMP | |

### 4.6 silver_campaigns

**Source:** `bronze/campaigns`

**Transformations:**
- Parse `budget` string ("USD 38988255"):
  - `budget_currency`: `split(budget, ' ')[0]` → STRING
  - `budget_amount`: `cast(split(budget, ' ')[1] as BIGINT)` → LONG
- Compute `campaign_duration_days`: `datediff(end_date, start_date)`
- Drop original `budget` column
- Drop Bronze audit columns
- Add `_silver_processed_at`

**Output schema:**
| Column | Type | Notes |
|--------|------|-------|
| campaign_id | STRING | PK |
| campaign_name | STRING | |
| budget_amount | LONG | parsed from string |
| budget_currency | STRING | parsed from string |
| start_date | DATE | cast from timestamp |
| end_date | DATE | cast from timestamp |
| campaign_duration_days | INTEGER | computed |
| _silver_processed_at | TIMESTAMP | |

## 5. Execution Strategy

**Pattern:** Hybrid — individual transform modules + single orchestrator with shared SparkSession.

**Execution order** (dimensions first, then facts):
1. `transform_users(spark)` → `silver/users/`
2. `transform_subscriptions(spark)` → `silver/subscriptions/`
3. `transform_content_enriched(spark)` → `silver/content_enriched/`
4. `transform_campaigns(spark)` → `silver/campaigns/`
5. `transform_ratings_clean(spark)` → `silver/ratings_clean/`
6. `transform_viewing_events(spark)` → `silver/viewing_events/`

No inter-Silver dependencies exist — all transforms read only from Bronze.

**CLI:** `python3 -m spark.jobs.run_silver`

## 6. Validation (`validate_silver.py`)

Checks to implement:

| # | Check | Logic |
|---|-------|-------|
| 1 | silver_users row count | == bronze users count |
| 2 | silver_users no null user_id | count(null user_id) == 0 |
| 3 | silver_users is_valid_email populated | no nulls in column |
| 4 | silver_users device_os populated | no nulls in column |
| 5 | silver_subscriptions row count | == bronze count |
| 6 | silver_subscriptions duration_days no nulls | coalesce logic works |
| 7 | silver_content_enriched row count | == bronze content count (left join) |
| 8 | silver_content_enriched runtime_minutes no nulls | conversion worked |
| 9 | silver_content_enriched genre_array is array | check type |
| 10 | silver_ratings_clean dedup check | count <= bronze ratings count |
| 11 | silver_ratings_clean no duplicate user+content | distinct check |
| 12 | silver_ratings_clean rating_value in 0-5 | min >= 0, max <= 5 |
| 13 | silver_viewing_events row count | >= batch count (union adds streaming) |
| 14 | silver_viewing_events no duplicate event_id | distinct check |
| 15 | silver_viewing_events event_ts is timestamp | schema check |
| 16 | silver_viewing_events event_date populated | no nulls |
| 17 | silver_campaigns budget_amount is numeric | no nulls, > 0 |
| 18 | silver_campaigns budget_currency populated | no nulls |
| 19 | All silver tables have _silver_processed_at | schema check |
| 20 | All silver tables _delta_log exists | Delta format check |

**CLI:** `python3 -m spark.jobs.validate_silver`

## 7. Spark Configuration

Reuses existing `spark/conf/spark_settings.py`. New helper:

```python
def get_silver_path(table_name):
    base = get_lakehouse_path()
    return f"{base}/silver/{table_name}"
```

No new Spark packages required — Silver uses only PySpark built-in functions.

## 8. Dependencies

- **Python:** PySpark 3.5.1, delta-spark 3.1.0 (already installed)
- **Infrastructure:** Same EC2 + S3 from Phase 2 (no new AWS resources)
- **Data:** Bronze tables must exist and be populated (validated by Phase 2)
