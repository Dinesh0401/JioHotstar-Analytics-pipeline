# Phase 3: Silver Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform raw Bronze Delta tables into clean, normalized Silver Delta tables with type casting, deduplication, joins, and derived columns.

**Architecture:** 6 Silver transform modules read from Bronze, apply transformations, and write Delta tables in overwrite mode. A single orchestrator runs all transforms with a shared SparkSession. A validation script runs 20 checks against the output.

**Tech Stack:** PySpark 3.5.1, Delta Lake 3.1.0 OSS, AWS S3 (s3a://), Python 3.x

**Spec:** `docs/superpowers/specs/2026-04-05-phase3-silver-layer-design.md`

---

## File Structure

```
spark/
├── conf/
│   └── spark_settings.py          # MODIFY: add get_silver_path()
├── silver/
│   ├── __init__.py                # CREATE: empty package init
│   └── transforms/
│       ├── __init__.py            # CREATE: empty package init
│       ├── transform_users.py     # CREATE: silver_users transform
│       ├── transform_subscriptions.py  # CREATE: silver_subscriptions
│       ├── transform_content.py   # CREATE: silver_content_enriched
│       ├── transform_ratings.py   # CREATE: silver_ratings_clean
│       ├── transform_events.py    # CREATE: silver_viewing_events
│       └── transform_campaigns.py # CREATE: silver_campaigns
└── jobs/
    ├── run_silver.py              # CREATE: orchestrator
    └── validate_silver.py         # CREATE: 20 validation checks
```

---

### Task 1: Add `get_silver_path()` helper

**Files:**
- Modify: `spark/conf/spark_settings.py:70-72`

- [ ] **Step 1: Add the helper function**

Add `get_silver_path` right after `get_bronze_path` in `spark/conf/spark_settings.py`:

```python
def get_silver_path(table_name):
    base = get_lakehouse_path()
    return f"{base}/silver/{table_name}"
```

- [ ] **Step 2: Verify the import works**

Run:
```bash
python -c "from spark.conf.spark_settings import get_silver_path; print(get_silver_path('users'))"
```

Expected: prints the lakehouse path ending in `/silver/users`

- [ ] **Step 3: Commit**

```bash
git add spark/conf/spark_settings.py
git commit -m "feat: add get_silver_path helper to spark_settings"
```

---

### Task 2: Create Silver package structure

**Files:**
- Create: `spark/silver/__init__.py`
- Create: `spark/silver/transforms/__init__.py`

- [ ] **Step 1: Create the package directories and init files**

Create `spark/silver/__init__.py` — empty file:
```python
```

Create `spark/silver/transforms/__init__.py` — empty file:
```python
```

- [ ] **Step 2: Verify the package imports**

Run:
```bash
python -c "import spark.silver; import spark.silver.transforms; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add spark/silver/__init__.py spark/silver/transforms/__init__.py
git commit -m "feat: create silver package structure"
```

---

### Task 3: transform_users.py

**Files:**
- Create: `spark/silver/transforms/transform_users.py`

**Bronze schema (users):** user_id, email, full_name, signup_date, device_category, user_agent, country, created_at, _ingested_at, _source

**Key data notes:**
- `user_agent` values include full UA strings like `"Mozilla/5.0 (iPhone; CPU iPhone OS 17_4..."`, short strings like `"Android"`, `"iPhone"`, `"Chrome"`, `"Desktop"`, `"Fire TV"`, `"SmartTV"`, and app-specific like `"JioHotstar/6.1.2 (Android 14; OnePlus 12)"`
- `country` values: "India", "United States", etc. — need `upper()`
- Email: standard emails, some with `+tag` suffixes

- [ ] **Step 1: Write the transform module**

Create `spark/silver/transforms/transform_users.py`:

```python
"""Silver transform: users — clean, extract device_os, validate email."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
    upper,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_users(spark):
    """Read bronze/users, apply Silver transformations, write to silver/users."""
    print("  Transforming silver_users...")

    df = spark.read.format("delta").load(get_bronze_path("users"))

    # Extract device_os from user_agent string
    df = df.withColumn(
        "device_os",
        when(col("user_agent").rlike("(?i)iphone|ios"), "iOS")
        .when(col("user_agent").rlike("(?i)android"), "Android")
        .when(col("user_agent").rlike("(?i)windows"), "Windows")
        .when(col("user_agent").rlike("(?i)macintosh|mac os"), "macOS")
        .when(col("user_agent").rlike("(?i)linux|tizen"), "Linux")
        .otherwise("Other"),
    )

    # Normalize country to uppercase
    df = df.withColumn("country", upper(col("country")))

    # Validate email with basic regex
    df = df.withColumn(
        "is_valid_email",
        col("email").rlike("^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"),
    )

    # Select final columns, dropping user_agent and bronze audit columns
    result = df.select(
        "user_id",
        "email",
        "is_valid_email",
        "full_name",
        "signup_date",
        "device_category",
        "device_os",
        "country",
        "created_at",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("users")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/users: {count} rows -> {output_path}")
    return count
```

- [ ] **Step 2: Smoke test the transform locally**

Run (requires Docker containers running for Bronze data):
```bash
python -c "
from spark.conf.spark_settings import get_spark_session
from spark.silver.transforms.transform_users import transform_users
spark = get_spark_session('Silver-Test')
count = transform_users(spark)
print(f'Wrote {count} rows')
spark.stop()
"
```

Expected: prints `Wrote 5000 rows` (matches bronze users count)

- [ ] **Step 3: Commit**

```bash
git add spark/silver/transforms/transform_users.py
git commit -m "feat: add silver transform for users"
```

---

### Task 4: transform_subscriptions.py

**Files:**
- Create: `spark/silver/transforms/transform_subscriptions.py`

**Bronze schema (subscriptions):** subscription_id, user_id, plan_id, start_date, end_date, cancel_reason, created_at, _ingested_at, _source

**Key data notes:**
- `end_date` is NULL for active subscriptions
- `cancel_reason` is NULL for active subscriptions, or one of many strings including Hindi phrases
- Multiple subscriptions per user (1-3)

- [ ] **Step 1: Write the transform module**

Create `spark/silver/transforms/transform_subscriptions.py`:

```python
"""Silver transform: subscriptions — compute is_active, duration_days."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    coalesce,
    col,
    current_date,
    current_timestamp,
    datediff,
    lit,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_subscriptions(spark):
    """Read bronze/subscriptions, apply Silver transformations, write to silver/subscriptions."""
    print("  Transforming silver_subscriptions...")

    df = spark.read.format("delta").load(get_bronze_path("subscriptions"))

    # Compute is_active: end_date is null OR end_date >= today
    df = df.withColumn(
        "is_active",
        when(col("end_date").isNull(), True)
        .when(col("end_date") >= current_date(), True)
        .otherwise(False),
    )

    # Compute duration_days: datediff(coalesce(end_date, today), start_date)
    df = df.withColumn(
        "duration_days",
        datediff(coalesce(col("end_date"), current_date()), col("start_date")).cast("int"),
    )

    # Fill null cancel_reason with "active"
    df = df.withColumn(
        "cancel_reason",
        coalesce(col("cancel_reason"), lit("active")),
    )

    # Select final columns, dropping bronze audit columns
    result = df.select(
        "subscription_id",
        "user_id",
        "plan_id",
        "start_date",
        "end_date",
        "cancel_reason",
        "is_active",
        "duration_days",
        "created_at",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("subscriptions")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/subscriptions: {count} rows -> {output_path}")
    return count
```

- [ ] **Step 2: Smoke test**

Run:
```bash
python -c "
from spark.conf.spark_settings import get_spark_session
from spark.silver.transforms.transform_subscriptions import transform_subscriptions
spark = get_spark_session('Silver-Test')
count = transform_subscriptions(spark)
print(f'Wrote {count} rows')
spark.stop()
"
```

Expected: prints row count (6000-8000 range, matching bronze subscriptions)

- [ ] **Step 3: Commit**

```bash
git add spark/silver/transforms/transform_subscriptions.py
git commit -m "feat: add silver transform for subscriptions"
```

---

### Task 5: transform_content.py

**Files:**
- Create: `spark/silver/transforms/transform_content.py`

**Bronze schemas:**
- **content** (from PostgreSQL): content_id, title, content_type, genre, runtime_value, runtime_unit, release_year, language, created_at, _ingested_at, _source
- **content_metadata** (from CSV): content_id, budget, cast_members, content_rating, _ingested_at, _source

**Key data notes:**
- `genre` uses MIXED separators: `","`, `", "`, and `"|"` — must split on all three
- `cast_members` uses `"|"` as separator
- `runtime_unit` is either `"seconds"` (80% of data) or `"minutes"` (20%)
- `runtime_value` in seconds ranges from 300-14400; in minutes from 5-240
- `budget` in content_metadata is an integer (not the string format — that's campaigns)

- [ ] **Step 1: Write the transform module**

Create `spark/silver/transforms/transform_content.py`:

```python
"""Silver transform: content enriched — join content + metadata, normalize runtime/genre/cast."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    split,
    trim,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_content_enriched(spark):
    """Read bronze/content + bronze/content_metadata, join and transform to silver/content_enriched."""
    print("  Transforming silver_content_enriched...")

    content = spark.read.format("delta").load(get_bronze_path("content"))
    metadata = spark.read.format("delta").load(get_bronze_path("content_metadata"))

    # LEFT JOIN on content_id — drop metadata audit columns before join
    metadata_clean = metadata.select(
        col("content_id").alias("meta_content_id"),
        "budget",
        "cast_members",
        "content_rating",
    )
    joined = content.join(metadata_clean, content["content_id"] == metadata_clean["meta_content_id"], "left") \
        .drop("meta_content_id")

    # Convert runtime to minutes: seconds -> divide by 60, minutes -> keep as-is
    joined = joined.withColumn(
        "runtime_minutes",
        when(col("runtime_unit") == "seconds", (col("runtime_value") / 60).cast("int"))
        .otherwise(col("runtime_value").cast("int")),
    )

    # Split genre on mixed separators: first replace all separators with comma, then split
    # Genre uses: "," or ", " or "|"
    from pyspark.sql.functions import regexp_replace
    joined = joined.withColumn(
        "genre_normalized", regexp_replace(col("genre"), r"\s*[,|]\s*", ",")
    )
    joined = joined.withColumn("genre_array", split(col("genre_normalized"), ","))

    # Split cast_members on "|"
    joined = joined.withColumn("cast_members", split(col("cast_members"), r"\|"))

    # Select final columns
    result = joined.select(
        "content_id",
        trim(col("title")).alias("title"),
        "content_type",
        "genre_array",
        "runtime_minutes",
        "release_year",
        "language",
        col("budget").cast("long").alias("budget"),
        "cast_members",
        "content_rating",
        content["created_at"],
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("content_enriched")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/content_enriched: {count} rows -> {output_path}")
    return count
```

- [ ] **Step 2: Smoke test**

Run:
```bash
python -c "
from spark.conf.spark_settings import get_spark_session
from spark.silver.transforms.transform_content import transform_content_enriched
spark = get_spark_session('Silver-Test')
count = transform_content_enriched(spark)
print(f'Wrote {count} rows')
spark.stop()
"
```

Expected: prints `Wrote 2000 rows` (matches bronze content count — left join preserves all content rows)

- [ ] **Step 3: Commit**

```bash
git add spark/silver/transforms/transform_content.py
git commit -m "feat: add silver transform for content enriched"
```

---

### Task 6: transform_ratings.py

**Files:**
- Create: `spark/silver/transforms/transform_ratings.py`

**Bronze schema (ratings):** rating_id, user_id, content_id, rating_value, rating_source, rated_at, created_at, _ingested_at, _source

**Key data notes:**
- Intentional duplicates: ~5% extra records share same `(user_id, content_id)` with different `rating_value` and `rated_at`
- `rating_value` is DECIMAL(2,1), range 1.0-5.0 (generated with beta distribution, all should be in range)
- Spec says validate 0.0-5.0 range, drop outliers

- [ ] **Step 1: Write the transform module**

Create `spark/silver/transforms/transform_ratings.py`:

```python
"""Silver transform: ratings — dedup by user+content, validate rating range."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_ratings_clean(spark):
    """Read bronze/ratings, dedup and validate, write to silver/ratings_clean."""
    print("  Transforming silver_ratings_clean...")

    df = spark.read.format("delta").load(get_bronze_path("ratings"))

    # Filter rating_value to 0.0-5.0 range
    df = df.filter((col("rating_value") >= 0.0) & (col("rating_value") <= 5.0))

    # Dedup: keep latest rating per (user_id, content_id)
    window = Window.partitionBy("user_id", "content_id").orderBy(col("rated_at").desc())
    df = df.withColumn("_rn", row_number().over(window))
    df = df.filter(col("_rn") == 1).drop("_rn")

    # Select final columns, dropping bronze audit columns
    result = df.select(
        "rating_id",
        "user_id",
        "content_id",
        col("rating_value").cast("decimal(2,1)").alias("rating_value"),
        "rating_source",
        "rated_at",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("ratings_clean")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/ratings_clean: {count} rows -> {output_path}")
    return count
```

- [ ] **Step 2: Smoke test**

Run:
```bash
python -c "
from spark.conf.spark_settings import get_spark_session
from spark.silver.transforms.transform_ratings import transform_ratings_clean
spark = get_spark_session('Silver-Test')
count = transform_ratings_clean(spark)
print(f'Wrote {count} rows')
spark.stop()
"
```

Expected: row count less than bronze ratings (dedup removes ~5% duplicates)

- [ ] **Step 3: Commit**

```bash
git add spark/silver/transforms/transform_ratings.py
git commit -m "feat: add silver transform for ratings"
```

---

### Task 7: transform_events.py

**Files:**
- Create: `spark/silver/transforms/transform_events.py`

**Bronze schemas:**
- **viewing_events_batch** (JSON): event_id, user_id, content_id, session_id, device_category, event_type, watch_duration_ms, seek_position_ms, event_ts, session_start_ts, referrer, _ingested_at, _source
- **viewing_events** (Kafka streaming): same fields + _kafka_partition, _kafka_offset, _ingested_at, _source

**Key data notes:**
- `event_ts` and `session_start_ts` are ISO 8601 strings like `"2025-06-15T21:30:45.123Z"`
- `session_id` and `session_start_ts` can be NULL (5% of batch data)
- `watch_duration_ms` is NULL for SEEK events; `seek_position_ms` is NULL for non-SEEK events
- `event_type` values: "PLAY", "PAUSE", "SEEK", "BUFFER_START", "BUFFER_END", "COMPLETE" — need lowercase
- The streaming table `viewing_events` may not exist (only populated when Kafka streaming runs)

- [ ] **Step 1: Write the transform module**

Create `spark/silver/transforms/transform_events.py`:

```python
"""Silver transform: viewing events — union batch+streaming, dedup, cast timestamps."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lower,
    row_number,
    to_date,
    to_timestamp,
)
from pyspark.sql.window import Window

from spark.conf.spark_settings import get_bronze_path, get_silver_path

# Common columns shared between batch and streaming sources
_COMMON_COLS = [
    "event_id", "user_id", "content_id", "session_id",
    "device_category", "event_type", "watch_duration_ms",
    "seek_position_ms", "event_ts", "session_start_ts", "referrer",
]


def transform_viewing_events(spark):
    """Read bronze viewing events (batch + streaming), union, dedup, transform."""
    print("  Transforming silver_viewing_events...")

    # Read batch source (always exists)
    batch = spark.read.format("delta").load(get_bronze_path("viewing_events_batch"))
    batch = batch.select(*_COMMON_COLS)

    # Try to read streaming source (may not exist if Kafka wasn't run)
    try:
        streaming = spark.read.format("delta").load(get_bronze_path("viewing_events"))
        streaming = streaming.select(*_COMMON_COLS)
        combined = batch.unionByName(streaming)
        print("    Merged batch + streaming sources")
    except Exception:
        combined = batch
        print("    Using batch source only (streaming table not found)")

    # Dedup by event_id: keep first occurrence (arbitrary — events are unique by design,
    # but streaming may re-ingest some batch events)
    window = Window.partitionBy("event_id").orderBy("event_ts")
    combined = combined.withColumn("_rn", row_number().over(window))
    combined = combined.filter(col("_rn") == 1).drop("_rn")

    # Cast timestamps from string to timestamp
    combined = combined.withColumn("event_ts", to_timestamp(col("event_ts")))
    combined = combined.withColumn("session_start_ts", to_timestamp(col("session_start_ts")))

    # Compute derived columns
    combined = combined.withColumn("watch_duration_sec", (col("watch_duration_ms") / 1000).cast("double"))
    combined = combined.withColumn("event_date", to_date(col("event_ts")))
    combined = combined.withColumn("event_type", lower(col("event_type")))

    # Select final columns
    result = combined.select(
        "event_id",
        "user_id",
        "content_id",
        "session_id",
        "device_category",
        "event_type",
        "watch_duration_ms",
        "watch_duration_sec",
        "seek_position_ms",
        "event_ts",
        "session_start_ts",
        "event_date",
        "referrer",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("viewing_events")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/viewing_events: {count} rows -> {output_path}")
    return count
```

- [ ] **Step 2: Smoke test**

Run:
```bash
python -c "
from spark.conf.spark_settings import get_spark_session
from spark.silver.transforms.transform_events import transform_viewing_events
spark = get_spark_session('Silver-Test')
count = transform_viewing_events(spark)
print(f'Wrote {count} rows')
spark.stop()
"
```

Expected: `Wrote 50000 rows` (or more if streaming data exists)

- [ ] **Step 3: Commit**

```bash
git add spark/silver/transforms/transform_events.py
git commit -m "feat: add silver transform for viewing events"
```

---

### Task 8: transform_campaigns.py

**Files:**
- Create: `spark/silver/transforms/transform_campaigns.py`

**Bronze schema (campaigns from Excel):** campaign_id, campaign_name, budget, start_date, end_date, _ingested_at, _source

**Key data notes:**
- `budget` is MIXED: 30% are strings like `"USD 38988255"`, 70% are plain integers (e.g., `12345678`)
- `start_date` and `end_date` come from Excel via pandas — they may be date or timestamp types
- Need to handle both budget formats: parse string format, keep integer format

- [ ] **Step 1: Write the transform module**

Create `spark/silver/transforms/transform_campaigns.py`:

```python
"""Silver transform: campaigns — parse budget string, compute duration."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    datediff,
    regexp_extract,
    to_date,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_campaigns(spark):
    """Read bronze/campaigns, parse budget, compute duration, write to silver/campaigns."""
    print("  Transforming silver_campaigns...")

    df = spark.read.format("delta").load(get_bronze_path("campaigns"))

    # Cast budget to string for uniform processing (some are int, some are "USD 12345")
    df = df.withColumn("budget_str", col("budget").cast("string"))

    # Extract currency: "USD" if string starts with letters, else "USD" (default)
    df = df.withColumn(
        "budget_currency",
        when(
            col("budget_str").rlike("^[A-Za-z]"),
            regexp_extract(col("budget_str"), r"^([A-Za-z]+)", 1),
        ).otherwise("INR"),
    )

    # Extract amount: digits after space if string format, otherwise the whole value
    df = df.withColumn(
        "budget_amount",
        when(
            col("budget_str").rlike("^[A-Za-z]"),
            regexp_extract(col("budget_str"), r"(\d+)$", 1).cast("long"),
        ).otherwise(col("budget_str").cast("long")),
    )

    # Cast dates and compute duration
    df = df.withColumn("start_date", to_date(col("start_date")))
    df = df.withColumn("end_date", to_date(col("end_date")))
    df = df.withColumn(
        "campaign_duration_days",
        datediff(col("end_date"), col("start_date")).cast("int"),
    )

    # Select final columns
    result = df.select(
        "campaign_id",
        "campaign_name",
        "budget_amount",
        "budget_currency",
        "start_date",
        "end_date",
        "campaign_duration_days",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("campaigns")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/campaigns: {count} rows -> {output_path}")
    return count
```

- [ ] **Step 2: Smoke test**

Run:
```bash
python -c "
from spark.conf.spark_settings import get_spark_session
from spark.silver.transforms.transform_campaigns import transform_campaigns
spark = get_spark_session('Silver-Test')
count = transform_campaigns(spark)
print(f'Wrote {count} rows')
spark.stop()
"
```

Expected: `Wrote 50 rows` (matches bronze campaigns count)

- [ ] **Step 3: Commit**

```bash
git add spark/silver/transforms/transform_campaigns.py
git commit -m "feat: add silver transform for campaigns"
```

---

### Task 9: Silver Orchestrator (run_silver.py)

**Files:**
- Create: `spark/jobs/run_silver.py`

- [ ] **Step 1: Write the orchestrator**

Create `spark/jobs/run_silver.py`:

```python
"""Silver orchestrator: runs all Silver transforms with a shared SparkSession."""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_silver_path, get_spark_session


def main():
    print("=" * 60)
    print("  SILVER LAYER TRANSFORMS")
    print("=" * 60)

    start_time = time.time()
    spark = get_spark_session("Silver-Orchestrator")

    try:
        # Dimensions first, then facts
        print("\n[1/6] Users")
        print("-" * 40)
        from spark.silver.transforms.transform_users import transform_users
        user_count = transform_users(spark)

        print("\n[2/6] Subscriptions")
        print("-" * 40)
        from spark.silver.transforms.transform_subscriptions import transform_subscriptions
        sub_count = transform_subscriptions(spark)

        print("\n[3/6] Content Enriched")
        print("-" * 40)
        from spark.silver.transforms.transform_content import transform_content_enriched
        content_count = transform_content_enriched(spark)

        print("\n[4/6] Campaigns")
        print("-" * 40)
        from spark.silver.transforms.transform_campaigns import transform_campaigns
        campaign_count = transform_campaigns(spark)

        print("\n[5/6] Ratings")
        print("-" * 40)
        from spark.silver.transforms.transform_ratings import transform_ratings_clean
        rating_count = transform_ratings_clean(spark)

        print("\n[6/6] Viewing Events")
        print("-" * 40)
        from spark.silver.transforms.transform_events import transform_viewing_events
        event_count = transform_viewing_events(spark)

        elapsed = time.time() - start_time

        print("\n" + "=" * 60)
        print("  SILVER LAYER TRANSFORMS COMPLETE")
        print("=" * 60)
        print(f"\n  Time: {elapsed:.1f}s")
        print(f"\n  Silver tables written:")
        print(f"    users:            {user_count:>6} rows")
        print(f"    subscriptions:    {sub_count:>6} rows")
        print(f"    content_enriched: {content_count:>6} rows")
        print(f"    campaigns:        {campaign_count:>6} rows")
        print(f"    ratings_clean:    {rating_count:>6} rows")
        print(f"    viewing_events:   {event_count:>6} rows")
        print(f"\n  Silver path: {get_silver_path('')}")
        print(f"\n  Next: python -m spark.jobs.validate_silver")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the full Silver pipeline locally**

Run:
```bash
python -m spark.jobs.run_silver
```

Expected: all 6 transforms complete with row counts printed, no errors.

- [ ] **Step 3: Commit**

```bash
git add spark/jobs/run_silver.py
git commit -m "feat: add silver orchestrator"
```

---

### Task 10: Silver Validation (validate_silver.py)

**Files:**
- Create: `spark/jobs/validate_silver.py`

- [ ] **Step 1: Write the validation script**

Create `spark/jobs/validate_silver.py`:

```python
"""Validation script for Silver Delta Lake tables."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import (
    get_bronze_path,
    get_lakehouse_path,
    get_silver_path,
    get_spark_session,
)


class SilverValidation:
    """Collect and report Silver validation checks."""

    def __init__(self):
        self.results = []

    def check(self, name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        self.results.append((name, status, detail))
        marker = "+" if condition else "X"
        print(f"  [{marker}] {name}: {detail}")

    def summary(self):
        passed = sum(1 for _, s, _ in self.results if s == "PASS")
        total = len(self.results)
        print(f"\n{'=' * 50}")
        print(f"  SILVER VALIDATION: {passed}/{total} checks passed")
        print(f"{'=' * 50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def main():
    """Run all Silver validation checks."""
    spark = get_spark_session("Silver-Validation")
    v = SilverValidation()

    # ---- silver_users (checks 1-4) ----
    print("\n--- silver_users ---")
    try:
        silver_users = spark.read.format("delta").load(get_silver_path("users"))
        bronze_users = spark.read.format("delta").load(get_bronze_path("users"))

        su_count = silver_users.count()
        bu_count = bronze_users.count()
        v.check("1. silver_users row count", su_count == bu_count,
                f"{su_count} silver == {bu_count} bronze")

        null_uid = silver_users.filter("user_id IS NULL").count()
        v.check("2. silver_users no null user_id", null_uid == 0,
                f"{null_uid} nulls")

        null_email_valid = silver_users.filter("is_valid_email IS NULL").count()
        v.check("3. silver_users is_valid_email populated", null_email_valid == 0,
                f"{null_email_valid} nulls")

        null_os = silver_users.filter("device_os IS NULL").count()
        v.check("4. silver_users device_os populated", null_os == 0,
                f"{null_os} nulls")
    except Exception as e:
        v.check("silver_users load", False, str(e))

    # ---- silver_subscriptions (checks 5-6) ----
    print("\n--- silver_subscriptions ---")
    try:
        silver_subs = spark.read.format("delta").load(get_silver_path("subscriptions"))
        bronze_subs = spark.read.format("delta").load(get_bronze_path("subscriptions"))

        ss_count = silver_subs.count()
        bs_count = bronze_subs.count()
        v.check("5. silver_subscriptions row count", ss_count == bs_count,
                f"{ss_count} silver == {bs_count} bronze")

        null_dur = silver_subs.filter("duration_days IS NULL").count()
        v.check("6. silver_subscriptions duration_days no nulls", null_dur == 0,
                f"{null_dur} nulls")
    except Exception as e:
        v.check("silver_subscriptions load", False, str(e))

    # ---- silver_content_enriched (checks 7-9) ----
    print("\n--- silver_content_enriched ---")
    try:
        silver_content = spark.read.format("delta").load(get_silver_path("content_enriched"))
        bronze_content = spark.read.format("delta").load(get_bronze_path("content"))

        sc_count = silver_content.count()
        bc_count = bronze_content.count()
        v.check("7. silver_content_enriched row count", sc_count == bc_count,
                f"{sc_count} silver == {bc_count} bronze")

        null_rt = silver_content.filter("runtime_minutes IS NULL").count()
        v.check("8. silver_content_enriched runtime_minutes no nulls", null_rt == 0,
                f"{null_rt} nulls")

        # Check genre_array is actually an array type
        genre_type = str(silver_content.schema["genre_array"].dataType)
        is_array = "ArrayType" in genre_type
        v.check("9. silver_content_enriched genre_array is array", is_array,
                genre_type)
    except Exception as e:
        v.check("silver_content_enriched load", False, str(e))

    # ---- silver_ratings_clean (checks 10-12) ----
    print("\n--- silver_ratings_clean ---")
    try:
        silver_ratings = spark.read.format("delta").load(get_silver_path("ratings_clean"))
        bronze_ratings = spark.read.format("delta").load(get_bronze_path("ratings"))

        sr_count = silver_ratings.count()
        br_count = bronze_ratings.count()
        v.check("10. silver_ratings_clean dedup check", sr_count <= br_count,
                f"{sr_count} silver <= {br_count} bronze")

        # No duplicate (user_id, content_id) pairs
        distinct_pairs = silver_ratings.select("user_id", "content_id").distinct().count()
        v.check("11. silver_ratings_clean no duplicate user+content",
                distinct_pairs == sr_count,
                f"{distinct_pairs} distinct pairs == {sr_count} rows")

        # rating_value in 0-5 range
        from pyspark.sql.functions import max as spark_max, min as spark_min
        stats = silver_ratings.agg(
            spark_min("rating_value").alias("min_val"),
            spark_max("rating_value").alias("max_val"),
        ).first()
        min_val = float(stats["min_val"])
        max_val = float(stats["max_val"])
        v.check("12. silver_ratings_clean rating_value in 0-5",
                min_val >= 0.0 and max_val <= 5.0,
                f"range [{min_val}, {max_val}]")
    except Exception as e:
        v.check("silver_ratings_clean load", False, str(e))

    # ---- silver_viewing_events (checks 13-16) ----
    print("\n--- silver_viewing_events ---")
    try:
        silver_events = spark.read.format("delta").load(get_silver_path("viewing_events"))
        bronze_batch = spark.read.format("delta").load(get_bronze_path("viewing_events_batch"))

        se_count = silver_events.count()
        bb_count = bronze_batch.count()
        v.check("13. silver_viewing_events row count", se_count >= bb_count,
                f"{se_count} silver >= {bb_count} batch bronze")

        # No duplicate event_id
        distinct_events = silver_events.select("event_id").distinct().count()
        v.check("14. silver_viewing_events no duplicate event_id",
                distinct_events == se_count,
                f"{distinct_events} distinct == {se_count} total")

        # event_ts is timestamp type
        ts_type = str(silver_events.schema["event_ts"].dataType)
        v.check("15. silver_viewing_events event_ts is timestamp",
                "Timestamp" in ts_type, ts_type)

        # event_date populated
        null_date = silver_events.filter("event_date IS NULL").count()
        v.check("16. silver_viewing_events event_date populated", null_date == 0,
                f"{null_date} nulls")
    except Exception as e:
        v.check("silver_viewing_events load", False, str(e))

    # ---- silver_campaigns (checks 17-18) ----
    print("\n--- silver_campaigns ---")
    try:
        silver_campaigns = spark.read.format("delta").load(get_silver_path("campaigns"))

        null_amount = silver_campaigns.filter("budget_amount IS NULL").count()
        v.check("17. silver_campaigns budget_amount is numeric", null_amount == 0,
                f"{null_amount} nulls")

        null_currency = silver_campaigns.filter("budget_currency IS NULL").count()
        v.check("18. silver_campaigns budget_currency populated", null_currency == 0,
                f"{null_currency} nulls")
    except Exception as e:
        v.check("silver_campaigns load", False, str(e))

    # ---- Cross-table checks (19-20) ----
    print("\n--- Cross-table checks ---")
    silver_tables = ["users", "subscriptions", "content_enriched",
                     "ratings_clean", "viewing_events", "campaigns"]

    # Check 19: _silver_processed_at exists in all tables
    all_have_ts = True
    missing_ts = []
    for table in silver_tables:
        try:
            df = spark.read.format("delta").load(get_silver_path(table))
            if "_silver_processed_at" not in df.columns:
                all_have_ts = False
                missing_ts.append(table)
        except Exception:
            all_have_ts = False
            missing_ts.append(table)
    v.check("19. All silver tables have _silver_processed_at", all_have_ts,
            "all present" if all_have_ts else f"missing in: {missing_ts}")

    # Check 20: _delta_log exists for all tables
    lakehouse = get_lakehouse_path()
    all_have_log = True
    missing_log = []
    if lakehouse.startswith("s3a://"):
        import boto3
        s3 = boto3.client("s3")
        bucket = lakehouse.replace("s3a://", "").split("/")[0]
        for table in silver_tables:
            prefix = f"silver/{table}/_delta_log/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                all_have_log = False
                missing_log.append(table)
    else:
        for table in silver_tables:
            log_path = os.path.join(lakehouse, "silver", table, "_delta_log")
            if not os.path.isdir(log_path):
                all_have_log = False
                missing_log.append(table)
    v.check("20. All silver tables _delta_log exists", all_have_log,
            "all present" if all_have_log else f"missing: {missing_log}")

    all_passed = v.summary()
    spark.stop()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run validation locally**

Run:
```bash
python -m spark.jobs.validate_silver
```

Expected: `SILVER VALIDATION: 20/20 checks passed`

- [ ] **Step 3: Fix any failing checks**

If any checks fail, review the failing transform, fix the issue, re-run `python -m spark.jobs.run_silver`, then re-run validation.

- [ ] **Step 4: Commit**

```bash
git add spark/jobs/validate_silver.py
git commit -m "feat: add silver validation script (20 checks)"
```

---

### Task 11: Final commit and local validation pass

- [ ] **Step 1: Run full Silver pipeline end-to-end**

Run:
```bash
python -m spark.jobs.run_silver
```

Verify all 6 transforms complete successfully.

- [ ] **Step 2: Run full validation**

Run:
```bash
python -m spark.jobs.validate_silver
```

Verify: `SILVER VALIDATION: 20/20 checks passed`

- [ ] **Step 3: Final commit with all Silver code**

If there were any fixes during validation, stage and commit them:
```bash
git add -A spark/silver/ spark/jobs/run_silver.py spark/jobs/validate_silver.py spark/conf/spark_settings.py
git commit -m "feat: Phase 3 Silver layer complete — 6 transforms, orchestrator, 20/20 validation"
```

---

### Task 12: Deploy and validate on EC2/S3

**Prerequisites:** EC2 instance running, Bronze data exists in S3.

- [ ] **Step 1: Push code to GitHub**

```bash
git push origin main
```

- [ ] **Step 2: SSH into EC2 and pull latest code**

```bash
ssh -i infra/aws/jiohotstar-key.pem ubuntu@<EC2_PUBLIC_IP> "cd media_stream_analytics && git pull origin main"
```

- [ ] **Step 3: Run Silver pipeline on EC2**

```bash
ssh -i infra/aws/jiohotstar-key.pem ubuntu@<EC2_PUBLIC_IP> "cd media_stream_analytics && source .env.ec2 && python3 -m spark.jobs.run_silver"
```

Expected: all 6 transforms write to `s3://jiohotstar-lakehouse-868896905478/silver/`

- [ ] **Step 4: Run Silver validation on EC2**

```bash
ssh -i infra/aws/jiohotstar-key.pem ubuntu@<EC2_PUBLIC_IP> "cd media_stream_analytics && source .env.ec2 && python3 -m spark.jobs.validate_silver"
```

Expected: `SILVER VALIDATION: 20/20 checks passed`

- [ ] **Step 5: Stop EC2 instance**

```bash
aws ec2 stop-instances --instance-ids i-00464b8481d556b64 --region ap-south-1
```
