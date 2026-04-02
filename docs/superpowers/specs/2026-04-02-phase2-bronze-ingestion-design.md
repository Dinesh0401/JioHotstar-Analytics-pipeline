# Phase 2: AWS Cloud Bronze Ingestion Design

**Date:** 2026-04-02
**Status:** Approved
**Depends on:** Phase 1 (data generation) — complete

---

## 1. Overview

Phase 2 ingests all Phase 1 data sources into a Bronze layer using PySpark and Delta Lake on AWS. Bronze tables store raw, unmodified data with audit metadata. No cleaning, no deduplication — that is Silver (Phase 3).

## 2. Architecture

```
EC2 (t3.medium, Ubuntu 22.04)
├── Docker: MySQL, PostgreSQL, Kafka, Zookeeper, Adminer, Kafka-UI
├── PySpark 3.5.1 + Delta Lake 3.1.0 + Java 11
└── Writes to S3 (or local lakehouse/ for testing)

S3: jiohotstar-lakehouse-<account-id>
├── bronze/
│   ├── users/
│   ├── subscriptions/
│   ├── content/
│   ├── ratings/
│   ├── content_metadata/
│   ├── viewing_events_batch/
│   ├── campaigns/
│   └── viewing_events/
├── silver/   (empty — Phase 3)
├── gold/     (empty — Phase 4)
└── checkpoints/
    └── bronze_viewing_events/
```

## 3. Project Structure (New Files)

```
media_stream_analytics/
├── infra/
│   ├── aws/
│   │   ├── create_infrastructure.sh    # S3, IAM, EC2, security group
│   │   └── destroy_infrastructure.sh   # Tear down everything
│   └── ec2/
│       └── provision_ec2.sh            # Docker, Java, PySpark, deploy
│
├── spark/
│   ├── conf/
│   │   └── spark_settings.py           # SparkSession factory, env-aware config
│   ├── bronze/
│   │   ├── batch/
│   │   │   ├── ingest_mysql.py         # users + subscriptions via JDBC
│   │   │   ├── ingest_postgres.py      # content + ratings via JDBC
│   │   │   └── ingest_files.py         # CSV + JSON + Excel
│   │   └── streaming/
│   │       └── ingest_kafka_streaming.py  # Kafka micro-batch
│   └── jobs/
│       ├── run_bronze.py               # Batch orchestrator
│       └── validate_bronze.py          # 12 validation checks
│
├── lakehouse/                          # Local Delta Lake storage
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
│       └── bronze_viewing_events/
│
├── .env.local                          # Local: MYSQL_PORT=3307, POSTGRES_PORT=5433
├── .env.ec2                            # EC2: MYSQL_PORT=3306, POSTGRES_PORT=5432
│
├── config/settings.py                  # Updated: env-variable ports + lakehouse path
├── docker/docker-compose.yml           # Updated: parameterized ports
└── .gitignore                          # Updated: lakehouse/, checkpoints/
```

## 4. AWS Infrastructure

### 4.1 Resources

| Resource | Name | Purpose |
|----------|------|---------|
| S3 Bucket | `jiohotstar-lakehouse-<account-id>` | Delta Lake storage |
| IAM Role | `jiohotstar-ec2-role` | EC2 to S3 access |
| IAM Policy | `jiohotstar-s3-access` | Scoped to lakehouse bucket |
| Instance Profile | `jiohotstar-ec2-profile` | Attaches role to EC2 |
| Security Group | `jiohotstar-sg` | SSH(22) + Spark UI(8080) + Kafka UI(8081) |
| Key Pair | `jiohotstar-key` | SSH access |
| EC2 Instance | `jiohotstar-spark` (tag) | t3.medium, Ubuntu 22.04, 30GB EBS |

### 4.2 IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::jiohotstar-lakehouse-<account-id>",
        "arn:aws:s3:::jiohotstar-lakehouse-<account-id>/*"
      ]
    }
  ]
}
```

### 4.3 Security Group Rules

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | TCP | Your IP/32 | SSH |
| 8080 | TCP | Your IP/32 | Adminer (DB web UI) |
| 8081 | TCP | Your IP/32 | Kafka UI |

Internal ports (3306, 5432, 9092, 2181) are NOT exposed externally.

### 4.4 Instance Tags

```
Name = jiohotstar-spark
Project = JioHotstar-DE-ML-AI
StopAfterHours = 4
```

### 4.5 Cost Estimate

| Resource | Cost |
|----------|------|
| EC2 t3.medium | ~$0.04/hr |
| S3 storage | ~cents/month |
| IAM, Security Group | free |
| **Monthly estimate (4 hrs/day)** | **$5-10** |

### 4.6 `create_infrastructure.sh`

Runs from laptop. Creates all AWS resources in order:
1. Create S3 bucket with prefix structure (bronze/, silver/, gold/, checkpoints/)
2. Create IAM policy (scoped to bucket)
3. Create IAM role with EC2 trust policy
4. Attach policy to role
5. Create instance profile, add role
6. Create security group (SSH + 8080 + 8081 from caller IP)
7. Create key pair (save .pem locally)
8. Launch EC2 instance with instance profile
9. Wait for instance to be running
10. Output config file (`infra/aws/ec2_config.txt`) with instance ID, public IP, bucket name

### 4.7 `destroy_infrastructure.sh`

Runs from laptop. Tears down everything:
1. Terminate EC2 instance
2. Delete key pair
3. Delete security group
4. Remove role from instance profile
5. Delete instance profile
6. Detach policy from role
7. Delete IAM role and policy
8. Optionally empty and delete S3 bucket (with confirmation prompt)

## 5. EC2 Provisioning

### `provision_ec2.sh`

Runs on EC2 after SSH. Installation order:

| Step | Action | Details |
|------|--------|---------|
| 1 | System updates | apt update, git, curl, unzip, wget |
| 2 | Docker + Compose | Docker CE, Compose plugin, ubuntu user in docker group |
| 3 | Java 11 | OpenJDK 11 headless (Spark dependency) |
| 4 | Python packages | pyspark==3.5.1, delta-spark==3.1.0, boto3, pymysql, psycopg2-binary, kafka-python, numpy, faker, openpyxl, Pillow |
| 5 | AWS CLI v2 | Verify available (pre-installed on Ubuntu AMI or install) |
| 6 | Clone repo | git clone, cp .env.ec2 .env, docker compose up -d |
| 7 | Verify services | MySQL, PostgreSQL, Kafka ready checks + run_phase1.py + validate_phase1.py |
| 8 | Test S3 connectivity | PySpark writes small Delta table to S3, confirm it exists |

### Spark Configuration

```
spark.hadoop.fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider = com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.jars.packages = io.delta:delta-spark_2.12:3.1.0,
                      org.apache.hadoop:hadoop-aws:3.3.2,
                      org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,
                      com.mysql:mysql-connector-j:8.3.0,
                      org.postgresql:postgresql:42.7.3
```

## 6. Spark Bronze Pipelines

### 6.1 Spark Session Factory (`spark/conf/spark_settings.py`)

Shared module providing `get_spark_session()`:
- Reads `LAKEHOUSE_PATH` from env (local: `lakehouse/`, EC2: `s3://jiohotstar-lakehouse-<id>`)
- Reads `MYSQL_PORT`, `POSTGRES_PORT` from env
- Configures S3A + IAM credentials provider (only when LAKEHOUSE_PATH starts with `s3://`)
- Registers Delta Lake extensions (`delta.tables`, `delta.enableChangeDataFeed`)
- Sets shuffle partitions = 4
- Enables Adaptive Query Execution (AQE) with partition coalescing

### 6.2 Batch Ingestion

**`ingest_mysql.py`** — MySQL users + subscriptions
- Source: `localhost:MYSQL_PORT` / `streaming_users`
- Method: `spark.read.jdbc()` with MySQL JDBC driver
- Output: `LAKEHOUSE_PATH/bronze/users/`, `LAKEHOUSE_PATH/bronze/subscriptions/`
- Format: Delta Lake, overwrite mode
- Adds: `_ingested_at` (current_timestamp), `_source = "mysql"`

**`ingest_postgres.py`** — PostgreSQL content + ratings
- Source: `localhost:POSTGRES_PORT` / `streaming_content`
- Method: `spark.read.jdbc()` with PostgreSQL JDBC driver
- Output: `LAKEHOUSE_PATH/bronze/content/`, `LAKEHOUSE_PATH/bronze/ratings/`
- Format: Delta Lake, overwrite mode
- Adds: `_ingested_at`, `_source = "postgres"`

**`ingest_files.py`** — CSV, JSON, Excel
- CSV: `data_sources/csv/content_metadata.csv` → `bronze/content_metadata/`
- JSON: `data_sources/json/viewing_events_batch.json` → `bronze/viewing_events_batch/`
- Excel: `data_sources/excel/ad_campaigns.xlsx` → read via pandas, convert to Spark DF → `bronze/campaigns/`
- Format: Delta Lake, overwrite mode
- Adds: `_ingested_at`, `_source = "csv"/"json"/"excel"`

### 6.3 Streaming Ingestion

**`ingest_kafka_streaming.py`** — Kafka micro-batch
- Source: Kafka topic `media.viewing.live` on `localhost:9092`
- Method: `spark.readStream.format("kafka")`
- Trigger: `processingTime="30 seconds"`
- JSON parsing: Kafka value (bytes) → string → parse to struct with fields:
  `event_id, user_id, content_id, session_id, device_category, event_type, watch_duration_ms, seek_position_ms, event_ts, session_start_ts, referrer`
- Adds: `_ingested_at`, `_source = "kafka"`, `_kafka_partition`, `_kafka_offset`
- Output: `LAKEHOUSE_PATH/bronze/viewing_events/`
- Format: Delta Lake, append mode
- Checkpoint: `LAKEHOUSE_PATH/checkpoints/bronze_viewing_events/`

### 6.4 Bronze Orchestrator (`run_bronze.py`)

Runs batch ingestion only (streaming is separate):
1. Initialize Spark session
2. Ingest MySQL (users + subscriptions)
3. Ingest PostgreSQL (content + ratings)
4. Ingest files (CSV + JSON + Excel)
5. Print summary (row counts per Bronze table)

Streaming started independently: `python -m spark.bronze.streaming.ingest_kafka_streaming`

### 6.5 Bronze Rules

| Rule | Implementation |
|------|---------------|
| Raw data only | No column renaming, no type casting, no filtering |
| Append-only (streaming) | Kafka uses append mode |
| Full snapshot (batch) | JDBC/files use overwrite mode |
| No deduplication | Duplicates from source preserved intentionally |
| Audit columns | Every table gets `_ingested_at`, `_source` |

Batch Bronze tables represent the **latest snapshot** of the source systems (not CDC).
Streaming Bronze tables **append continuously** from Kafka.

## 7. Delta Lake File Sizing

### Batch Tables
- Small tables (5K-50K rows): Spark writes 1-2 files naturally
- `spark.sql.shuffle.partitions = 4` prevents tiny file explosion

### Streaming Table
- Micro-batch every 30s creates small files over time
- Use Spark Adaptive Query Execution (AQE) to coalesce partitions:
  ```
  spark.sql.adaptive.enabled = true
  spark.sql.adaptive.coalescePartitions.enabled = true
  ```
- Note: `autoOptimize` and `optimize().executeCompaction()` are Databricks-only features
  and are NOT available in open-source Delta Lake. AQE is the correct OSS alternative.

## 8. Validation (`validate_bronze.py`)

12 checks:

| # | Check | Expected |
|---|-------|----------|
| 1 | bronze/users row count | 5,000 |
| 2 | bronze/subscriptions row count | 6,000-8,000 |
| 3 | bronze/content row count | 2,000 |
| 4 | bronze/ratings row count | 15,000-18,000 |
| 5 | bronze/content_metadata row count | 2,000 |
| 6 | bronze/viewing_events_batch row count | 50,000 |
| 7 | bronze/campaigns row count | 50 |
| 8 | bronze/viewing_events exists | Delta table present |
| 9 | All tables have `_ingested_at` | Not null |
| 10 | All tables have `_source` | Correct source tag |
| 11 | Delta log exists | `_delta_log/` present for each table |
| 12 | S3 path validation (EC2 only) | Tables exist in bucket |

Exit 0 = all pass, exit 1 = any fail.

## 9. Environment Configuration

### `.env.local` (laptop)
```
MYSQL_PORT=3307
POSTGRES_PORT=5433
KAFKA_BOOTSTRAP=localhost:9092
LAKEHOUSE_PATH=lakehouse
DATA_SOURCES_DIR=data_sources
```

### `.env.ec2` (cloud)
```
MYSQL_PORT=3306
POSTGRES_PORT=5432
KAFKA_BOOTSTRAP=localhost:9092
LAKEHOUSE_PATH=s3://jiohotstar-lakehouse-<account-id>
DATA_SOURCES_DIR=data_sources
```

### `config/settings.py` changes
- All ports read from `os.getenv()` with current values as defaults
- New: `LAKEHOUSE_PATH`, `CHECKPOINT_PATH` derived from env

### `docker/docker-compose.yml` changes
- Ports parameterized: `"${MYSQL_PORT:-3306}:3306"`, `"${POSTGRES_PORT:-5432}:5432"`

## 10. Execution Order

### Local Testing
```
1. Ensure Phase 1 containers running
2. cp .env.local .env
3. python -m spark.jobs.run_bronze
4. python -m spark.jobs.validate_bronze
5. Test streaming: start kafka producer, then run streaming ingestion
```

### EC2 Deployment
```
1. Run infra/aws/create_infrastructure.sh (from laptop)
2. SSH into EC2
3. Run infra/ec2/provision_ec2.sh (on EC2)
4. python run_phase1.py (generate data on EC2)
5. python validate_phase1.py (verify sources)
6. python -m spark.jobs.run_bronze (batch ingestion)
7. python -m spark.jobs.validate_bronze (verify Bronze)
8. python -m data_generation.kafka_producer --mode batch (start Kafka events)
9. python -m spark.bronze.streaming.ingest_kafka_streaming (stream to Bronze)
10. Verify S3: aws s3 ls s3://jiohotstar-lakehouse-<id>/bronze/
```

## 11. Cost Controls

- `destroy_infrastructure.sh` tears down all resources
- EC2 tagged with `StopAfterHours=4` (reminder)
- Stop EC2 when not in use: `aws ec2 stop-instances --instance-ids <id>`
- Avoid: EMR, MSK, Redshift, Glue (expensive)
- Budget: ~$100 credits covers entire project lifecycle
