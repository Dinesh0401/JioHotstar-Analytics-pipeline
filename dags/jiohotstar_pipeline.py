"""
JioHotstar Streaming Analytics — Airflow DAG
Orchestrates the full Medallion Lakehouse pipeline:
  Data Generation → Bronze Ingestion → Silver Transforms → Gold Analytics → Validation
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ── Project root (where run_phase1.py, spark/, etc. live) ───────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PYTHON = sys.executable  # use the same Python that runs Airflow

default_args = {
    "owner": "jiohotstar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="jiohotstar_medallion_pipeline",
    default_args=default_args,
    description="End-to-end Medallion Lakehouse: Generate → Bronze → Silver → Gold → Validate",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["jiohotstar", "medallion", "lakehouse"],
) as dag:

    # ── 1. Data Generation ──────────────────────────────────────────────────
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} run_phase1.py",
    )

    # ── 2. Bronze Ingestion ─────────────────────────────────────────────────
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m spark.jobs.run_bronze",
    )

    # ── 3. Silver Transforms ────────────────────────────────────────────────
    silver_transforms = BashOperator(
        task_id="silver_transforms",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m spark.jobs.run_silver",
    )

    # ── 4. Silver Validation ────────────────────────────────────────────────
    silver_validation = BashOperator(
        task_id="silver_validation",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m spark.jobs.validate_silver",
    )

    # ── 5. Gold Analytics ───────────────────────────────────────────────────
    gold_analytics = BashOperator(
        task_id="gold_analytics",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m spark.jobs.run_gold",
    )

    # ── 6. Gold Validation ──────────────────────────────────────────────────
    gold_validation = BashOperator(
        task_id="gold_validation",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m spark.jobs.validate_gold",
    )

    # ── Pipeline DAG ────────────────────────────────────────────────────────
    # generate_data → bronze_ingestion → silver_transforms → silver_validation
    #                                                              ↓
    #                                          gold_analytics → gold_validation
    generate_data >> bronze_ingestion >> silver_transforms >> silver_validation
    silver_validation >> gold_analytics >> gold_validation
