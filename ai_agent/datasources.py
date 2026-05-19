"""Athena and PostgreSQL access for the AI agent runtime.

Ported from langgraph_agent.py. All DB access funnels through this module so
tool tests can monkeypatch a single seam.
"""

from __future__ import annotations

import os
import time

_athena_client = None
_pg_engine = None


class DataSourceError(RuntimeError):
    """Raised when an Athena or PostgreSQL query cannot be completed."""


def _get_athena_client():
    global _athena_client
    if _athena_client is None:
        import boto3

        _athena_client = boto3.client("athena", region_name=os.getenv("AWS_REGION", "ap-south-1"))
    return _athena_client


def parse_athena_result(result: dict) -> list[dict]:
    """Convert an Athena get_query_results payload into a list of row dicts."""
    meta = result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    columns = [c["Name"] for c in meta]
    rows = []
    for row in result["ResultSet"]["Rows"][1:]:  # skip the header row
        values = [cell.get("VarCharValue", "") for cell in row.get("Data", [])]
        rows.append(dict(zip(columns, values)))
    return rows


def _retry_once(fn):
    """Run fn; on a transient DataSourceError retry exactly once after a short pause.

    Deliberately minimal per the spec — one retry, no exponential backoff. A second
    failure propagates to the caller (where the engine turns it into an observation).
    """
    try:
        return fn()
    except DataSourceError:
        time.sleep(1.0)
        return fn()


def _athena_query_once(sql: str) -> list[dict]:
    client = _get_athena_client()
    database = os.getenv("ATHENA_DATABASE", "jiohotstar_gold")
    output = os.getenv(
        "ATHENA_OUTPUT_LOCATION",
        "s3://jiohotstar-lakehouse-868896905478/athena-results/",
    )
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output},
        WorkGroup=os.getenv("ATHENA_WORKGROUP", "primary"),
    )
    exec_id = resp["QueryExecutionId"]

    for _ in range(30):
        status = client.get_query_execution(QueryExecutionId=exec_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise DataSourceError(f"Athena query {state}: {reason}")
        time.sleep(1.5)
    else:
        raise DataSourceError("Athena query timed out.")

    return parse_athena_result(client.get_query_results(QueryExecutionId=exec_id, MaxResults=50))


def run_athena_sql(sql: str) -> list[dict]:
    """Execute SQL on Athena (one retry on transient failure). Raises DataSourceError."""
    return _retry_once(lambda: _athena_query_once(sql))


def _pg_query_once(sql: str) -> list[dict]:
    global _pg_engine
    try:
        if _pg_engine is None:
            from sqlalchemy import create_engine

            _pg_engine = create_engine(
                os.getenv(
                    "STREAMING_PG_URL",
                    "postgresql+psycopg2://grafana:grafana123@localhost:5432/jiohotstar_streaming",
                )
            )
        from sqlalchemy import text

        with _pg_engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = list(result.keys())
            return [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as exc:  # noqa: BLE001 — wrap any driver error
        raise DataSourceError(f"PostgreSQL query failed: {exc}") from exc


def run_pg_sql(sql: str) -> list[dict]:
    """Execute SQL on PostgreSQL (one retry on transient failure). Raises DataSourceError."""
    return _retry_once(lambda: _pg_query_once(sql))
