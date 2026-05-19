"""Tests for the Athena/PostgreSQL access layer (boto3 client faked)."""

from __future__ import annotations

import pytest

from ai_agent import datasources
from ai_agent.datasources import DataSourceError, parse_athena_result


def test_parse_athena_result_maps_rows_to_dicts():
    raw = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Name": "genre"}, {"Name": "views"}]},
            "Rows": [
                {"Data": [{"VarCharValue": "genre"}, {"VarCharValue": "views"}]},  # header
                {"Data": [{"VarCharValue": "Sports"}, {"VarCharValue": "900"}]},
            ],
        }
    }
    assert parse_athena_result(raw) == [{"genre": "Sports", "views": "900"}]


def test_run_athena_sql_raises_on_failed_query(monkeypatch):
    class FakeAthena:
        def start_query_execution(self, **kwargs):
            return {"QueryExecutionId": "q1"}

        def get_query_execution(self, **kwargs):
            return {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "bad"}}}

    monkeypatch.setattr(datasources, "_get_athena_client", lambda: FakeAthena())
    with pytest.raises(DataSourceError):
        datasources.run_athena_sql("SELECT 1")
