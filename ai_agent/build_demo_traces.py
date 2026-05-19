"""Build deterministic, AWS-free demo traces for the dashboard showcase.

Runs the offline runtime against canned data so the dashboard can replay a real
serialized Trace with no network access.
"""

from __future__ import annotations

import json
from pathlib import Path

from ai_agent import datasources
from ai_agent.analytics_tools import build_default_registry
from ai_agent.brains import RuleBrain
from ai_agent.reasoning import ReasoningEngine
from ai_agent.trace import Trace

_DEMO_DIR = Path(__file__).parent / "demo_traces"

# Canned rows keyed by a substring of the SQL, so each tool gets plausible data.
_CANNED: dict[str, list[dict]] = {
    "subscription_metrics": [
        {"plan_id": "PREMIUM", "total_subscriptions": "5000", "active_subscriptions": "3800",
         "cancelled_subscriptions": "1200", "top_cancel_reason": "price"},
        {"plan_id": "VIP", "total_subscriptions": "3000", "active_subscriptions": "2600",
         "cancelled_subscriptions": "400", "top_cancel_reason": "content"},
    ],
    "user_churn_prediction": [
        {"plan_id": "PREMIUM", "total_users": "5000", "predicted_churners": "1500",
         "avg_churn_prob": "0.48"},
        {"plan_id": "VIP", "total_users": "3000", "predicted_churners": "300",
         "avg_churn_prob": "0.19"},
    ],
    "content_watch_metrics": [
        {"title": "World Cup Final", "content_type": "Sports",
         "total_views": "98000", "unique_viewers": "71000"},
    ],
}


def _canned_athena(sql: str) -> list[dict]:
    for key, rows in _CANNED.items():
        if key in sql:
            return rows
    return []


def build_demo_trace(question: str) -> Trace:
    """Run the offline runtime for one question and return its Trace."""
    original = datasources.run_athena_sql
    datasources.run_athena_sql = _canned_athena  # type: ignore[assignment]
    try:
        engine = ReasoningEngine(registry=build_default_registry(), rule_brain=RuleBrain())
        return engine.run(question).trace
    finally:
        datasources.run_athena_sql = original  # type: ignore[assignment]


_DEMO_QUESTIONS = {
    "compare_churn_by_plan": "compare churn risk across subscription plans",
    "top_content": "what are the top trending shows?",
}


def main() -> None:
    _DEMO_DIR.mkdir(exist_ok=True)
    for slug, question in _DEMO_QUESTIONS.items():
        trace = build_demo_trace(question)
        path = _DEMO_DIR / f"{slug}.json"
        path.write_text(trace.to_json(), encoding="utf-8")
        print(f"wrote {path} ({len(trace.events)} events)")


if __name__ == "__main__":
    main()
