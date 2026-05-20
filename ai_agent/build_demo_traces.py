"""Build deterministic, AWS-free demo traces for the dashboard showcase.

Runs the offline runtime against canned data so the dashboard can replay a real
serialized Trace with no network access.
"""

from __future__ import annotations

from pathlib import Path

from ai_agent.analytics_tools import build_default_registry
from ai_agent.brains import RuleBrain
from ai_agent.demo_data import apply_canned_datasource, restore_datasource
from ai_agent.reasoning import ReasoningEngine
from ai_agent.trace import Trace

_DEMO_DIR = Path(__file__).parent / "demo_traces"


def build_demo_trace(question: str) -> Trace:
    """Run the offline runtime for one question and return its Trace."""
    apply_canned_datasource()
    try:
        engine = ReasoningEngine(registry=build_default_registry(), rule_brain=RuleBrain())
        return engine.run(question).trace
    finally:
        restore_datasource()


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
