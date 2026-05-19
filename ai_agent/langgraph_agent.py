"""Conversational session shim over the AI agent reasoning runtime.

This module previously held a standalone LangGraph agent. It is now a thin
adapter: the same ConversationSession API (used by api.py) drives ReasoningEngine.
"""

from __future__ import annotations

import os

from ai_agent.analytics_tools import build_default_registry
from ai_agent.brains import BedrockBrain, RuleBrain
from ai_agent.reasoning import EngineResult, ReasoningEngine


def _maybe_bedrock_brain() -> BedrockBrain | None:
    """Build a BedrockBrain if boto3 and a model id are available, else None."""
    model_id = os.getenv("BEDROCK_MODEL_ID")
    if not model_id:
        return None
    try:
        import boto3

        client = boto3.client("bedrock-runtime",
                               region_name=os.getenv("AWS_REGION", "ap-south-1"))
        return BedrockBrain(client=client, model_id=model_id)
    except Exception:  # noqa: BLE001 — no Bedrock available, fall back to rules
        return None


def build_engine() -> ReasoningEngine:
    """Construct the runtime: full tool registry + both brains where available."""
    return ReasoningEngine(
        registry=build_default_registry(),
        bedrock_brain=_maybe_bedrock_brain(),
        rule_brain=RuleBrain(),
    )


class ConversationSession:
    """Manages a single conversation. API-compatible with the previous version."""

    def __init__(self) -> None:
        self.engine = build_engine()
        self.history: list[str] = []
        self.last_result: EngineResult | None = None

    def ask(self, question: str, on_event=None) -> str:
        """Run one question through the runtime and return the answer text."""
        self.history.append(question)
        result = self.engine.run(question, history=self.history, on_event=on_event)
        self.last_result = result
        return result.answer

    def reset(self) -> None:
        self.history = []
        self.last_result = None
