"""Decision-making brains for the reasoning runtime.

A brain answers one question: given the state and the tool catalog, what is the
next action? RuleBrain is deterministic and offline; BedrockBrain (Task 13) uses
the LLM. The ReasoningEngine is brain-agnostic.
"""

from __future__ import annotations

import re
from typing import Protocol

from ai_agent.reasoning import AgentState, FinalAnswer, ToolCall
from ai_agent.tools import Tool

_USER_ID_RE = re.compile(r"USR-?\d+", re.IGNORECASE)

# Keyword scoring table for fixed tools (query_analytics is the zero-score fallback).
_TOOL_KEYWORDS: dict[str, list[str]] = {
    "top_content": ["top", "most watched", "popular", "trending", "best show", "most viewed"],
    "genre_popularity": ["genre", "genres"],
    "content_ratings": ["rating", "rated", "review"],
    "subscriptions": ["subscription", "plan", "cancel", "cancellation"],
    "dau": ["daily active", "dau", "user growth", "activity trend"],
    "popularity_predictions": ["popularity prediction", "will be popular", "predicted popular"],
    "recommendations": ["recommend", "suggest", "watch next"],
    "churn_risk": ["churn", "retain", "retention", "at risk", "leave", "losing"],
    "streaming_traffic": ["stream", "live", "real-time", "realtime", "traffic", "current"],
    "pipeline_health": ["pipeline", "health", "healthy", "data quality"],
}

# Chain rules: question pattern -> ordered list of tool names. Every multi-step
# sample question shown in the Command Center MUST be covered here so the offline
# demo chains correctly.
_CHAIN_RULES: list[tuple[re.Pattern, list[str]]] = [
    (re.compile(r"\bcompare\b.*\b(plan|subscription)", re.IGNORECASE),
     ["subscriptions", "churn_risk"]),
    (re.compile(r"\bchurn\b.*\brecommend", re.IGNORECASE),
     ["churn_risk", "recommendations"]),
]


class Brain(Protocol):
    """The interface every brain implements."""

    def next_action(self, state: AgentState, tools: list[Tool]) -> ToolCall | FinalAnswer: ...

    def summarize(self, state: AgentState) -> str: ...


def _score_tool(question: str) -> str:
    """Return the best-scoring fixed tool, or 'query_analytics' at zero score."""
    q = question.lower()
    best_name, best_score = "query_analytics", 0
    for name, keywords in _TOOL_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in q)
        if score > best_score:
            best_name, best_score = name, score
    return best_name


def _tool_args(tool_name: str, question: str) -> dict:
    """Derive deterministic args for a tool from the question text."""
    if tool_name in ("churn_risk", "recommendations"):
        match = _USER_ID_RE.search(question)
        if match:
            raw = match.group(0).upper().replace("USR", "USR-").replace("--", "-")
            return {"user_id": raw}
    if tool_name == "churn_risk" and "plan" in question.lower():
        return {"by": "plan"}
    if tool_name == "streaming_traffic" and "genre" in question.lower():
        return {"dimension": "genre"}
    if tool_name == "query_analytics":
        return {"question": question}
    return {}


class RuleBrain:
    """Deterministic, fully offline planner. Chains only the patterns encoded above."""

    name = "rule"

    def next_action(self, state: AgentState, tools: list[Tool]) -> ToolCall | FinalAnswer:
        question = state.user_query
        done = len(state.tool_history)

        # Multi-step chain rules take priority.
        for pattern, chain in _CHAIN_RULES:
            if pattern.search(question):
                if done < len(chain):
                    name = chain[done]
                    return ToolCall(tool_name=name, args=_tool_args(name, question),
                                    thought=f"Chain step {done + 1}/{len(chain)}: {name}.")
                return FinalAnswer(text=self.summarize(state),
                                   thought="Chain complete — summarising.")

        # Single-tool path: one tool call, then finish.
        if done == 0:
            name = _score_tool(question)
            return ToolCall(tool_name=name, args=_tool_args(name, question),
                            thought=f"Best match for this question: {name}.")
        return FinalAnswer(text=self.summarize(state), thought="Have the data — answering.")

    def summarize(self, state: AgentState) -> str:
        parts = [obs.text for obs in state.observations if getattr(obs, "text", "")]
        errors = [obs.error for obs in state.observations if getattr(obs, "error", "")]
        if not parts and errors:
            return "I could not retrieve the data: " + "; ".join(errors)
        return "\n\n".join(parts) if parts else "No data was returned."
