"""Decision-making brains for the reasoning runtime.

A brain answers one question: given the state and the tool catalog, what is the
next action? RuleBrain is deterministic and offline; BedrockBrain (Task 13) uses
the LLM. The ReasoningEngine is brain-agnostic.
"""

from __future__ import annotations

import re
import time
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


_SYSTEM_PROMPT = (
    "You are JioHotstar's analytics agent. Answer the user's question by calling "
    "tools. Prefer the specific fixed tools; use 'query_analytics' ONLY when no "
    "fixed tool fits. Keep any reasoning text terse — one short sentence. When you "
    "have enough information, stop calling tools and give the final answer."
)


def build_tool_config(tools: list[Tool]) -> dict:
    """Build a Bedrock Converse toolConfig from the registry's tool specs."""
    return {
        "tools": [
            {
                "toolSpec": {
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": {"json": tool.parameters or {"type": "object",
                                                                 "properties": {}}},
                }
            }
            for tool in tools
        ]
    }


def _observations_block(state: AgentState) -> str:
    if not state.observations:
        return ""
    lines = []
    for i, obs in enumerate(state.observations, 1):
        body = getattr(obs, "error", "") or getattr(obs, "text", "")
        lines.append(f"[observation {i}] {body}")
    return "\n".join(lines)


class BedrockBrain:
    """LLM-backed brain using Bedrock's Converse tool-use API."""

    name = "bedrock"

    def __init__(self, client, model_id: str) -> None:
        self._client = client
        self._model_id = model_id

    def _converse(self, user_text: str, tools: list[Tool], with_tools: bool = True) -> dict:
        kwargs = {
            "modelId": self._model_id,
            "system": [{"text": _SYSTEM_PROMPT}],
            "messages": [{"role": "user", "content": [{"text": user_text}]}],
            "inferenceConfig": {"temperature": 0.2, "maxTokens": 700},
        }
        if with_tools:
            kwargs["toolConfig"] = build_tool_config(tools)
        try:
            return self._client.converse(**kwargs)
        except Exception:  # noqa: BLE001 — one retry, then the engine falls back
            time.sleep(0.5)
            return self._client.converse(**kwargs)

    def next_action(self, state: AgentState, tools: list[Tool]) -> ToolCall | FinalAnswer:
        prompt = f"User question: {state.user_query}"
        observations = _observations_block(state)
        if observations:
            prompt += f"\n\nObservations so far:\n{observations}"

        response = self._converse(prompt, tools)
        content = response.get("output", {}).get("message", {}).get("content", [])

        thought = "".join(b.get("text", "") for b in content if "text" in b).strip()
        tool_use = next((b["toolUse"] for b in content if "toolUse" in b), None)

        if tool_use is not None:
            return ToolCall(tool_name=tool_use["name"],
                            args=tool_use.get("input", {}) or {},
                            thought=thought or f"Calling {tool_use['name']}.")
        return FinalAnswer(text=thought or "I could not find an answer.", thought=thought)

    def summarize(self, state: AgentState) -> str:
        prompt = (f"Question: {state.user_query}\n\nObservations:\n"
                  f"{_observations_block(state)}\n\nGive a concise final answer.")
        try:
            response = self._converse(prompt, tools=[], with_tools=False)
            content = response.get("output", {}).get("message", {}).get("content", [])
            text = "".join(b.get("text", "") for b in content if "text" in b).strip()
            return text or "No answer could be produced."
        except Exception:  # noqa: BLE001 — summary must not crash the run
            parts = [getattr(o, "text", "") for o in state.observations if getattr(o, "text", "")]
            return "\n\n".join(parts) if parts else "No answer could be produced."
