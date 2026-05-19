"""Reasoning runtime: state, brain actions, and the plan->act->observe engine.

The ReasoningEngine class is added in a later task; this module starts with the
shared dataclasses so the brains can import them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ai_agent.tools import ToolResult
from ai_agent.trace import Trace

MAX_STEPS = 5


@dataclass
class AgentState:
    """Explicit state carried through the whole reasoning loop."""

    conversation_id: str
    user_query: str
    step_count: int = 0
    observations: list[ToolResult] = field(default_factory=list)
    tool_history: list[str] = field(default_factory=list)
    intermediate_results: dict = field(default_factory=dict)
    memory: dict = field(default_factory=dict)  # user prefs / prior selections
    active_brain: str = "rule"
    started_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ToolCall:
    """A brain decision: call a tool with these args."""

    tool_name: str
    args: dict = field(default_factory=dict)
    thought: str = ""


@dataclass
class FinalAnswer:
    """A brain decision: stop and return this answer."""

    text: str
    thought: str = ""


@dataclass
class EngineResult:
    """The outcome of one ReasoningEngine.run() call."""

    answer: str
    trace: Trace
    state: AgentState
