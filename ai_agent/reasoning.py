"""Reasoning runtime: state, brain actions, and the plan->act->observe engine.

The ReasoningEngine class is added in a later task; this module starts with the
shared dataclasses so the brains can import them.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4

from ai_agent.tools import ToolRegistry, ToolResult
from ai_agent.trace import StepKind, Trace, TraceEvent

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


def _signature(tool_name: str, args: dict) -> str:
    """Stable identity for a tool call, used by the loop guard."""
    return f"{tool_name}({sorted((args or {}).items())})"


class ReasoningEngine:
    """Brain-agnostic plan->act->observe loop. Never raises out of run()."""

    def __init__(self, registry: ToolRegistry, bedrock_brain=None, rule_brain=None) -> None:
        self.registry = registry
        self.bedrock_brain = bedrock_brain
        self.rule_brain = rule_brain
        if bedrock_brain is None and rule_brain is None:
            raise ValueError("ReasoningEngine needs at least one brain.")

    def run(self, query: str, history=None, on_event=None) -> EngineResult:
        trace = Trace(on_event=on_event)
        state = AgentState(conversation_id=str(uuid4()), user_query=query)

        brain = self.bedrock_brain or self.rule_brain
        state.active_brain = getattr(brain, "name", "rule")

        answer = ""
        while state.step_count < MAX_STEPS:
            state.step_count += 1

            t0 = time.perf_counter()
            try:
                action = brain.next_action(state, self.registry.all())
            except Exception:  # noqa: BLE001 — a brain failure must not crash the run
                if state.active_brain != "rule" and self.rule_brain is not None:
                    trace.add(TraceEvent(
                        kind=StepKind.FALLBACK,
                        title="Bedrock unavailable — switched to rule planner",
                        status="fallback", step_index=state.step_count))
                    brain = self.rule_brain
                    state.active_brain = "rule"
                    state.step_count -= 1  # the switch itself does not consume a step
                    continue
                answer = "The agent could not process this question."
                trace.add(TraceEvent(kind=StepKind.ERROR, title="Reasoning failed",
                                     status="error", step_index=state.step_count))
                break
            think_ms = int((time.perf_counter() - t0) * 1000)

            if action.thought:
                trace.add(TraceEvent(kind=StepKind.THOUGHT, title=action.thought,
                                     duration_ms=think_ms, step_index=state.step_count))

            if isinstance(action, FinalAnswer):
                answer = action.text
                trace.add(TraceEvent(kind=StepKind.FINAL, title="Answer ready",
                                     detail=action.text, step_index=state.step_count))
                break

            # action is a ToolCall — loop guard on repeated tool+args.
            signature = _signature(action.tool_name, action.args)
            if signature in state.tool_history:
                answer = brain.summarize(state)
                trace.add(TraceEvent(kind=StepKind.FINAL,
                                     title="Repeated call detected — finishing",
                                     detail=answer, step_index=state.step_count))
                break
            state.tool_history.append(signature)

            trace.add(TraceEvent(kind=StepKind.TOOL_CALL, title=action.tool_name,
                                 tool_name=action.tool_name, tool_args=action.args,
                                 step_index=state.step_count))

            t1 = time.perf_counter()
            result = self.registry.execute(action.tool_name, action.args, state)
            tool_ms = int((time.perf_counter() - t1) * 1000)
            state.observations.append(result)

            trace.add(TraceEvent(
                kind=StepKind.OBSERVATION,
                title=f"{action.tool_name} result",
                detail=result.error or result.text,
                sql=result.sql,
                duration_ms=tool_ms,
                status="error" if result.error else "ok",
                step_index=state.step_count))
        else:
            # Loop exhausted MAX_STEPS without a FinalAnswer — force a summary.
            answer = brain.summarize(state)
            trace.add(TraceEvent(kind=StepKind.FINAL, title="Step limit reached — summarising",
                                 detail=answer, step_index=state.step_count))

        return EngineResult(answer=answer, trace=trace, state=state)
