"""Tool framework for the AI agent reasoning runtime.

Defines the typed tool spec, the structured tool result, the registry,
and the SQL guardrail. Concrete tools live in ``analytics_tools.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:  # avoids a circular import with reasoning.py
    from ai_agent.reasoning import AgentState


@dataclass
class ToolResult:
    """Standardised result returned by every tool."""

    text: str
    dataframe: pd.DataFrame | None = None
    sql: str = ""
    error: str = ""
    source: str = ""  # data origin: fixed_sql | generated_sql | streaming | replay


@dataclass(frozen=True)
class Tool:
    """An inspectable, serialisable tool spec."""

    name: str
    description: str
    parameters: dict  # JSON Schema object — feeds Bedrock toolConfig directly
    fn: Callable[[dict, "AgentState"], ToolResult]


class ToolRegistry:
    """An ordered, name-indexed collection of tools."""

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"Tool '{tool.name}' is already registered.")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def all(self) -> list[Tool]:
        return list(self._tools.values())

    def names(self) -> list[str]:
        return list(self._tools.keys())

    def execute(self, name: str, params: dict, state: "AgentState" | None) -> ToolResult:
        """Run a tool by name. Never raises — failures become error results."""
        tool = self._tools.get(name)
        if tool is None:
            return ToolResult(text="", error=f"Unknown tool '{name}'.")
        try:
            return tool.fn(params or {}, state)
        except Exception as exc:  # tool failures become observations, never crashes
            return ToolResult(text="", error=f"{type(exc).__name__}: {exc}")
