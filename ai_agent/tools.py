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


class SqlValidationError(ValueError):
    """Raised when generated SQL fails the runtime guardrail."""


_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE|UNLOAD)\b",
    re.IGNORECASE,
)
_TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)", re.IGNORECASE)


def validate_sql(sql: str, approved_tables: set[str]) -> None:
    """Validate that ``sql`` is a single read-only query over approved tables.

    Raises SqlValidationError on any violation. Returns None when the SQL is safe.
    """
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        raise SqlValidationError("Empty SQL.")
    if ";" in stripped:
        raise SqlValidationError("Multiple statements are not allowed.")

    head = stripped.upper().lstrip()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
        raise SqlValidationError("Only SELECT / WITH queries are allowed.")

    if _FORBIDDEN_KEYWORDS.search(stripped):
        raise SqlValidationError("SQL contains a forbidden write/DDL keyword.")

    if "INFORMATION_SCHEMA" in stripped.upper():
        raise SqlValidationError("Access to INFORMATION_SCHEMA is not allowed.")

    referenced = {ref.split(".")[-1].lower() for ref in _TABLE_REF.findall(stripped)}
    # CTE aliases are allowed; only flag refs that are neither approved nor a CTE name.
    cte_names = {
        m.lower()
        for m in re.findall(r"\b([a-zA-Z0-9_]+)\s+AS\s*\(", stripped, re.IGNORECASE)
    }
    unknown = referenced - {t.lower() for t in approved_tables} - cte_names
    if unknown:
        raise SqlValidationError(f"SQL references non-approved tables: {sorted(unknown)}")
