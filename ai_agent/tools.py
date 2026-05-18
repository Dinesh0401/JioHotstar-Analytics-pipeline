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
