# AI Agent Reasoning Runtime — Design Spec

**Date:** 2026-05-18
**Phase:** AI Agent (`ai_agent/`)
**Status:** Approved design — ready for implementation planning

## Goal

Transform the AI agent phase from a single-shot keyword router into a real
**reasoning runtime** — an agent that plans, acts, observes, and chains tools to
answer multi-step analytics questions — and give it a standout showcase so it
visually highlights itself as the most advanced part of the project.

Two outcomes:

1. **Capability:** a genuine plan → act → observe loop that chains tools, with a
   resilient hybrid fallback (live AWS when available, deterministic offline mode
   otherwise).
2. **Showcase:** the reasoning made *visible* — a live execution trace surfaced in
   an upgraded "AI Command Center" app and a teaser section in the main dashboard.

## Problem with the current state

- The agent is **single-shot**: a question is keyword-scored, routed to exactly
  one agent/tool, runs one query, returns. It cannot reason or chain steps.
- Example it cannot answer today: *"Compare churn risk between our most-cancelled
  plan and our top-rated content's audience."*
- There are **two parallel implementations** that have drifted apart:
  `orchestrator.py` (7 keyword-routed agents) and `langgraph_agent.py` (a linear
  10-tool LangGraph). Routing in both is keyword-score based, not reasoning.

## Chosen approach

**Approach A — Custom ReAct loop over a unified tool registry.**

A new `ReasoningEngine` runs an iterative plan → act → observe loop. A
brain-agnostic engine drives the lifecycle; only the "decision maker" (the brain)
varies. Two interchangeable brains: a `BedrockBrain` using Bedrock's native
tool-use API for real LLM reasoning, and a deterministic `RuleBrain` that still
chains a known set of multi-step patterns fully offline. Every loop step emits a
structured trace event, which powers the showcase UI.

Rejected alternatives:

- **Approach B (full LangGraph cyclic-graph rewrite):** the offline rule fallback
  and a clean streamed trace are awkward to bolt onto LangGraph internals —
  fragile for a "never breaks during a demo" requirement.
- **Approach C (LLM planner in front of the orchestrator):** the 7 agents stay
  single-shot, so it is not real tool chaining, and it leaves the codebase with
  two diverging implementations.

### Guiding principle

This is a transition from **tool router** to **reasoning runtime**. The reasoning
lifecycle, tool execution, trace capture, retries, fallback, and state must not
depend on the LLM provider. Only the decision maker varies. The system is
**bounded intelligence**: capable, but constrained, observable, interruptible,
and replayable.

## Architecture

Five new/reworked modules in `ai_agent/`, plus showcase wiring:

```
ai_agent/
  tools.py        NEW     unified tool registry: typed Tool specs
                          (name, description, JSON-schema params, fn -> ToolResult)
  trace.py        NEW     TraceEvent + Trace: structured record of every loop step,
                          with to_json / from_json serialization
  brains.py       NEW     BedrockBrain (native tool-use) + RuleBrain (offline planner)
  reasoning.py    NEW     AgentState + ReasoningEngine: the plan->act->observe loop
  langgraph_agent.py  REWORK  thin shim delegating to ReasoningEngine; keeps the
                              ConversationSession API so api.py / cli.py still work
  streamlit_app.py    REWORK  "AI Command Center" showcase with live reasoning trace
  demo_traces/        NEW     serialized Trace fixtures (dashboard demo + regression)
```

**Untouched (no regression):** `orchestrator.py` and the 7 `agents/` stay as-is, so
the FastAPI `api.py` and the dashboard's `AnalyticsAgent` keep working. The engine
reuses the tool *logic* from `langgraph_agent.py` by moving those functions into
`tools.py`. New runtime layer over existing tools — incremental, not a rewrite.

**Key decision — one loop, two brains.** The engine is brain-agnostic. It hands the
brain the conversation, the tool catalog, and observations so far; the brain
returns either a `ToolCall` or a `FinalAnswer`. The engine auto-selects
`BedrockBrain` when Bedrock is reachable, else `RuleBrain`. A Bedrock failure
mid-run falls back to `RuleBrain` for the rest of that run. The active brain is
shown in the UI as a mode badge — the fallback is honest and visible, never hidden.

## Components

### `ai_agent/tools.py` — tool registry

Each tool is a wrapped, inspectable spec — not a raw function — so the registry is
serializable, inspectable, LLM-friendly, and UI-friendly:

```python
@dataclass(frozen=True)
class Tool:
    name: str
    description: str          # LLM-facing
    parameters: dict          # JSON Schema -> feeds Bedrock toolConfig directly
    fn: Callable[[dict, AgentState], ToolResult]

@dataclass
class ToolResult:
    text: str                 # markdown summary for the brain + UI
    dataframe: pd.DataFrame | None = None
    sql: str = ""             # SQL actually executed, if any
    error: str = ""
```

Two tiers of tools — this is the **SQL guardrail**:

- **Fixed-SQL tools** (~10, ported from `langgraph_agent.py`): `top_content`,
  `genre_popularity`, `churn_risk`, `recommendations`, `content_ratings`,
  `subscriptions`, `dau`, `popularity_predictions`, `streaming_traffic`,
  `pipeline_health`. SQL is templated in code; the brain only fills typed
  parameters (`limit`, `user_id`, `plan_id`, `dimension`).
- **One escape-hatch tool** `query_analytics(question: str)`: for open-ended
  questions. The brain passes a natural-language intent, **never SQL**. Internally
  it calls the existing `BedrockSQLGenerator` / `RuleBasedSQLGenerator`, both
  constrained to `catalog.py`'s `APPROVED_TABLES`. Generated SQL then passes
  `validate_sql()` before reaching Athena.

The reasoning brain decides *what to ask*; it never writes SQL. The catalog plus
validator is the wall against hallucinated joins, schema drift, and SQL injection.

**`validate_sql()` rules (paranoid by default):**

- Single statement only.
- `SELECT` only.
- Every referenced table must be in the `APPROVED_TABLES` whitelist.
- Block `INFORMATION_SCHEMA`.
- Reject `INSERT / UPDATE / DELETE / DROP / ALTER / CREATE / UNLOAD` as keyword
  tokens even though SELECT-only already catches most.

### `ai_agent/trace.py` — observable reasoning

```python
class StepKind(Enum):
    THOUGHT, TOOL_CALL, OBSERVATION, FALLBACK, SUMMARY, FINAL, ERROR

@dataclass
class TraceEvent:
    kind: StepKind
    title: str
    detail: str = ""          # thought text / observation summary
    tool_name: str = ""
    tool_args: dict = field(default_factory=dict)
    sql: str = ""
    duration_ms: int = 0
    status: str = "ok"        # ok | error | fallback
    step_index: int = 0       # for "Step 3/5" depth labels
```

- `Trace` collects events and accepts an optional `on_event` callback so any
  consumer (Streamlit, FastAPI, CLI, logs) can render each step live.
- `Trace.to_json()` / `Trace.from_json()` serialize a full run. Serialized traces
  are reused as the dashboard demo and as regression fixtures — replay over fakery.
- `StepKind.SUMMARY` lets the brain compress observations into a memory snapshot so
  long multi-step reasoning does not bloat the trace.

### `ai_agent/reasoning.py` — `AgentState` + `ReasoningEngine`

`AgentState` is the explicit state object carried through the whole loop:

```python
@dataclass
class AgentState:
    conversation_id: str
    user_query: str
    step_count: int = 0
    observations: list[ToolResult] = field(default_factory=list)
    tool_history: list[str] = field(default_factory=list)
    intermediate_results: dict = field(default_factory=dict)
    memory: dict = field(default_factory=dict)        # user prefs / prior selections
    active_brain: str = "rule"
    started_at: datetime = field(default_factory=datetime.utcnow)
```

`ReasoningEngine.run(query, history) -> EngineResult` loop:

1. Build / extend `AgentState`.
2. Ask the brain for the next action given query + tool catalog + observations.
3. If `ToolCall` -> execute via registry, append `ToolResult` to
   `state.observations`, emit `TOOL_CALL` + `OBSERVATION` events, loop.
4. If `FinalAnswer` -> emit `FINAL`, return `EngineResult(answer, trace, state)`.
5. Cap at `MAX_STEPS = 5`; on cap, brain is forced to summarize from observations
   into a `FINAL` (no hard crash, no silent truncation).

### `ai_agent/brains.py` — two interchangeable brains

Shared protocol: `next_action(state, tools) -> ToolCall | FinalAnswer`.

- `BedrockBrain` — Bedrock Converse with `toolConfig` built from the registry's
  JSON schemas; native multi-turn tool-use. Real reasoning, real chaining.
  THOUGHT text is kept terse by prompt design (concise thoughts read as smarter).
- `RuleBrain` — deterministic planner. Keyword scoring picks the first tool; a
  small set of encoded **chain rules** handles known multi-step intents (e.g.
  "compare" + "plan" -> `subscriptions` then `churn_risk` per plan). Honest scope:
  it chains the patterns we encode, not arbitrary reasoning — but it always runs,
  fully offline. Deterministic logic is not presented as reasoning.

The engine picks `BedrockBrain` when Bedrock is reachable; a Bedrock exception
mid-run flips `state.active_brain` to `rule` and emits a `FALLBACK` event so the
UI shows the switch.

## Data flow

```
User question
   |
   v
ReasoningEngine.run()  -- builds AgentState (conversation_id, query, memory)
   |
   v  +------------------------- loop (<= MAX_STEPS) -------------------------+
   |  | Brain.next_action(state, tools)                                      |
   |  |    |- ToolCall  -> registry.execute() -> ToolResult                  |
   |  |    |                  |- append to state.observations                |
   |  |    |                  +- emit TOOL_CALL + OBSERVATION -> on_event     |
   |  |    +- FinalAnswer -> emit FINAL -> break                              |
   |  +----------------------------------------------------------------------+
   |    (Bedrock error -> emit FALLBACK, switch active_brain="rule", continue)
   v
EngineResult(answer, trace, state)  ->  UI / API / CLI
```

Every consumer drives the *same* engine; they differ only in how they render
`TraceEvent`s via the `on_event` callback. One runtime everywhere — no duplicated
orchestration logic, no drift.

## Showcase UI

### AI Command Center — reworked `streamlit_app.py`

A three-zone layout on the existing dark IPL theme:

- **Hero strip (top):** title "Autonomous Analytics Runtime", a one-line pitch, a
  live **mode badge** (`Bedrock reasoning` green / `Local rule planner` amber), and
  tool count. Fallback state is visible.
- **Left column — Conversation:** chat input, message history, the final answer
  with its chart. Sample questions are deliberately multi-step (e.g. "Compare churn
  risk between our most-cancelled plan and our top-rated content's audience") to
  show off chaining.
- **Right column — Reasoning Trace (the showcase):** as the loop runs, each
  `TraceEvent` streams in as a styled card in a **vertical timeline** — events
  connected top-to-bottom with the inter-step latency shown between them:

  ```
  [1] THOUGHT      Need churn comparison by plan.
        | 112ms
  [2] TOOL_CALL    subscriptions(dimension="plan")
        | 428ms
  [3] OBSERVATION  6 rows  (SQL + mini preview, collapsible)
        | 91ms
  [4] SUMMARY      PREMIUM highest cancellations; ...
        | 54ms
  [5] FINAL        ...
  ```

  Each card shows a step-depth label ("Step 3/5") and latency, so the viewer
  literally watches bounded, controlled cognition. Card kinds: `THOUGHT`,
  `TOOL_CALL` (tool + args chip), `OBSERVATION` (collapsible SQL + row count +
  preview + `duration_ms`), `FALLBACK` (amber), `SUMMARY`, `FINAL` (green capstone).

### Dashboard entry — new section in `dashboard/app.py`

A prominent "AI Agent" section with: a hero card framing it as the runtime, 3-4
capability tiles, and a **canned mini-demo** — a *real serialized `Trace`* loaded
from `ai_agent/demo_traces/*.json` and replayed inline (works with zero AWS,
deterministic, presentation-safe) — plus a button to launch the full Command
Center app.

## Failure semantics — the loop never crashes

| Failure | Handling |
|---|---|
| Tool raises / returns error | Caught -> `ToolResult(error=...)`, emitted as `OBSERVATION` with `status=error`. The brain sees the error as an observation and can retry differently or conclude. |
| Bedrock call fails | `FALLBACK` event -> switch `active_brain="rule"` -> loop continues. One-time switch per run. |
| Athena query timeout | Existing 30-tick timeout returns an error result; treated as a tool error. |
| Brain repeats same tool+args | Engine **loop guard** (checks `state.tool_history`) forces a `FINAL`. No infinite loops. |
| `MAX_STEPS` reached | Brain forced to summarize from `state.observations` into a `FINAL`. |
| Empty result set | Tool returns `ToolResult(text="no matching rows")`; brain handles gracefully. |

Errors stay *inside* the reasoning model as observations, instead of exploding
outside it.

**Retry policy — deliberately minimal:** one retry for transient errors only
(Athena throttling, Bedrock 5xx) with a short fixed backoff. Bedrock: one retry,
then fallback. No exponential retry storms — bounded and predictable.

**Persistence:** in-session only. `ConversationSession` keeps history and
`AgentState.memory` in memory per Streamlit session — no database (YAGNI: no vector
DB, Redis, or embeddings). The one durable artifact is serialized traces under
`ai_agent/demo_traces/`.

## Testing strategy

TDD. Fully offline — **no live AWS in any test**. New `tests/ai_agent/` directory
(pytest); a new `requirements-dev.txt` with `pytest`.

| Component | Test type | Coverage |
|---|---|---|
| `tools.py` | functional | Each tool against a fake Athena/PG client returning canned rows: `ToolResult` shape, SQL templating, error path. |
| `validate_sql()` | security / table-driven | Accepts good `SELECT`s; rejects DDL/DML, `INFORMATION_SCHEMA`, multi-statement, non-whitelisted tables. |
| `RuleBrain` | deterministic | Query -> expected `ToolCall` sequence. |
| `ReasoningEngine` | orchestration | Scripted `FakeBrain` + fake tools: observation accumulation, `MAX_STEPS` cap, loop-guard, fallback path. |
| `BedrockBrain` | integration contract | Mocked `converse` client: `toolConfig` built from registry; `tool_use` blocks parse into `ToolCall`s. |
| `Trace` | serialization | `to_json` -> `from_json` round-trip equality. |

Two additional engine tests:

- **Partial-failure continuation:** Tool A succeeds, Tool B fails, Tool C still
  executes, a `FINAL` is still produced — validates resilience, non-fatal
  execution, and state consistency.
- **Trace latency assertions:** structural, not strict timing — e.g. every event's
  `duration_ms >= 0` — since timing fields are now core runtime artifacts.

Testing the `ReasoningEngine` with a `FakeBrain` tests *orchestration semantics*,
not LLM behavior — proper systems testing.

## Implementation order

Each step is its own commit + push, on a new branch off `main`. Foundation first,
UI last.

1. `tools.py` + tests
2. `trace.py` (incl. `to_json` / `from_json`) + tests
3. `brains.py` — `RuleBrain` + tests
4. `reasoning.py` — `AgentState` + `ReasoningEngine` + tests
5. `brains.py` — `BedrockBrain` + tests
6. `langgraph_agent.py` shim rework (delegate to `ReasoningEngine`)
   - **CLI-first smoke-test checkpoint:** validate the runtime end-to-end via
     `cli.py` before building any UI — debugging orchestration/state/traces is far
     faster in the CLI than inside Streamlit.
7. `streamlit_app.py` — AI Command Center
8. `dashboard/app.py` — AI Agent section + serialized demo trace

## Future evolution (out of scope now, noted so it is not forgotten)

- `ToolResult.artifact_ref: str` — large dataframes should eventually become
  references (cached IDs / S3) rather than inline frames, to keep traces and APIs
  light. Acceptable inline for current scope.
- Additional brains (`OpenAIBrain`, `LocalLLMBrain`, `HybridBrain`) — the
  brain-agnostic engine already supports this without orchestration changes.
- Cross-session memory persistence — only if a real need appears.

## Success criteria

- The agent answers a multi-step question that requires chaining 2+ tools.
- The reasoning trace is visible, streamed, and timed in the Command Center.
- The system runs end-to-end with no AWS access (RuleBrain + offline tools), and
  the fallback is shown, not hidden.
- All `tests/ai_agent/` tests pass offline.
- The dashboard shows an AI Agent section with a replayed real trace and a launch
  button.
- No regression in `orchestrator.py`, `api.py`, or the dashboard's `AnalyticsAgent`.
