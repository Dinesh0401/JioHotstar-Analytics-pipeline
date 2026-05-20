# Architecture Rationale

This document explains **why the AI reasoning runtime in `ai_agent/` was built
the way it was**, in the terms an engineer would defend in a design review or
an interview. The README describes *what* the system does; this doc explains
*why* each major decision exists, what alternative was rejected, and what the
system intentionally does NOT do.

Read this alongside:

- `docs/superpowers/specs/2026-05-18-ai-agent-reasoning-runtime-design.md` — the design spec.
- `docs/superpowers/plans/2026-05-18-ai-agent-reasoning-runtime.md` — the 16-task implementation plan.

The tone here is deliberate: this is engineering, not vision. The system is
small, bounded, and conservative on purpose.

---

## 1. Problem framing

Why most analytics-agent designs fail in practice.

**Unbounded orchestration.** A "do something, then think again" loop with no
hard cap eventually spirals: the model second-guesses itself, calls the same
tool repeatedly, or recurses into nested sub-tasks. Cost, latency, and
debuggability all degrade together. The agent's confidence is unchanged; the
operator's confidence collapses.

**Hallucinated SQL hitting a real warehouse.** Letting the model write
arbitrary SQL against a production lakehouse is one schema-rename away from
silent wrong answers, expensive scans, or — with the wrong permissions —
mutating queries that should never have been possible.

**Opaque reasoning.** When the only output is a final answer, a wrong answer
is unfalsifiable. You cannot tell whether the model misunderstood the
question, picked the wrong tool, ran a bad query, or hallucinated from an
empty result. Debugging becomes guessing.

**Single-provider coupling.** When the LLM call sits inside the orchestration
loop, the provider's outage *is* the system's outage. There is no honest
fallback path because no other component knows how to make decisions.

**Demo fragility.** The systems that look most impressive in a video are
often the ones that fail live: a credential expires, a region throttles, a
network blips. A system that requires a working LLM call to do anything has
no story for the day the LLM call doesn't work.

**Memory bloat.** "Just keep the whole conversation in context" hits token
limits, costs, and latency at roughly the same time. Without explicit
boundaries, context inflation is silent until it isn't.

**Generic-tool dominance.** A single "do anything" tool tends to swallow
every workflow because the model finds it flexible. Specific, well-named
tools become dead code; trace structure collapses to a single SQL call; the
system's safety boundaries quietly stop mattering.

This system is shaped around avoiding those specific failure modes, not
around an abstract vision of "AI agents."

---

## 2. Design goals

Six explicit goals, each one mapping to a concrete mechanism in the code.

| Goal | Mechanism |
|---|---|
| **Bounded execution** | `MAX_STEPS = 5`, loop guard on repeated `(tool, args)`, one retry per transient error |
| **Deterministic fallback** | `RuleBrain` — keyword routing + encoded chain rules, fully offline, no network |
| **Offline testability** | All DB access funnels through `ai_agent/datasources.py`; engine tested with a scripted `FakeBrain` |
| **Transparent reasoning** | Every step emits a structured `TraceEvent` with `duration_ms`, `step_index`, and `kind` |
| **Safe analytics access** | Typed fixed-SQL tools + a single `query_analytics` escape hatch guarded by `validate_sql()` |
| **Graceful degradation** | Bedrock failure → `FALLBACK` event → engine switches to `RuleBrain` mid-run without consuming a step |

These are not aspirations. Each one is enforced by a specific piece of code
that a reviewer can point at.

---

## 3. Why the engine is brain-agnostic

The single most important separation in the system.

**The engine owns the runtime.** `ReasoningEngine` owns: `AgentState`,
the plan → act → observe loop, the loop guard, `MAX_STEPS`, retries, the
fallback transition, trace emission, and tool dispatch.

**The brain owns one question.** A `Brain` answers
`next_action(state, tools) → ToolCall | FinalAnswer` and
`summarize(state) → str`. Nothing else.

This split exists because **reasoning-quality concerns and runtime-quality
concerns evolve at different speeds**. The model improves continuously;
orchestration changes rarely. Coupling them means every model upgrade is a
runtime change, and every runtime fix risks regressing model behavior.

What this separation gives:

- **Portability.** `BedrockBrain` and `RuleBrain` already coexist behind the
  same protocol. Adding `OpenAIBrain` or `LocalLLMBrain` is a new file —
  not a refactor.
- **Deterministic testing.** `tests/ai_agent/test_reasoning_engine.py` uses
  a `FakeBrain` that returns a scripted sequence of actions. Every
  orchestration property — observation accumulation, step cap, loop guard,
  partial-failure continuation, fallback — is tested without invoking any
  LLM. The tests exercise *orchestration semantics*, not model behavior.
- **Graceful degradation.** Because the engine is the one switching brains,
  fallback is a state transition (`active_brain = "rule"`, emit
  `FALLBACK`), not an exception. The loop keeps going from the same step
  count.

**Alternative considered: full LangGraph cyclic-graph rewrite.** Rejected
because the rule fallback and a clean streamed trace become awkward to bolt
onto LangGraph internals. The runtime needs to fall back *between* steps
based on its own state — a property that's natural in a small custom loop
and fights the abstractions of a graph framework.

---

## 4. Why typed tools through a registry

`Tool` is a frozen dataclass — `name`, `description`, `parameters` (JSON
Schema), `fn`. `ToolResult` is the standardized return value — `text`,
`dataframe`, `sql`, `error`, `source`. `ToolRegistry` is the single dispatch
point: `register`, `get`, `all`, `names`, `execute`.

This costs more lines than `tools = {"foo": foo_fn}`. The reasons it's
worth those lines:

- **Execution contracts.** The brain only sees the typed `parameters`
  schema, not Python signatures. A `limit: int` constraint is the same
  whether the brain is an LLM or a rule planner; both must produce valid
  args or the registry rejects the call.
- **Single source of truth for Bedrock `toolConfig`.** `build_tool_config()`
  reads `tool.parameters` directly. There is no second copy of the tool
  spec to drift from the function. Add a tool, the LLM sees it; rename a
  parameter, the schema follows.
- **Crash safety.** `ToolRegistry.execute(name, params, state)` is wrapped
  in `try/except Exception`. Tool errors become
  `ToolResult(error="<type>: <message>")` instead of unwinding the loop.
  The engine sees observations, not exceptions.
- **Introspection.** The Command Center can list registered tools, the CLI
  can print them, future evaluation code can iterate them. The registry is
  the catalog.
- **Decoupled definition and execution.** Tool definitions live in
  `analytics_tools.py`; the engine never imports them by name. Adding a
  tool is one registration; nothing in the engine changes.

**Why two tiers (fixed-SQL + one escape hatch).** Most analytics questions
are repetitive — "top content", "churn by plan", "DAU trend". Templating
their SQL in code makes them cheap, fast, deterministic, and immune to
hallucination. `query_analytics` exists for the genuinely open-ended cases
that no fixed tool fits — and it is described to the brain explicitly as
*"use ONLY when no fixed tool fits"*. The `RuleBrain` enforces the same
policy by construction: keyword scoring picks a fixed tool whenever any
score is non-zero; `query_analytics` is only reached at score zero.

---

## 5. Why the trace exists

The trace is not debug logging. It is the runtime's first-class output.

Every loop step emits a `TraceEvent` carrying `kind` (one of `THOUGHT`,
`TOOL_CALL`, `OBSERVATION`, `FALLBACK`, `SUMMARY`, `FINAL`, `ERROR`),
`title`, optional `detail`, `tool_name`, `tool_args`, `sql`,
`duration_ms`, `status`, `step_index`. `Trace` collects them and accepts an
optional `on_event` callback so consumers can render events as the loop
runs.

What this design buys:

- **Replayability.** `Trace.to_json` / `Trace.from_json` serialise a full
  run. The dashboard's "AI Agent" section replays a real serialised trace
  from `ai_agent/demo_traces/compare_churn_by_plan.json`. The demo is not
  a screenshot or a mock; it is a recording of an actual offline run.
- **Reproducible debugging.** A failing run can be saved as a trace JSON
  and committed as a regression fixture. The test asserts the same kinds
  appear in the same order, without rerunning the LLM.
- **Operational observability.** Streamlit, the CLI `--engine` mode, and
  (in the future) the FastAPI app all consume the same event stream via
  `on_event`. One callback, three surfaces.
- **A foundation for evaluation.** `ToolResult.source` (`fixed_sql` /
  `generated_sql` / `streaming` / `replay`) lets you compute escape-hatch
  dominance after the fact. `step_index` and `duration_ms` give you
  step-budget and latency distributions. The metrics layer is deferred,
  but the data already exists in every serialised trace.
- **Honest storytelling.** A streamed trace lets a viewer see the bounded
  reasoning happen, including the fallback transition. There is nothing to
  fake. The system is what the trace says it did.

The decision to make the trace serialisable from day one is what makes
every later evolution (evaluation, regression tests, demo recordings)
cheap.

---

## 6. Why `RuleBrain` exists

A deterministic offline brain is not a placeholder. It is a load-bearing
component.

**Offline mode is real.** With no AWS credentials, the engine selects
`RuleBrain`, the registry's fixed tools either succeed (against a stubbed
datasource) or return error observations (against unreachable Athena), and
the loop still produces a `FinalAnswer`. The Command Center reads
"🟡 Local rule planner" in the hero strip. Nothing is faked.

**CI is deterministic.** The 59-test suite runs end-to-end without AWS.
The orchestration tests script a `FakeBrain` to assert engine behavior;
the integration-style tests (`test_conversation_session`, `test_demo_traces`)
drive `RuleBrain` against monkeypatched datasources. Tests are fast (~3s),
stable, and don't depend on Bedrock availability or network.

**Demos and interviews are stable.** A live demo of an agent that requires
Bedrock to work is a demo waiting to fail. The dashboard demo replays a
real trace; the Command Center will run end-to-end with no credentials.

**The fallback path is honest.** When Bedrock fails mid-run, the engine
switches to `RuleBrain` and emits a `FALLBACK` event. There is no silent
retry that pretends nothing happened. The UI shows the switch.

**It forces a clean protocol.** Two brains behind one `Brain` protocol
means the protocol is real, not aspirational. A future `OpenAIBrain` can
slot in without orchestration changes.

**The honest scope.** `RuleBrain` is keyword scoring plus a small set of
encoded chain rules. It is not pretending to reason. The constraint that
the encoded chain rules must cover every multi-step sample question shown
in the Command Center is documented in the spec — the offline demo chains
correctly because the rules and the UI samples were written together.

---

## 7. Why SQL validation exists

The brain decides *what to ask*; it never writes SQL. That sentence is the
whole safety boundary.

**The two tiers, again, but from the safety angle.**

The ~10 **fixed-SQL tools** template their SQL in Python. The brain only
fills typed parameters (`limit`, `user_id`, `plan_id`, `dimension`). There
is no path for the brain to inject arbitrary SQL into a fixed tool. These
tools deliberately *bypass* `validate_sql` — their SQL is code, not data.

The one **escape hatch** `query_analytics(question)` accepts a
natural-language question and calls the existing `BedrockSQLGenerator` /
`RuleBasedSQLGenerator` (both constrained to `catalog.py`'s
`APPROVED_TABLES`). The generated SQL is then run through
`validate_sql()` before reaching Athena.

**`validate_sql` is intentionally paranoid.**

- Single statement only — multi-statement strings rejected.
- `SELECT` or `WITH` only.
- Forbidden keyword denylist: `INSERT`, `UPDATE`, `DELETE`, `DROP`,
  `ALTER`, `CREATE`, `TRUNCATE`, `MERGE`, `GRANT`, `REVOKE`, `UNLOAD`.
- `INFORMATION_SCHEMA` blocked entirely.
- Every referenced table must be in the approved-table whitelist
  (`APPROVED_TABLES.keys()`), with a CTE-alias allowance so `WITH t AS
  (SELECT …) SELECT * FROM t` works.

**Scope is bounded honestly.** This is "bounded analytics query validation"
— guardrails over a known set of tables — not "arbitrary SQL security."
A determined attacker writing through the escape hatch is out of scope.
The threat model is *the LLM producing wrong-but-plausible SQL against
the wrong table*, which the whitelist catches.

**What the model can't do, by construction:**

- Write to any table.
- Run DDL against the catalog.
- Query `INFORMATION_SCHEMA` to discover other tables.
- Reference a non-approved table by name.
- Run multiple statements in one call.

These guarantees come from `validate_sql`, not from prompting.

---

## 8. Failure semantics — the loop never crashes

A `ReasoningEngine.run(...)` call never raises. Every failure path produces
a `FinalAnswer`. This is the design property the rest of the runtime
depends on.

| Failure | Handling |
|---|---|
| Tool raises an exception | `ToolRegistry.execute` catches it → `ToolResult(error="<type>: <msg>")` → emitted as an `OBSERVATION` with `status="error"`. The brain sees the error as an observation and can react. |
| Tool returns `ToolResult(error=...)` | Same as above — `error` is the canonical "this didn't work" signal. |
| Bedrock raises mid-run | Engine catches it → emits `FALLBACK` event → `active_brain = "rule"` → loop continues from the same step. The fallback switch does not consume a step. |
| Brain calls the same `(tool_name, args)` twice | Loop guard checks `state.tool_history` for the signature → forces `summarize` → emits `FINAL`. |
| `MAX_STEPS = 5` reached | `while…else` branch forces the brain to summarise from `state.observations` → emits `FINAL`. No hard stop, no silent truncation. |
| Empty result set from a tool | Tool returns `ToolResult(text="no matching rows")`. Treated as a normal observation. |
| Both brains fail | Engine emits `ERROR` event with a graceful message; the run still returns an `EngineResult`. |

Two retry policies, both deliberately minimal:

- **Datasource retry.** `datasources._retry_once` runs the query, sleeps 1s
  on `DataSourceError`, retries exactly once. No exponential storms.
- **Bedrock retry.** `BedrockBrain._converse` retries the `converse` call
  once after a 0.5s sleep before propagating. If the second attempt fails,
  the engine's fallback path takes over.

The combined guarantee: bounded steps × bounded retries × crash-safe
dispatch × graceful brain fallback = a loop that always finishes, with a
trace that documents how.

---

## 9. What this system does NOT do

Stating limits explicitly is more credible than implying capabilities.
Strong engineers trust systems more when the boundary is visible.

- **Not AGI.** Not autonomous in any meaningful sense. The agent answers
  one bounded question at a time and stops.
- **Not long-horizon planning.** The plan → act → observe loop is capped
  at 5 steps. There is no multi-session task tracking.
- **Not arbitrary tool execution.** Tools are a closed registry. The brain
  can only call what's registered; the registry can only run what's typed.
- **Not arbitrary SQL.** Fixed tools template their SQL in code; the
  escape hatch is guardrailed. The brain never writes SQL the validator
  hasn't approved.
- **Not cross-session memory.** `AgentState.memory` exists as a field but
  is per-session, in-memory. There is no vector store, no embeddings, no
  Redis, no persistence.
- **Not a multi-agent swarm.** One engine, one brain at a time. The 7
  specialised agents under `ai_agent/agents/` are an older orchestrator
  pattern, untouched by this runtime and explicitly out of its scope.
- **Not self-modifying.** No code generation, no dynamic tool registration
  at runtime, no prompt rewriting based on prior runs.
- **Not provider-coupled.** No part of the runtime requires Bedrock to
  function. The system is designed to degrade, not depend.
- **Not a research artifact.** No novel model, no fine-tuning, no
  embedding pipeline. This is a runtime around an existing API.

The system is conservative on purpose. Most of the "capabilities" listed
above are easy to add and hard to control; their absence is a feature.

---

## 10. Tradeoffs accepted

Every decision had a cost. Naming them is part of owning them.

| Decision | Cost accepted |
|---|---|
| Custom ReAct loop instead of LangGraph | More code to maintain; full ownership of the loop semantics |
| Two brains, hand-rolled protocol | No framework community to lean on for brain improvements |
| Fixed-SQL tools | Limited generality — adding a new question shape requires a new tool, not just a prompt change |
| Bounded `MAX_STEPS = 5` | Some genuinely complex questions can't be answered in 5 steps; the system summarises rather than continues |
| In-session memory only | No conversational continuity across sessions; each question is reasoned in isolation |
| Hardcoded chain rules in `RuleBrain` | Offline mode chains only what's encoded — it does not generalise |
| One datasource module | If we ever need a non-monkeypatchable seam, this refactor will be visible |
| `query_analytics` exists | The escape hatch always carries some risk of being over-used; the policy (description + system prompt + RuleBrain by construction) is the answer, not a banning the tool |

None of these are regrets. They are the price of the properties on the
other side of the trade.

---

## 11. Realistic future evolutions

Deferred deliberately. Listed here so they aren't forgotten and so the
scope is honest.

- **Evaluation layer over serialised traces.** Compute `success_rate`,
  `fallback_rate`, `avg_steps`, `generated_sql` share, tool-usage
  distribution. Inputs already exist in every trace; the metrics code
  doesn't. Highest-value addition.
- **Latency and cost instrumentation.** Per-tool, per-brain, per-step.
  `duration_ms` already on every event; what's missing is aggregation and
  a Bedrock-cost mapping.
- **`ToolResult.artifact_ref`.** Large dataframes should eventually become
  references (cached IDs or S3 paths) rather than inline frames, to keep
  traces and APIs light. Acceptable inline at current scope.
- **Additional brains.** `OpenAIBrain`, `LocalLLMBrain`. The protocol
  supports it without orchestration changes.
- **Async tool execution.** When a single question genuinely needs to fan
  out across independent tools (e.g. two independent dimensions of the
  same comparison), `asyncio.gather` over the registry would help. Not
  worth doing speculatively.
- **CI pipeline.** A GitHub Actions workflow that runs
  `pytest tests/ai_agent` on every push and validates the demo-trace JSON
  shapes. Completes the maturity story.

What's explicitly **not** on this list: multi-agent orchestration, vector
stores, conversational memory, autonomous web browsing, recursive
self-improvement. Those are the directions that make systems like this
collapse.

---

## 12. How to defend this architecture

The two-sentence version, for a conversation:

> "It's a bounded reasoning runtime over an analytics lakehouse. I
> separated the engine from the brain so the loop, retries, fallback, and
> tracing don't depend on the LLM provider — which means it stays testable
> offline, degrades gracefully when Bedrock is down, and exposes the same
> trace stream to the UI, CLI, and any future evaluation layer."

The one-paragraph version:

> "The interesting design choice was treating the agent as a runtime, not
> a script. A `ReasoningEngine` runs a plan → act → observe loop that's
> capped at 5 steps with a loop guard and one retry per transient error,
> so it can't spiral. Tool errors and brain failures become observations
> or fallback transitions, never exceptions — the loop never crashes.
> Tools are typed and dispatched through a single crash-safe registry,
> and SQL hitting Athena either comes from code-defined templates or
> passes a hardened validator with a table whitelist. Every step emits a
> structured `TraceEvent`, and the whole trace serialises to JSON, which
> is how the dashboard demo replays a real run with zero AWS. There are
> two interchangeable brains behind one protocol — a Bedrock one for live
> reasoning and a deterministic rule planner for offline mode — and the
> engine auto-falls-back between them mid-run if Bedrock errors. The
> 59-test suite runs offline because all DB access funnels through one
> monkeypatchable seam. None of this is novel individually; the value is
> that the runtime semantics, not the LLM, are what makes the system
> reliable."

If asked "why not just use LangChain / LangGraph?":

> "LangGraph's cyclic graphs would work for the agent loop, but the
> properties I wanted — clean mid-run fallback, a streamed trace that's
> also a serialisable replay artifact, a fully offline test path —
> compose more naturally in a small custom loop than they fight an
> abstraction. The custom loop is also where most of the runtime
> guarantees live, so owning it kept the failure model visible."

If asked "what would you change?":

> "An evaluation layer over the serialised traces is the next step — the
> data is there, the aggregation isn't. After that, async tool execution
> for genuinely parallel sub-questions, and a Bedrock-cost mapping over
> the existing per-step latency."

These aren't scripts to memorise — they're the right shape of answer.
Adapt the wording.
