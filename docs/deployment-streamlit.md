# Deploying the AI Command Center on Streamlit Community Cloud

Offline-first deployment plan. The goal is **proving the runtime
assumptions survive outside localhost**, not maximising capability. A
stable RuleBrain demo with replayed canned data is the target for the
first deploy; live Bedrock can be enabled later.

---

## TL;DR — the settings to enter

When you click **"Deploy a public app from GitHub" → Deploy now**:

| Field | Value |
|---|---|
| Repository | `Dinesh0401/JioHotstar-Analytics-pipeline` |
| Branch | `feature/ai-agent-reasoning-runtime` (until merged to `main`) |
| Main file path | `ai_agent/streamlit_app.py` |
| App URL | your choice — e.g. `jiohotstar-ai-runtime` |
| Python version | `3.11` (pinned by `.python-version` and `runtime.txt`) |
| Advanced → Secrets | paste the **two-line** block from "Secrets" below |

Leave everything else as default. Streamlit Cloud will read
`requirements.txt` and `.streamlit/config.toml` automatically.

---

## Secrets — paste this into Streamlit Cloud → App settings → Secrets

For the **offline-first first deploy**, paste exactly this and nothing
else:

```toml
RUN_MODE = "offline"
```

That single env var:

- skips Bedrock construction entirely (no AWS calls attempted, ever),
- triggers `apply_canned_datasource()` inside `build_engine()` so all
  tool queries return deterministic in-memory rows,
- flips the hero badge in the Command Center to "🟡 Offline
  deterministic mode" with an explicit subtitle.

A reference template lives at `.streamlit/secrets.toml.example`.

---

## Why this approach

Most "AI agent demo" deployments are fragile because they require a
live LLM connection to do anything. When credentials expire or the
provider rate-limits, the demo simply breaks. This repo is designed
around the opposite default: the system should work *first*, then
optionally improve when an LLM is reachable.

The runtime architecture supports this by construction:

- `ReasoningEngine` is brain-agnostic. The `RuleBrain` (deterministic,
  no network) is a first-class brain, not a placeholder.
- The trace UI consumes events from a callback, identical for both
  brains — there is nothing to fake.
- `ai_agent/demo_data.py` provides a canned datasource keyed to every
  multi-step sample question shown in the Command Center, so the trace
  is genuine: engine, brain, registry, tools, and the SQL the tools
  emit all run for real — only the rows are canned.

This is what the deploy is verifying: that "AWS-free" is not a story,
it's a code path the test suite exercises and the deploy demonstrates.

---

## Pre-deploy safety checks (already done)

Every check from the user's deployment checklist is already satisfied
in the repo on this branch:

| Check | Status |
|---|---|
| No import-time AWS initialization (no `boto3.client(...)` at module scope) | ✅ `_get_athena_client()` is lazy; `_maybe_bedrock_brain()` early-returns when `RUN_MODE=offline` |
| App boots with zero AWS creds | ✅ `RUN_MODE=offline` short-circuits both Bedrock and live datasources |
| RuleBrain auto-activates | ✅ `build_engine()` picks `bedrock_brain or rule_brain`; `_maybe_bedrock_brain()` returns `None` in offline mode |
| Demo traces replay correctly | ✅ `dashboard/app.py` reads `ai_agent/demo_traces/compare_churn_by_plan.json` deterministically |
| `requirements.txt` complete | ✅ streamlit / pandas / plotly / tabulate / boto3 / sqlalchemy / psycopg2-binary all present |
| Replay traces committed | ✅ `ai_agent/demo_traces/*.json` are tracked |
| Relative paths only | ✅ `Path(__file__).parent.parent / "ai_agent" / "demo_traces"` in the dashboard; nothing hardcoded |
| Streamlit secrets template | ✅ `.streamlit/secrets.toml.example` (real secrets gitignored) |
| Python version pinned | ✅ `.python-version` and `runtime.txt` both say 3.11 |
| Dark theme matches the app | ✅ `.streamlit/config.toml` |

---

## Acceptance criteria — what to verify after deploy

Run these checks against the public URL. The deploy is only "done"
when **all 8** pass.

1. **App loads publicly.** Cold-boot under ~60s. No 5xx, no stack
   traces on first load. Page title reads "JioHotstar AI Command
   Center".
2. **"🟡 Offline deterministic mode" badge is visible** in the hero
   strip, with the subtitle explaining no LLM / no AWS / real engine.
3. **Multi-step demo query works.** Click "Compare churn risk across
   subscription plans". The reasoning-trace column should show:
   - Step 1: `THOUGHT` "Chain step 1/2: subscriptions."
   - Step 1: `TOOL_CALL` `subscriptions`
   - Step 1: `OBSERVATION` 3 rows (PREMIUM / VIP / BASIC subscription metrics)
   - Step 2: `THOUGHT` "Chain step 2/2: churn_risk."
   - Step 2: `TOOL_CALL` `churn_risk({'by': 'plan'})`
   - Step 2: `OBSERVATION` 3 rows (per-plan churn aggregations)
   - Step 3: `FINAL`
4. **Trace timeline streams correctly.** Cards appear in the right
   column *as the loop runs* (not all at once at the end). Latency
   numbers visible between cards.
5. **Replay-only path also works.** Open the main dashboard
   (separate deploy or local) and confirm the AI Agent section
   renders the replayed trace from `compare_churn_by_plan.json`.
6. **No crashes when AWS is absent.** Try all four sample buttons.
   Every one of them should finish with a `FINAL` event, not an
   `ERROR`. The observations should contain real data (canned, but
   non-empty).
7. **Cold boot acceptable.** Refresh after the app idles. Re-cold-boot
   should be under ~30s (the engine is cached via
   `@st.cache_resource`).
8. **Mobile layout not broken.** Open the URL on a phone. The two
   columns should stack vertically; the hero strip and badge should
   still be visible.

If all 8 pass: you have a publicly deployable AI runtime, not just a
GitHub repo.

---

## Iterating on the deploy

Streamlit Cloud watches the configured branch and redeploys on every
push. Workflow once live:

- Push to `feature/ai-agent-reasoning-runtime` → auto-redeploy (fast).
- After PR #2 merges to `main`, switch the deploy's branch in app
  settings from `feature/ai-agent-reasoning-runtime` to `main`.

For a redeploy in place: **App settings → Reboot app**.
**App settings → Manage app → View logs** shows boot + runtime logs.

---

## Enabling live Bedrock later (optional)

Once the offline deploy is stable and you've recorded the demo, you
can flip live LLM mode on:

1. App settings → Secrets, **replace** the secrets block with:

   ```toml
   # RUN_MODE removed (or empty) — allows Bedrock when configured
   BEDROCK_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"
   AWS_ACCESS_KEY_ID = "AKIA..."
   AWS_SECRET_ACCESS_KEY = "..."
   AWS_REGION = "ap-south-1"
   ```

2. Reboot the app.

3. The hero badge should now read "🟢 Bedrock reasoning". The runtime
   will still fall back to `RuleBrain` on any Bedrock error, with a
   visible `FALLBACK` event in the trace. **This degradation is the
   feature.**

Note that this still uses canned datasources unless you ALSO unset
`RUN_MODE` and provide Athena credentials. The recommended progression
for showing maturity is: offline-only → Bedrock + canned data →
(optionally, never publicly) Bedrock + live Athena.

---

## When things break

| Symptom | Likely cause | Fix |
|---|---|---|
| Cold-boot times out / install fails | Heavy unused deps in `requirements.txt` (pyspark, delta-spark) | Trim them or move to a `requirements-deploy.txt` and point Streamlit Cloud at it |
| Hero badge says "Local rule planner" instead of "Offline deterministic mode" | `RUN_MODE` secret not set or not lowercased | Set `RUN_MODE = "offline"` and reboot |
| Trace shows `ERROR` observations | A tool's SQL doesn't substring-match any key in `_CANNED_ATHENA` | Add the matching key + canned rows in `ai_agent/demo_data.py` and push |
| Dashboard AI section reads "Run `python -m ai_agent.build_demo_traces`" | The committed demo trace JSON is missing on this branch | `python -m ai_agent.build_demo_traces` locally, commit `ai_agent/demo_traces/*.json`, push |
| Mobile layout broken | Streamlit Cloud sometimes serves stale CSS | Hard refresh; reboot the app |

---

## What this deploy is NOT

To stay honest with the architecture rationale:

- It is not a live AI agent over a real lakehouse. The data is canned.
- It is not testing Bedrock cost or scaling. Bedrock is off.
- It is not exercising the SQL guardrail against generated SQL.
  `query_analytics` is reachable but the RuleBrain prefers fixed tools.
- It is not a benchmark of model quality. There is no model.

It is testing exactly one thing: **the runtime architecture survives
outside localhost**. That property is what makes the rest of the work
credible.
