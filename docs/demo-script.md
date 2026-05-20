# Demo Recording Script

A 3–5 minute engineer walkthrough of the AI reasoning runtime. Designed to
record in **one take** with minimal editing. Read this once start-to-finish
before you hit record so the cues feel natural; don't read it on camera.

The goal is **engineer demo**, not startup promo. A clean real walkthrough
with live traces and one small stumble is more credible than a hyper-produced
reel.

---

## Before you hit record

### What to have open (4 things)

1. **Browser tab 1** — your deployed Streamlit Cloud app (the AI Command
   Center). Pre-load it so cold-boot is done. Confirm the hero badge reads
   **"🟡 Offline deterministic mode"**.
2. **Browser tab 2** — the GitHub repo on the README so the architecture
   diagram is rendered. Scroll to the Mermaid diagram. Keep zoom at 100%.
3. **Browser tab 3** *(optional)* — the dashboard, either deployed or local
   (`streamlit run dashboard/app.py`), scrolled to the AI Agent showcase
   section.
4. **Local terminal** *(optional, only if you choose to show CLI)* — sitting
   at the repo root with `RUN_MODE=offline` exported.

### Window state

- 1080p, 1920×1080. Single monitor, no notifications.
- Browser zoom 100% — the trace cards need to read clearly on playback.
- Close Slack, mail, anything that can pop a notification.
- Quit anything CPU-heavy so the trace streams smoothly.

### Audio

- Test mic levels with a one-sentence recording first.
- Pop filter / be ~15cm from the mic.
- Quiet room. Recordings forgive small audio noise but not loud HVAC.

### Mindset

You are not selling. You are showing an engineer what you built and why.
Talk like you're explaining it to a senior engineer over Zoom.

---

## The script (3–5 min, 5 segments)

Time markers are targets, not constraints. If you go a little long, fine.

### 0:00 – 0:30 — Frame it correctly

**Action:** Tab 1 (the deployed Command Center). Hero strip on screen.

**Say (verbatim is fine):**

> "This is a bounded reasoning runtime for analytics workflows. It's not a
> chatbot or an autonomous agent — it's a small orchestration system that
> takes a natural-language question, plans which analytics tools to call,
> executes them, observes what comes back, and produces an answer. Every
> step is bounded, observable, and crash-safe. What you see in the right
> column is the system's reasoning made visible."

**Cue:** point the cursor at the **"🟡 Offline deterministic mode"** badge.
Say:

> "It's running offline right now — no Bedrock, no AWS — which I'll come
> back to."

---

### 0:30 – 1:30 — Architecture, quickly

**Action:** Switch to Tab 2 (the rendered Mermaid diagram in the README).

**Say:**

> "The architecture has four pieces. A *tool registry* — about eleven typed
> tools over Athena and a live Postgres feed. A *reasoning engine* — the
> plan-act-observe loop with a step cap, a loop guard, and one retry per
> transient error. A *trace* — every loop step is a structured event. And
> on top, a *brain* — the decision-maker that picks the next action."

**Cue:** point at the "Brain ↔ Engine" arrows.

**Say (this is the key architectural sentence — get it right):**

> "The most important decision is that brains and the engine are decoupled.
> There are two interchangeable brains behind one protocol — a Bedrock
> brain for live LLM tool-use, and a deterministic rule planner that runs
> with no network. The engine doesn't know or care which one is active.
> That's what makes the system testable offline and degradeable
> gracefully — not by retrying harder, but by switching decision-makers."

---

### 1:30 – 3:30 — Live multi-step query

**Action:** Switch back to Tab 1 (the Command Center).

**Say:**

> "Here's a question that needs more than one tool call to answer."

**Cue:** click the sample button **"Compare churn risk across subscription
plans"**.

**While the trace streams in, narrate:**

> "Step 1 — the brain reasons that this needs subscription metrics first.
> It emits a thought, picks the `subscriptions` tool, calls it, and the
> observation comes back: three rows, one per plan."

**Pause briefly when the OBSERVATION card appears.**

> "Step 2 — same loop, different decision. Now it picks `churn_risk` with
> `by='plan'` and the second observation is the per-plan churn aggregation.
> Notice the SQL is visible if I expand the card."

**Action:** click the "SQL" expander on the second OBSERVATION.

> "The brain never wrote that SQL. These tools template their SQL in
> Python and only accept typed parameters — the brain fills the
> parameters. There's a separate escape-hatch tool that does generate SQL,
> and that one goes through a validator before it ever reaches Athena."

**Action:** scroll/point to the FINAL card.

> "Step 3 — final answer. The whole trace — thought, tool call, SQL,
> observation, latency, final — is a serializable artifact. It's not
> debug logging. The same event stream feeds this UI, the CLI, and the
> dashboard's replay demo."

**Optional, if you want to show the dashboard replay:** switch to Tab 3
briefly and point at the AI Agent section.

> "This dashboard section replays a real serialized trace, not a
> screenshot. The system can demo deterministically with zero network."

---

### 3:30 – 4:15 — Offline determinism is the point

**Action:** stay on Tab 1. Point at the badge again.

**Say:**

> "I deployed this with `RUN_MODE=offline`. No AWS credentials, no
> Bedrock, no Athena connection. The engine is real, the brain is real,
> the tools are real, the trace is real — only the data is canned. Every
> sample question you saw works exactly the same with the LLM off."

**Cue:** scroll back up and quickly click another sample — **"Is the data
pipeline healthy?"** — let it run.

> "Same loop, different tool. Six per-table count queries, one
> per-table observation, one final."

**Say (important framing):**

> "This matters because most agent demos require a live LLM to work. A
> credential expires, a region throttles, the demo dies. This one works
> with zero LLM and zero AWS. The fallback isn't a story — it's the
> default."

---

### 4:15 – 5:00 — Close with engineering framing

**Action:** can stay on Tab 1, or scroll the README's "Why this
architecture?" section briefly.

**Say (closing — get this right; it's the credibility line):**

> "Six engineering decisions matter here. The engine is brain-agnostic so
> it stays testable offline. The loop never crashes — tool errors become
> observations the brain can react to. Reasoning is bounded — five steps,
> a loop guard, one retry, no entropy spiral. Tools are typed and
> dispatched through a single crash-safe registry. Generated SQL passes
> a paranoid validator before reaching Athena. And the trace is a
> first-class output, not a log line."

**Final sentence (verbatim — this is the line that signals judgment):**

> "This is not an autonomous AGI system. It's a constrained orchestration
> runtime designed for observability, resilience, and predictable
> analytics workflows."

**Then stop recording.** Don't add a sign-off. Don't say "thanks for
watching." The ending is the line above.

---

## Recovery moves (if something goes wrong live)

| What | Do this |
|---|---|
| Trace doesn't stream / app frozen | Hard refresh (Ctrl+F5). The engine is cached so re-cold-boot is fast. Keep narrating — say "let me reload, this is Streamlit on a free tier." |
| Wrong sample button clicked | Don't apologize; just say "actually let me show the multi-step one" and click the right one. |
| You stumble on a sentence | Pause one beat, then restart the sentence. Don't say "sorry." Editors call this a "natural retake" and it reads as confidence. |
| The mode badge says something other than "Offline deterministic mode" | Stop, check Streamlit Cloud secrets has `RUN_MODE = "offline"`, reboot the app, re-record. Don't wing it. |
| Tool observation comes back empty | A canned-data key didn't match the SQL. Re-record after fixing `ai_agent/demo_data.py` — don't try to talk around an empty observation. |

---

## What NOT to do

- Don't say "AI agent", "autonomous", "AGI", or "intelligent system" except
  in the closing line where you *deny* them.
- Don't use the word "amazing", "powerful", or "smart" anywhere.
- Don't read the architecture rationale doc on camera. Reference it ("the
  reasoning is written up in detail in the repo") at most.
- Don't show secrets, API keys, or `.env` files on screen.
- Don't run the live AWS-backed mode in this video. Save that for later if
  you want a "live LLM mode" follow-up demo. This first video is the
  stable offline demo.
- Don't add background music. Engineers find it distracting.
- Don't over-edit. Cuts should remove dead air, not stitch together
  artificial energy.

---

## After recording

### Quick QC (do once before publishing)

- Watch on 2× to spot dead air or filler ("um", "so", "basically").
- Confirm audio is consistent (no clipping, no abrupt volume changes).
- Confirm the trace card text is actually readable at the resolution
  you'll publish at.
- Length check — target 3:30 to 5:00. Under 3 minutes feels rushed; over
  6 loses engineer attention.

### Where it goes

- **LinkedIn post** (the demo as the main asset, with a 2-sentence
  caption: what it is, link to repo)
- **README** — add a "Demo" section near the top linking to the video.
- **Resume** — link in the project bullet for the runtime.
- **Job applications** — paste link in the cover-letter "selected work"
  section.

### Caption template (LinkedIn / repo)

> A bounded reasoning runtime for analytics workflows. Brain-agnostic
> plan→act→observe loop with deterministic offline fallback, typed tool
> registry, SQL guardrails, and a streamed reasoning trace.
> Repo: github.com/Dinesh0401/JioHotstar-Analytics-pipeline

That's it. Hit record.
