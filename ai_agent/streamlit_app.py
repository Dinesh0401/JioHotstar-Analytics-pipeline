"""JioHotstar Analyst — friendly user-facing UI over the reasoning runtime.

Left column: a conversational chat with a welcome card, sample-question
chips, avatars, and a loading spinner. Right column: the agent's reasoning
shown live with friendly labels — so the user can SEE how the answer was
found. A sidebar 'Engineer view' toggle restores raw step kinds, args, SQL,
and ms timings for technical viewers.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

from ai_agent.langgraph_agent import build_engine, is_offline_mode
from ai_agent.trace import StepKind

# Offline mode (RUN_MODE=offline) handling lives inside build_engine() — see
# ai_agent/langgraph_agent.py. The UI just reflects which brain is active.

st.set_page_config(
    page_title="JioHotstar Analyst",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Styles ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0e1117; }

    /* Hero */
    .hero {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155; border-radius: 16px;
        padding: 22px 26px; margin-bottom: 16px;
    }
    .hero h1 { margin: 0; font-size: 1.8rem; color: #f1f5f9; }
    .hero .tagline { margin: 6px 0 0 0; color: #cbd5e1; font-size: 1rem; }
    .hero .chips { margin-top: 12px; }
    .chip { display: inline-block; padding: 4px 11px; border-radius: 999px;
            font-size: 0.74rem; font-weight: 600; margin-right: 6px; }
    .chip-live    { background: #14321f; color: #4ade80; }
    .chip-offline { background: #3a2e10; color: #fbbf24; }
    .chip-meta    { background: #1e293b; color: #94a3b8;
                    border: 1px solid #334155; }

    /* Welcome card (shown when chat is empty) */
    .welcome {
        background: linear-gradient(135deg, #112030 0%, #0f172a 100%);
        border: 1px dashed #334155; border-radius: 12px;
        padding: 14px 18px; margin: 4px 0 12px 0;
    }
    .welcome h3 { margin: 0 0 4px 0; color: #e2e8f0; font-size: 1.02rem; }
    .welcome p  { margin: 0; color: #94a3b8; font-size: 0.88rem; }

    /* Sample question buttons → inviting cards */
    .stButton > button {
        width: 100%; height: auto; min-height: 64px;
        background: linear-gradient(135deg, #1e293b 0%, #1a2538 100%) !important;
        border: 1px solid #334155 !important; border-radius: 12px !important;
        color: #e2e8f0 !important; text-align: left !important;
        padding: 12px 14px !important; font-size: 0.92rem !important;
        line-height: 1.35 !important; white-space: normal !important;
        transition: border-color 0.15s ease, background 0.15s ease !important;
    }
    .stButton > button:hover {
        border-color: #38bdf8 !important;
        background: linear-gradient(135deg, #1e293b 0%, #1f2f48 100%) !important;
    }

    /* Trace heading + cards */
    h2.trace-heading { color: #e2e8f0; font-size: 1.1rem;
                       margin: 4px 0 10px 0; }
    .step-card {
        border-left: 3px solid #475569; padding: 10px 14px; margin: 6px 0;
        background: #141d2b; border-radius: 0 10px 10px 0;
    }
    .step-think    { border-left-color: #818cf8; }
    .step-tool     { border-left-color: #38bdf8; }
    .step-obs      { border-left-color: #2dd4bf; }
    .step-fallback { border-left-color: #fbbf24; }
    .step-final    { border-left-color: #4ade80; }
    .step-error    { border-left-color: #f87171; }
    .step-meta     { color: #64748b; font-size: 0.72rem; }
    .step-title    { color: #e2e8f0; font-size: 0.92rem; margin-top: 2px; }
    .latency       { color: #475569; font-size: 0.7rem;
                     margin: 4px 0 0 18px; }

    /* Subtle chat tweaks */
    [data-testid="stChatMessage"] {
        background: #141d2b; border: 1px solid #1e293b;
        border-radius: 12px; padding: 8px 14px; margin: 6px 0;
    }
</style>
""", unsafe_allow_html=True)


# ── Engine + active-mode chip ──────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return build_engine()


engine = get_engine()
_BEDROCK = engine.bedrock_brain is not None
_OFFLINE = is_offline_mode()

if _BEDROCK:
    _CHIP = '<span class="chip chip-live">🟢 Live LLM</span>'
elif _OFFLINE:
    _CHIP = '<span class="chip chip-offline">🟡 Offline demo</span>'
else:
    _CHIP = '<span class="chip chip-offline">🟡 Rule planner</span>'

_TOOL_COUNT = len(engine.registry.all())


# ── Sidebar (Engineer view + about) ────────────────────────────────────────
with st.sidebar:
    st.markdown("### View")
    engineer_view = st.toggle(
        "Engineer view",
        value=False,
        help=("Show raw step kinds (THOUGHT / TOOL_CALL / OBSERVATION), "
              "tool args, SQL, and per-step ms timings."),
    )
    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        "A bounded reasoning runtime that answers analytics questions by "
        "chaining typed tools over the JioHotstar lakehouse. "
        "[Architecture rationale]"
        "(https://github.com/Dinesh0401/JioHotstar-Analytics-pipeline/"
        "blob/main/docs/architecture-rationale.md)"
    )


# ── Hero ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="hero">
  <h1>🏏 JioHotstar Analyst</h1>
  <p class="tagline">Ask me about audiences, content, churn, and what's trending.</p>
  <p class="chips">{_CHIP}
     <span class="chip chip-meta">{_TOOL_COUNT} tools available</span></p>
</div>
""", unsafe_allow_html=True)


# ── Sample questions: friendly framing → real prompt ──────────────────────
_SAMPLES = [
    ("🏆 What's trending right now?",
     "What are the top trending shows?"),
    ("📉 Which plans are losing the most users?",
     "Compare churn risk across subscription plans"),
    ("🎯 Who are our highest-risk users?",
     "Which users are most likely to churn?"),
    ("💚 Is everything healthy?",
     "Is the data pipeline healthy?"),
]


# ── Step labels: friendly default, technical when Engineer view is on ─────
_FRIENDLY = {
    StepKind.THOUGHT:     ("step-think",    "💭 Thinking"),
    StepKind.TOOL_CALL:   ("step-tool",     "🛠️ Looking up"),
    StepKind.OBSERVATION: ("step-obs",      "📊 Found"),
    StepKind.FALLBACK:    ("step-fallback", "⚠️ Switching mode"),
    StepKind.SUMMARY:     ("step-obs",      "✏️ Summarising"),
    StepKind.FINAL:       ("step-final",    "✅ Answer ready"),
    StepKind.ERROR:       ("step-error",    "❌ Hit an issue"),
}
_TECHNICAL = {
    StepKind.THOUGHT:     ("step-think",    "🧠 Thought"),
    StepKind.TOOL_CALL:   ("step-tool",     "🔧 Tool call"),
    StepKind.OBSERVATION: ("step-obs",      "📊 Observation"),
    StepKind.FALLBACK:    ("step-fallback", "⚠️ Fallback"),
    StepKind.SUMMARY:     ("step-obs",      "🧷 Summary"),
    StepKind.FINAL:       ("step-final",    "✅ Final"),
    StepKind.ERROR:       ("step-error",    "❌ Error"),
}


def render_event(container, event, total_steps: int, eng_view: bool) -> None:
    """Render one trace event as a timeline card."""
    style_map = _TECHNICAL if eng_view else _FRIENDLY
    css, label = style_map.get(event.kind, ("step-card", "Step"))

    # Friendly titles strip jargon; engineer view keeps args/SQL on screen.
    if event.kind == StepKind.TOOL_CALL:
        if eng_view and event.tool_args:
            title = f"{event.tool_name}({event.tool_args})"
        elif event.tool_args:
            args = ", ".join(f"{k}={v}" for k, v in event.tool_args.items())
            title = f"{event.tool_name} — {args}"
        else:
            title = event.tool_name or event.title
    elif event.kind == StepKind.OBSERVATION and not eng_view:
        first = next((ln for ln in (event.detail or "").splitlines() if ln.strip()), "")
        title = first or "got a result"
    else:
        title = event.title

    meta = (f"Step {event.step_index}/{total_steps} · {label}"
            if eng_view else f"Step {event.step_index} · {label}")

    container.markdown(
        f'<div class="step-card {css}">'
        f'<span class="step-meta">{meta}</span>'
        f'<div class="step-title">{title}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if event.kind == StepKind.OBSERVATION and event.sql and eng_view:
        with container.expander("SQL", expanded=False):
            st.code(event.sql, language="sql")
    if event.duration_ms and eng_view:
        container.markdown(
            f'<div class="latency">↓ {event.duration_ms} ms</div>',
            unsafe_allow_html=True,
        )


# ── Session state ──────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []


# ── Layout ────────────────────────────────────────────────────────────────
left, right = st.columns([5, 4])

with left:
    if not st.session_state.messages:
        st.markdown(
            '<div class="welcome">'
            "<h3>👋 Hi! I can dig into JioHotstar's data for you.</h3>"
            "<p>Pick one of the suggestions below, or type your own question.</p>"
            '</div>',
            unsafe_allow_html=True,
        )
    for msg in st.session_state.messages:
        avatar = "🙋" if msg["role"] == "user" else "🏏"
        with st.chat_message(msg["role"], avatar=avatar):
            st.markdown(msg["content"])

    st.caption("Try one of these:")
    cols = st.columns(2)
    for i, (label, prompt) in enumerate(_SAMPLES):
        if cols[i % 2].button(label, key=f"sample_{i}", use_container_width=True):
            st.session_state["prefill"] = prompt

question = st.chat_input("Ask anything — content, churn, plans, streaming, pipeline…")
if not question:
    question = st.session_state.pop("prefill", None)

with right:
    st.markdown('<h2 class="trace-heading">🔍 How I figured this out</h2>',
                unsafe_allow_html=True)
    trace_box = st.container()
    if not st.session_state.messages:
        trace_box.caption("Each step appears here while I work — thinking, "
                          "looking up data, then the final answer.")

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with left:
        with st.chat_message("user", avatar="🙋"):
            st.markdown(question)

    # MAX_STEPS=5; fixed total keeps the step-counter label stable.
    total = 5
    events: list = []

    def on_event(event):
        events.append(event)
        render_event(trace_box, event, total, engineer_view)

    with left:
        with st.chat_message("assistant", avatar="🏏"):
            with st.spinner("Looking into it…"):
                result = engine.run(question, on_event=on_event)
            st.markdown(f"**Here's what I found:**\n\n{result.answer}")

    st.session_state.messages.append({
        "role": "assistant",
        "content": f"**Here's what I found:**\n\n{result.answer}",
    })
