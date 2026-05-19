"""JioHotstar AI Command Center — the reasoning-runtime showcase.

Three zones: a hero strip with the live brain mode badge, a conversation column,
and a streamed vertical reasoning trace.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

from ai_agent.langgraph_agent import build_engine
from ai_agent.trace import StepKind

st.set_page_config(page_title="JioHotstar AI Command Center", page_icon="🛰️",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .hero { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border: 1px solid #334155; border-radius: 14px; padding: 18px 24px;
            margin-bottom: 14px; }
    .hero h1 { margin: 0; font-size: 1.6rem; color: #f1f5f9; }
    .hero p { margin: 4px 0 0 0; color: #94a3b8; font-size: 0.9rem; }
    .badge { display: inline-block; padding: 4px 12px; border-radius: 14px;
             font-size: 0.8rem; font-weight: 700; }
    .badge-bedrock { background: #14321f; color: #4ade80; }
    .badge-rule { background: #3a2e10; color: #fbbf24; }
    .step-card { border-left: 3px solid #475569; padding: 8px 14px; margin: 2px 0;
                 background: #1e293b; border-radius: 0 8px 8px 0; }
    .step-think { border-left-color: #818cf8; }
    .step-tool { border-left-color: #38bdf8; }
    .step-obs { border-left-color: #2dd4bf; }
    .step-fallback { border-left-color: #fbbf24; }
    .step-final { border-left-color: #4ade80; }
    .step-error { border-left-color: #f87171; }
    .step-meta { color: #64748b; font-size: 0.72rem; }
    .latency { color: #475569; font-size: 0.7rem; margin-left: 18px; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_engine():
    return build_engine()


engine = get_engine()
_BRAIN_IS_BEDROCK = engine.bedrock_brain is not None
_MODE = ("🟢 Bedrock reasoning", "badge-bedrock") if _BRAIN_IS_BEDROCK \
    else ("🟡 Local rule planner", "badge-rule")
_TOOL_COUNT = len(engine.registry.all())

st.markdown(f"""
<div class="hero">
  <h1>🛰️ Autonomous Analytics Runtime</h1>
  <p>A bounded plan → act → observe agent over the JioHotstar lakehouse.
     <span class="badge {_MODE[1]}">{_MODE[0]}</span>
     &nbsp;<span class="step-meta">{_TOOL_COUNT} tools registered</span></p>
</div>
""", unsafe_allow_html=True)

_SAMPLES = [
    "Compare churn risk across subscription plans",
    "What are the top trending shows?",
    "Which users are most likely to churn?",
    "Is the data pipeline healthy?",
]

_STEP_STYLE = {
    StepKind.THOUGHT: ("step-think", "🧠 Thought"),
    StepKind.TOOL_CALL: ("step-tool", "🔧 Tool call"),
    StepKind.OBSERVATION: ("step-obs", "📊 Observation"),
    StepKind.FALLBACK: ("step-fallback", "⚠️ Fallback"),
    StepKind.SUMMARY: ("step-obs", "🧷 Summary"),
    StepKind.FINAL: ("step-final", "✅ Final"),
    StepKind.ERROR: ("step-error", "❌ Error"),
}


def render_event(container, event, total_steps: int) -> None:
    """Render one trace event as a timeline card into the given container."""
    css, label = _STEP_STYLE.get(event.kind, ("step-card", "Step"))
    title = event.title
    if event.kind == StepKind.TOOL_CALL and event.tool_args:
        title = f"{event.tool_name}({event.tool_args})"
    container.markdown(
        f'<div class="step-card {css}">'
        f'<span class="step-meta">Step {event.step_index}/{total_steps} · {label}</span><br>'
        f'{title}</div>',
        unsafe_allow_html=True,
    )
    if event.kind == StepKind.OBSERVATION and event.sql:
        with container.expander("SQL", expanded=False):
            st.code(event.sql, language="sql")
    if event.duration_ms:
        container.markdown(f'<div class="latency">↓ {event.duration_ms} ms</div>',
                           unsafe_allow_html=True)


if "messages" not in st.session_state:
    st.session_state.messages = []

left, right = st.columns([5, 4])

with left:
    st.subheader("Conversation")
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    st.caption("Try a multi-step question:")
    cols = st.columns(2)
    for i, sample in enumerate(_SAMPLES):
        if cols[i % 2].button(sample, key=f"s{i}", use_container_width=True):
            st.session_state["prefill"] = sample

question = st.chat_input("Ask about content, churn, plans, streaming, pipeline...")
if not question:
    question = st.session_state.pop("prefill", None)

with right:
    st.subheader("Reasoning Trace")
    trace_box = st.container()

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with left:
        with st.chat_message("user"):
            st.markdown(question)

    # MAX_STEPS=5 -> at most ~3 events per step; a fixed total keeps the label stable.
    total = 5
    events: list = []

    def on_event(event):
        events.append(event)
        render_event(trace_box, event, total)

    result = engine.run(question, on_event=on_event)

    with left:
        with st.chat_message("assistant"):
            st.markdown(result.answer)
    st.session_state.messages.append({"role": "assistant", "content": result.answer})
