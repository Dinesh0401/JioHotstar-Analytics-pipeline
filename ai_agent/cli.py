"""CLI entry point for the JioHotstar multi-agent analytics system."""

from __future__ import annotations

import argparse
import sys

from ai_agent.orchestrator import MultiAgentOrchestrator
from ai_agent.trace import StepKind


def main():
    parser = argparse.ArgumentParser(
        description="JioHotstar Multi-Agent AI Analytics — ask questions in natural language."
    )
    parser.add_argument("question", nargs="*", help="Natural language analytics question.")
    parser.add_argument("--list-agents", action="store_true", help="List all available agents.")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive chat mode.")
    parser.add_argument("--engine", action="store_true",
                        help="Use the reasoning runtime and print the live trace.")
    args = parser.parse_args()

    orchestrator = MultiAgentOrchestrator()

    if args.list_agents:
        print("Available Agents:")
        print("-" * 50)
        for agent in orchestrator.list_agents():
            print(f"  {agent['icon']} {agent['name']}")
            print(f"     {agent['description']}")
        return

    if args.interactive:
        _interactive_mode(orchestrator)
        return

    if args.engine:
        if not args.question:
            parser.print_help()
            sys.exit(1)
        _ask_with_engine(" ".join(args.question).strip())
        return

    if not args.question:
        parser.print_help()
        sys.exit(1)

    question = " ".join(args.question).strip()
    _ask_and_print(orchestrator, question)


def _ask_and_print(orchestrator, question):
    agent_name, agent_icon = orchestrator.route(question)
    print(f"\n{agent_icon} Routed to: {agent_name} Agent")
    print("=" * 60)

    result = orchestrator.ask(question)
    print(f"Title: {result.title}")
    print(f"Summary: {result.summary}")

    if result.detail:
        print(f"Detail: {result.detail}")

    if result.error:
        print(f"Error: {result.error}", file=sys.stderr)

    if result.sql:
        print(f"\nSQL:\n{result.sql}")

    if result.dataframe is not None and not result.dataframe.empty:
        print(f"\nResults ({len(result.dataframe)} rows):")
        print(result.dataframe.head(15).to_string(index=False))

    if result.suggestions:
        print("\nFollow-up questions:")
        for s in result.suggestions:
            print(f"  → {s}")

    print()


def _interactive_mode(orchestrator):
    print("=" * 60)
    print("  🏏 JioHotstar Multi-Agent AI Analytics")
    print("  Type your question or 'quit' to exit")
    print("=" * 60)

    while True:
        try:
            question = input("\n❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("Goodbye!")
            break
        if question.lower() == "agents":
            for a in orchestrator.list_agents():
                print(f"  {a['icon']} {a['name']} — {a['description']}")
            continue

        _ask_and_print(orchestrator, question)


def _ask_with_engine(question: str) -> None:
    """CLI smoke-test path: drive ReasoningEngine and print each trace event."""
    from ai_agent.langgraph_agent import build_engine

    print(f"\nQuestion: {question}")
    print("=" * 60)

    def show(event) -> None:
        marker = {StepKind.THOUGHT: "[think]", StepKind.TOOL_CALL: "[tool ]",
                  StepKind.OBSERVATION: "[obs  ]", StepKind.FALLBACK: "[fall ]",
                  StepKind.SUMMARY: "[sum  ]", StepKind.FINAL: "[final]",
                  StepKind.ERROR: "[error]"}.get(event.kind, "[step ]")
        suffix = f" ({event.duration_ms}ms)" if event.duration_ms else ""
        print(f"  {marker} step {event.step_index}: {event.title}{suffix}")

    result = build_engine().run(question, on_event=show)
    print("-" * 60)
    print(f"Answer:\n{result.answer}\n")
    print(f"Brain: {result.state.active_brain} | steps: {result.state.step_count}")


if __name__ == "__main__":
    main()
