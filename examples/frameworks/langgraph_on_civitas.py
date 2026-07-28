"""LangGraph workflow running on Agency — under 10 lines of adapter code.

Requires: pip install civitas civitas-contrib[langgraph]
(the adapter itself lives in civitas-contrib, not core civitas — see docs/adapters.md)

This example wraps a LangGraph compiled graph as an Agency AgentProcess.
The graph gains supervision, OTEL tracing, and transport-agnostic messaging.
"""

import asyncio

from civitas_contrib.adapters.langgraph import LangGraphAgent
from langgraph.graph import END, StateGraph

from civitas import Runtime, Supervisor

# --- Define a LangGraph workflow (this is pure LangGraph code) ---


def research(state: dict) -> dict:
    return {**state, "findings": f"Research on: {state['query']}"}


def summarize(state: dict) -> dict:
    return {**state, "summary": f"Summary of {state['findings']}"}


graph = StateGraph(dict)
graph.add_node("research", research)
graph.add_node("summarize", summarize)
graph.set_entry_point("research")
graph.add_edge("research", "summarize")
graph.add_edge("summarize", END)
compiled = graph.compile()

# --- Run it on Agency (the adapter is one line) ---


async def main():
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[
                LangGraphAgent("researcher", graph=compiled),  # <-- that's it
            ],
        )
    )
    await runtime.start()
    result = await runtime.ask("researcher", {"query": "quantum computing"})
    print(result.payload)
    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
