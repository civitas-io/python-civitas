"""OpenAI Agents SDK agent running on Agency — under 10 lines of adapter code.

Requires: pip install civitas civitas-contrib[openai]
(the adapter itself lives in civitas-contrib, not core civitas — see docs/adapters.md)

This example wraps an OpenAI Agents SDK agent as an Agency AgentProcess.
The agent gains supervision, OTEL tracing, and transport-agnostic messaging.
"""

import asyncio

from agents import Agent
from civitas_contrib.adapters.openai import OpenAIAgent

from civitas import Runtime, Supervisor

# --- Define an OpenAI agent (this is pure OpenAI SDK code) ---

assistant = Agent(
    name="assistant",
    instructions="You are a helpful research assistant. Be concise.",
)

# --- Run it on Agency (the adapter is one line) ---


async def main():
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[
                OpenAIAgent("assistant", agent=assistant),  # <-- that's it
            ],
        )
    )
    await runtime.start()
    result = await runtime.ask("assistant", {"input": "What is RLHF?"})
    print(result.payload)
    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
