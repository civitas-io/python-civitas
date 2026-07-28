"""Custom ModelProvider plugin (docs/plugins.md) — v0.9.2.

Every plugin extension point in Civitas (``ModelProvider``, ``StateStore``,
exporters) is a ``typing.Protocol`` — structural typing, no base class to inherit.
Implement the methods, pass an instance in, done. This example writes a small
CUSTOM ``ModelProvider`` from scratch (not `AnthropicProvider`/a mock buried inside
another example) to make that extension point explicit and copy-pasteable.

Usage:
    python examples/custom_plugin.py
"""

from __future__ import annotations

import asyncio
from typing import Any

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message
from civitas.plugins.model import ModelResponse


class UppercaseEchoProvider:
    """A toy ModelProvider: no LLM, no network, no API key — it just echoes the
    last user message in upper case. Implements ModelProvider's Protocol
    structurally (one method, matching signature) — no inheritance required.
    """

    def __init__(self) -> None:
        self.call_count = 0

    async def chat(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[Any] | None = None,
    ) -> ModelResponse:
        self.call_count += 1
        last_user_message = next(
            (m["content"] for m in reversed(messages) if m.get("role") == "user"), ""
        )
        content = last_user_message.upper()
        return ModelResponse(
            content=content,
            model=model or "uppercase-echo-v1",
            tokens_in=len(last_user_message.split()),
            tokens_out=len(content.split()),
            cost_usd=0.0,  # a real provider would compute this from its own pricing
        )


class ChatAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        response = await self.llm.chat(
            model=None,
            messages=[{"role": "user", "content": message.payload["text"]}],
        )
        return self.reply({"answer": response.content, "tokens": response.tokens_in})


async def main() -> None:
    provider = UppercaseEchoProvider()
    runtime = Runtime(
        supervisor=Supervisor("root", children=[ChatAgent("chat")]),
        model_provider=provider,
    )
    await runtime.start()

    result = await runtime.ask("chat", {"text": "hello from a custom plugin"})
    print(f"Answer: {result.payload['answer']}")
    print(f"Tokens: {result.payload['tokens']}")
    print(f"Provider was called {provider.call_count} time(s)")

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
