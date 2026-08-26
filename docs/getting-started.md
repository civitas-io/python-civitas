# Getting Started

This guide takes you from a blank environment to a running, supervised, observable agent system. Each step builds on the last — you can stop at any point once you have what you need.

**Time to complete: under 10 minutes.**

---

## Prerequisites

- Python 3.12 or later
- A terminal

That's it for the first three steps. An API key and Docker are optional extras introduced later.

---

## Step 1 — Install

```bash
pip install civitas
```

To verify:

```bash
python -c "import civitas; print('ok')"
```

---

## Step 2 — Hello Agent

Create `hello.py`:

```python
import asyncio
from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message

class Greeter(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        name = message.payload.get("name", "world")
        return self.reply({"greeting": f"Hello, {name}!"})

async def main():
    runtime = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter")])
    )
    await runtime.start()

    result = await runtime.ask("greeter", {"name": "Civitas"})
    print(result.payload["greeting"])   # Hello, Civitas!

    await runtime.stop()

asyncio.run(main())
```

Run it:

```bash
python hello.py
# Hello, Civitas!
```

What happened:

- `Runtime` wired up a `MessageBus`, `Registry`, and `InProcessTransport`
- `Supervisor` started `Greeter` as a supervised process
- `runtime.ask()` sent a message and waited for a reply
- `self.reply(...)` returned a message from inside `handle()`
- `runtime.stop()` shut everything down cleanly

---

## Step 3 — Add Supervision

Supervision is why Civitas exists. This example has an agent that crashes randomly. The supervisor restarts it automatically, every time.

Create `supervised.py`:

```python
import asyncio
import random
from civitas import AgentProcess, Runtime, Supervisor
from civitas.errors import ErrorAction
from civitas.messages import Message

class FlakyWorker(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        if random.random() < 0.5:
            raise RuntimeError("something went wrong")   # crashes 50% of the time
        return self.reply({"result": f"done: {message.payload['task']}"})

    async def on_error(self, error: Exception, message: Message) -> ErrorAction:
        # Crash the process — the supervisor will restart us
        return ErrorAction.ESCALATE

async def main():
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            strategy="ONE_FOR_ONE",   # restart only the crashed child
            max_restarts=10,
            restart_window=60.0,
            backoff="EXPONENTIAL",
            backoff_base=0.1,
            children=[FlakyWorker("worker")],
        )
    )
    await runtime.start()

    for i in range(10):
        result = await runtime.ask("worker", {"task": f"job-{i}"}, timeout=5.0)
        print(result.payload["result"])

    await runtime.stop()

asyncio.run(main())
```

Run it:

```bash
python supervised.py
```

You'll see all 10 tasks complete regardless of how many times the agent crashes. The supervisor handles every restart transparently.

### What the supervisor is doing

```
Task 0 → worker crashes → supervisor restarts (backoff: 0.1s) → task 0 retried
Task 1 → ok
Task 2 → worker crashes → supervisor restarts (backoff: 0.2s) → task 2 retried
...
```

Three restart strategies are available:

| Strategy | Behavior |
|---|---|
| `ONE_FOR_ONE` | Restart only the crashed process |
| `ONE_FOR_ALL` | Restart all children of this supervisor |
| `REST_FOR_ONE` | Restart the crashed process and all younger siblings |

See [Supervision](supervision.md) for backoff policies, escalation chains, and nested supervisors.

---

## Step 4 — Multi-Agent Communication

Agents communicate by name via the `MessageBus`. This example has three agents forming a pipeline.

Create `pipeline.py`:

```python
import asyncio
from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message

class Router(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        # ask another agent by name — waits for reply
        processed = await self.ask("processor", {"data": message.payload["input"]})
        formatted  = await self.ask("formatter", {"result": processed.payload["result"]})
        return self.reply(formatted.payload)

class Processor(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        data = message.payload["data"]
        return self.reply({"result": data.upper()})

class Formatter(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"output": f">>> {message.payload['result']} <<<"})

async def main():
    runtime = Runtime(
        supervisor=Supervisor("root", strategy="ONE_FOR_ONE", children=[
            Router("router"),
            Processor("processor"),
            Formatter("formatter"),
        ])
    )
    await runtime.start()

    result = await runtime.ask("router", {"input": "hello from civitas"})
    print(result.payload["output"])   # >>> HELLO FROM AGENCY <<<

    await runtime.stop()

asyncio.run(main())
```

Three messaging primitives are available inside `handle()`:

| Method | Description |
|---|---|
| `self.ask(name, payload)` | Send and wait for a reply (request-reply) |
| `self.send(name, payload)` | Send and move on (fire-and-forget) |
| `self.broadcast("agents.*", payload)` | Send to all agents matching a glob pattern |

See [Messaging](messaging.md) for request-reply internals, backpressure, and trace propagation.

---

## Step 5 — Add an LLM

Civitas's `ModelProvider` protocol abstracts LLM calls. Install the Anthropic provider:

```bash
pip install civitas-contrib[anthropic]
export ANTHROPIC_API_KEY=sk-...
```

Create `with_llm.py`:

```python
import asyncio
from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message
from civitas_contrib.plugins.anthropic import AnthropicProvider

class Assistant(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        response = await self.llm.chat(
            model="claude-haiku-4-5",
            messages=[{"role": "user", "content": message.payload["question"]}],
        )
        return self.reply({
            "answer": response.content,
            "tokens": response.tokens_in + response.tokens_out,
            "cost":   response.cost_usd,
        })

async def main():
    runtime = Runtime(
        supervisor=Supervisor("root", children=[Assistant("assistant")]),
        model_provider=AnthropicProvider(),
    )
    await runtime.start()

    result = await runtime.ask("assistant", {"question": "What is an OTP supervision tree?"})
    print(result.payload["answer"])
    print(f"  {result.payload['tokens']} tokens  ${result.payload['cost']:.4f}")

    await runtime.stop()

asyncio.run(main())
```

`self.llm` is injected by the runtime — you don't construct or configure it in your agent code. To switch to a different model provider (OpenAI, Gemini, Bedrock), change the `model_provider=` argument in `Runtime()`. See [Plugins](plugins.md).

---

## Step 6 — Add a Tool

Tools are registered with a `ToolRegistry` and injected into agents as `self.tools`.

```python
import asyncio
from typing import Any
from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message
from civitas.plugins.tools import ToolRegistry

class CalculatorTool:
    name = "calculator"
    schema: dict[str, Any] = {
        "name": "calculator",
        "description": "Evaluate a simple arithmetic expression",
        "input_schema": {
            "type": "object",
            "properties": {"expression": {"type": "string"}},
            "required": ["expression"],
        },
    }

    async def execute(self, **kwargs: Any) -> Any:
        expr = kwargs.get("expression", "0")
        return {"result": eval(expr, {"__builtins__": {}}, {})}  # noqa: S307

class MathAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        tool   = self.tools.get("calculator")
        result = await tool.execute(expression=message.payload["expr"])
        return self.reply({"answer": result["result"]})

async def main():
    tools = ToolRegistry()
    tools.register(CalculatorTool())

    runtime = Runtime(
        supervisor=Supervisor("root", children=[MathAgent("math")]),
        tool_registry=tools,
    )
    await runtime.start()

    result = await runtime.ask("math", {"expr": "2 ** 10"})
    print(result.payload["answer"])   # 1024

    await runtime.stop()

asyncio.run(main())
```

See [Plugins](plugins.md) for tool schema conventions and building custom providers.

---

## Step 7 — Observe what's happening

Civitas generates OpenTelemetry spans for every message, LLM call, and tool invocation automatically. No instrumentation code required.

### Console output (zero dependencies)

With just the core install, Civitas prints a human-readable trace summary to the console:

```
[10:00:00.123] orchestrator → researcher: research_query
[10:00:00.135] researcher: llm.chat(claude-haiku-4-5)  1520 → 430 tokens  $0.0089
[10:00:02.025] researcher: tool.invoke(web_search)  450ms  OK
[10:00:02.480] researcher → summarizer: summarize_request
[10:00:02.495] summarizer: llm.chat(claude-haiku-4-5)  890 → 210 tokens  $0.0003
[10:00:03.100] summarizer → orchestrator: reply  OK
─────────────────────────────────────────────────────────────────────
Total: 2.977s  |  2 LLM calls  |  $0.0092  |  0 errors
```

### Jaeger (full distributed tracing)

```bash
pip install civitas[otel]

# Start Jaeger (all-in-one for local dev)
docker run -d -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one

# Point Civitas at it
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# Run any example
python examples/research_assistant.py "Compare AI safety approaches"

# Open the trace UI
open http://localhost:16686
```

You'll see the full causal chain: every message hop, every LLM call, every tool invocation, with parent-child span relationships across agent boundaries.

See [Observability](observability.md) for the full span attribute reference and exporting to Grafana or other OTEL backends.

---

## Step 8 — Scaling up (preview)

The same agent code runs across multiple OS processes or across machines — swap the transport in your topology YAML, nothing else changes.

```yaml
# topology.yaml
transport:
  type: zmq          # multi-process on one machine
  pub_addr: tcp://127.0.0.1:5559
  sub_addr: tcp://127.0.0.1:5560
  start_proxy: true

supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent: { name: greeter, type: myapp.Greeter }
```

```bash
pip install civitas[zmq]
civitas run --topology topology.yaml
```

Change `type: zmq` to `type: nats` for distributed deployment across machines. See [Transports](transports.md) and [Deployment](deployment.md).

---

## What's next

| You want to… | Go to… |
|---|---|
| Understand the mental model deeply | [Core Concepts](concepts.md) |
| Configure supervision trees | [Supervision](supervision.md) |
| Learn all messaging patterns | [Messaging](messaging.md) |
| Scale to multi-process or distributed | [Transports](transports.md) |
| Write a custom plugin | [Plugins](plugins.md) |
| Define your system in YAML | [Topology YAML](topology.md) |
| Deploy to production | [Deployment](deployment.md) |
| Wrap an existing LangGraph agent | [Framework Adapters](adapters.md) |
| Contribute to Civitas | [Contributing](contributing.md) |

Or run the full hero demo:

```bash
python examples/research_assistant.py "Compare AI safety approaches"
```
