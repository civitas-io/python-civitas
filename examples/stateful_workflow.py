"""Stateful Workflow with Crash Recovery.

A workflow agent processes a 7-step pipeline, checkpointing after each step.
Kill it mid-execution (Ctrl+C), restart — it resumes from the last checkpoint.

Requires: pip install civitas civitas-contrib
(SQLiteStateStore lives in civitas-contrib, not core civitas)

Usage:
    python examples/stateful_workflow.py
    # Kill at step 4 with Ctrl+C
    python examples/stateful_workflow.py
    # Agent resumes from step 4
"""

import asyncio

from civitas_contrib.plugins.sqlite_store import SQLiteStateStore

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message

TOTAL_STEPS = 7


class WorkflowAgent(AgentProcess):
    """Processes a multi-step pipeline with checkpointing."""

    async def on_start(self) -> None:
        step = self.state.get("current_step", 0)
        if step > 0:
            print(
                f"Resuming workflow from step {step} of {TOTAL_STEPS} "
                f"(restored from civitas_state.db)"
            )
        else:
            print(f"Starting fresh workflow ({TOTAL_STEPS} steps)")

    async def handle(self, message: Message) -> Message | None:
        start_step = self.state.get("current_step", 0)

        for step in range(start_step, TOTAL_STEPS):
            current = step + 1
            print(f"  Step {current}/{TOTAL_STEPS}: processing...")
            await asyncio.sleep(0.5)  # simulate work

            # Checkpoint after each step
            self.state["current_step"] = current
            self.state["results"] = self.state.get("results", [])
            self.state["results"].append(f"result_{current}")
            await self.checkpoint()
            print(f"  Step {current}/{TOTAL_STEPS}: checkpointed ✓")

        # Reset for next workflow run
        final_results = self.state.get("results", [])
        self.state = {}
        await self.checkpoint()

        return self.reply(
            {
                "status": "complete",
                "steps": TOTAL_STEPS,
                "results": final_results,
            }
        )


async def main():
    store = SQLiteStateStore("civitas_state.db")
    runtime = Runtime(
        supervisor=Supervisor("root", children=[WorkflowAgent("workflow")]),
        state_store=store,
    )
    await runtime.start()

    try:
        result = await runtime.ask(
            "workflow",
            {"type": "start_workflow"},
            timeout=60.0,
        )
        print(f"\nWorkflow complete: {result.payload}")
    except KeyboardInterrupt:
        print("\n\nInterrupted! State saved. Restart to resume.")
    finally:
        await runtime.stop()
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
