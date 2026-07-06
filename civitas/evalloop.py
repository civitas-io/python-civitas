"""EvalLoop — corrective observability loop for agent behaviour monitoring.

Local evaluation tier (M2.5):
  EvalAgent sits in the supervision tree alongside regular agents.
  Agents emit EvalEvents via self.emit_eval(); EvalAgent scores them via
  on_eval_event() and sends CorrectionSignals back.

Remote evaluation tier (M2.6):
  EvalExporter protocol — adapters for Arize Phoenix, Fiddler, Langfuse,
  Braintrust, LangSmith. Defined here; implemented as civitas[arize] etc.

Severity levels:
  nudge   — soft guidance; agent continues, on_correction() is called
  redirect — significant concern; agent should change course, on_correction() called
  halt    — critical violation; agent's message loop is stopped cleanly
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from civitas.messages import Message, _new_span_id
from civitas.process import AgentProcess

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------


@dataclass
class EvalEvent:
    """An observable event emitted by an agent for evaluation.

    Fields align with OTEL GenAI Semantic Conventions so remote exporters
    can forward as standard spans without transformation.
    """

    agent_name: str
    event_type: str
    payload: dict[str, Any]
    trace_id: str = ""
    message_id: str = ""
    timestamp: float = field(default_factory=time.time)


@dataclass
class CorrectionSignal:
    """A correction returned by on_eval_event().

    severity:
        nudge    — minor issue; agent continues, on_correction() called
        redirect — significant concern; agent should change course
        halt     — critical violation; agent is stopped cleanly
    """

    severity: Literal["nudge", "redirect", "halt"]
    reason: str
    payload: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# EvalExporter protocol (M2.6 — interface only, no implementations here)
# ---------------------------------------------------------------------------


@runtime_checkable
class EvalExporter(Protocol):
    """Protocol for remote eval engine adapters.

    Implementations live in civitas[arize], civitas[fiddler], etc.
    Each exporter translates EvalEvent to the target platform's format.
    """

    async def export(self, event: EvalEvent) -> None:
        """Forward an EvalEvent to the remote eval engine."""
        ...


# ---------------------------------------------------------------------------
# EvalAgent
# ---------------------------------------------------------------------------


class EvalAgent(AgentProcess):
    """Supervised process that monitors agent behaviour and sends corrections.

    Override on_eval_event() to implement your eval logic. Return a
    CorrectionSignal to intervene, or None to take no action.

    Exporters (M2.6) receive every EvalEvent before on_eval_event() runs.
    Errors in exporters are logged and swallowed — they never block the
    local evaluation loop.

    Usage:
        class MyEval(EvalAgent):
            async def on_eval_event(self, event: EvalEvent) -> CorrectionSignal | None:
                if "IGNORE_ALL" in event.payload.get("content", ""):
                    return CorrectionSignal(severity="halt", reason="Prompt injection detected")
                return None
    """

    def __init__(
        self,
        name: str,
        max_corrections_per_window: int = 10,
        window_seconds: float = 60.0,
        exporters: list[EvalExporter] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self._max_corrections = max_corrections_per_window
        self._window_seconds = window_seconds
        self._exporters: list[EvalExporter] = exporters or []
        # Sliding window per target agent: agent_name -> list of correction timestamps
        self._correction_timestamps: dict[str, list[float]] = {}

    # ------------------------------------------------------------------
    # Override point
    # ------------------------------------------------------------------

    async def on_eval_event(self, event: EvalEvent) -> CorrectionSignal | None:
        """Evaluate an agent event. Return a CorrectionSignal to intervene.

        Return None to take no action. This is the only method you need to
        override — EvalAgent handles delivery, rate limiting, and halt.
        """
        return None

    # ------------------------------------------------------------------
    # Message dispatch
    # ------------------------------------------------------------------

    async def handle(self, message: Message) -> None:
        if message.type != "civitas.eval.event":
            return

        event = EvalEvent(
            agent_name=message.payload.get("agent_name", message.sender),
            event_type=message.payload.get("event_type", ""),
            payload=message.payload,
            trace_id=message.trace_id,
            message_id=message.id,
        )

        for exporter in self._exporters:
            try:
                await exporter.export(event)
            except Exception as exc:
                logger.warning(
                    "EvalExporter %r failed for event '%s' from agent '%s': %s",
                    exporter,
                    event.event_type,
                    event.agent_name,
                    exc,
                )

        signal = await self.on_eval_event(event)
        if signal is None:
            return

        target = event.agent_name
        if not self._check_rate_limit(target):
            logger.warning(
                "EvalAgent '%s': rate limit exceeded for agent '%s' — dropping %s correction",
                self.name,
                target,
                signal.severity,
            )
            return

        await self._send_correction(target, signal, message)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_rate_limit(self, agent_name: str) -> bool:
        """Return True if a correction can be sent; False if rate limit exceeded."""
        now = time.time()
        timestamps = self._correction_timestamps.setdefault(agent_name, [])
        # Prune entries outside the window
        self._correction_timestamps[agent_name] = [
            t for t in timestamps if now - t < self._window_seconds
        ]
        if len(self._correction_timestamps[agent_name]) >= self._max_corrections:
            return False
        self._correction_timestamps[agent_name].append(now)
        return True

    async def _send_correction(
        self, target: str, signal: CorrectionSignal, original: Message
    ) -> None:
        if self._bus is None:
            return

        msg_type = "civitas.eval.halt" if signal.severity == "halt" else "civitas.eval.correction"
        msg = Message(
            type=msg_type,
            sender=self.name,
            recipient=target,
            payload={
                "severity": signal.severity,
                "reason": signal.reason,
                **signal.payload,
            },
            trace_id=original.trace_id,
            span_id=_new_span_id(),
            parent_span_id=original.span_id or None,
            priority=1 if signal.severity == "halt" else 0,
        )
        await self._bus.route(msg)
