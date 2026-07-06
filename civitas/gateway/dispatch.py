"""Transport-agnostic dispatch core shared by the HTTP and gRPC gateway surfaces.

Both surfaces translate their protocol into a single normalized request, hand it
to :meth:`GatewayDispatcher.dispatch`, and translate the returned
:class:`DispatchResult` back into protocol-specific responses. Centralizing agent
resolution, ``ask``/``cast``, and error classification here keeps HTTP and gRPC
routing/error semantics identical instead of drifting across two copies (D3).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from civitas.errors import MessageRoutingError
from civitas.messages import _uuid7
from civitas.streaming import StreamSink as StreamSink
from civitas.streaming import _StreamClosed as _StreamClosed

if TYPE_CHECKING:
    from civitas.gateway.core import HTTPGateway

logger = logging.getLogger(__name__)


class DispatchStatus(Enum):
    """Normalized outcome of a bus dispatch, mapped to protocol codes by callers.

    Each transport maps these onto its own status space. HTTP maps ``OK``→200,
    ``ACCEPTED``→202, ``AGENT_ERROR``→400, ``NOT_FOUND``→404, ``TIMEOUT``→504,
    ``INTERNAL``→500. gRPC returns ``OK`` and ``AGENT_ERROR`` as an ``AgentReply``
    (``AGENT_ERROR`` additionally populates the reply's ``error`` field in-band),
    ``ACCEPTED`` as ``Empty``, and aborts ``NOT_FOUND``/``TIMEOUT``/``INTERNAL`` with
    the ``NOT_FOUND``/``DEADLINE_EXCEEDED``/``INTERNAL`` status codes (D6).
    """

    OK = "ok"
    ACCEPTED = "accepted"
    AGENT_ERROR = "agent_error"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    INTERNAL = "internal"


@dataclass
class DispatchResult:
    """Normalized result of a bus dispatch.

    Attributes:
        status: The normalized outcome.
        payload: The agent's reply payload for ``OK``/``AGENT_ERROR``; empty for
            casts and error statuses.
        error: A human-readable error message for non-``OK`` statuses; ``None``
            otherwise.
    """

    status: DispatchStatus
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class GatewayDispatcher:
    """Resolves a normalized request onto the Civitas bus for any transport.

    Wraps an :class:`~civitas.gateway.core.HTTPGateway` (an ``AgentProcess``) and
    drives it via ``ask()`` / ``send()``, mapping bus outcomes and failures onto
    :class:`DispatchStatus` values so every transport shares one code path.
    """

    def __init__(
        self,
        gateway: HTTPGateway,
        request_timeout: float,
        *,
        stream_idle_timeout: float = 300.0,
        stream_max_duration: float = 3600.0,
    ) -> None:
        self._gateway = gateway
        self._request_timeout = request_timeout
        self._stream_idle_timeout = stream_idle_timeout
        self._stream_max_duration = stream_max_duration

    async def dispatch(
        self,
        *,
        recipient: str,
        msg_type: str,
        payload: dict[str, Any],
        mode: str,
        correlation_id: str = "",
        trace_id: str = "",
    ) -> DispatchResult:
        """Dispatch a normalized request to an agent and normalize the outcome.

        Args:
            recipient: Target agent name.
            msg_type: Message type stamped on the bus message.
            payload: JSON-serializable request payload.
            mode: ``"call"`` for request-reply, ``"cast"`` for fire-and-forget.
            correlation_id: Client-supplied correlation id, carried for tracing.
            trace_id: W3C trace id parsed at the edge, carried for tracing.

        Returns:
            A :class:`DispatchResult` carrying a normalized status and
            payload/error.
        """
        # Carried for edge traceability only: the bus core (ask/send) is left
        # untouched in G1, so these do not flow onto the Message.
        logger.debug(
            "gateway dispatch mode=%s recipient=%s type=%s correlation_id=%s trace_id=%s",
            mode,
            recipient,
            msg_type,
            correlation_id,
            trace_id,
        )

        if mode == "cast":
            return await self._cast(recipient, payload, msg_type)
        return await self._call(recipient, payload, msg_type)

    async def _call(self, recipient: str, payload: dict[str, Any], msg_type: str) -> DispatchResult:
        try:
            reply = await self._gateway.ask(
                recipient, payload, message_type=msg_type, timeout=self._request_timeout
            )
        except MessageRoutingError:
            return DispatchResult(DispatchStatus.NOT_FOUND, error=f"agent '{recipient}' not found")
        except TimeoutError:
            return DispatchResult(DispatchStatus.TIMEOUT, error="upstream timeout")
        except Exception:
            logger.exception("Gateway call error to '%s'", recipient)
            return DispatchResult(DispatchStatus.INTERNAL, error="internal error")

        reply_payload = reply.payload
        if "error" in reply_payload:
            return DispatchResult(
                DispatchStatus.AGENT_ERROR,
                payload=reply_payload,
                error=str(reply_payload["error"]),
            )
        return DispatchResult(DispatchStatus.OK, payload=reply_payload)

    async def _cast(self, recipient: str, payload: dict[str, Any], msg_type: str) -> DispatchResult:
        try:
            await self._gateway.send(recipient, payload, message_type=msg_type)
        except MessageRoutingError:
            return DispatchResult(DispatchStatus.NOT_FOUND, error=f"agent '{recipient}' not found")
        except Exception:
            logger.exception("Gateway cast error to '%s'", recipient)
            return DispatchResult(DispatchStatus.INTERNAL, error="internal error")
        return DispatchResult(DispatchStatus.ACCEPTED)

    async def stream(
        self,
        *,
        recipient: str,
        msg_type: str,
        payload: dict[str, Any],
        trace_id: str = "",
    ) -> AsyncIterator[dict[str, Any]]:
        """Dispatch a streaming request and yield the agent's chunks in order.

        Opens a sink keyed by a fresh correlation_id, sends the request with
        ``reply_to`` pointing back at the gateway, then drains the sink until the
        agent's terminator. Raises ``_StreamClosed`` on agent error, slow consumer,
        or timeout; the sink is always unregistered on exit.
        """
        correlation_id = _uuid7()
        sink = self._gateway._open_stream(correlation_id)
        try:
            await self._gateway._send_stream_request(
                recipient=recipient,
                payload=payload,
                correlation_id=correlation_id,
                msg_type=msg_type,
                trace_id=trace_id,
            )
            async for chunk in sink.drain(
                idle_timeout=self._stream_idle_timeout,
                max_duration=self._stream_max_duration,
            ):
                yield chunk
        finally:
            self._gateway._close_stream(correlation_id)
