"""Civitas error hierarchy and ErrorAction enum."""

from __future__ import annotations

from enum import Enum


class ErrorAction(Enum):
    """Actions an agent can take when an error occurs in handle()."""

    RETRY = "RETRY"
    """Re-run handle() with the same message immediately, in place (up to
    max_retries, then escalate). Mailbox order is preserved — no other message
    is processed between attempts. For backoff, ``await asyncio.sleep(...)``
    inside ``on_error()`` before returning RETRY."""

    SKIP = "SKIP"
    """Discard the failed message, continue with next."""

    ESCALATE = "ESCALATE"
    """Crash the process — supervisor applies restart strategy."""

    STOP = "STOP"
    """Graceful shutdown of this process."""


class CivitasError(Exception):
    """Base exception for all Civitas runtime errors."""


class TransientError(CivitasError):
    """A transient, retryable error (e.g. network timeout, rate limit)."""


class MessageValidationError(CivitasError):
    """A message failed validation (e.g. reserved type prefix, non-serializable payload)."""


class MessageRoutingError(CivitasError):
    """A message could not be routed (e.g. unknown recipient)."""


class AgentSuspendedError(CivitasError):
    """An opt-in ``ask(..., fail_if_suspended=True)`` targeted a SUSPENDED agent
    (v0.10.0, hitl-polish.md D2). Raised immediately instead of buffering the
    request for the timeout — for callers that do not want to wait for a
    (possibly hours/days-long) HITL approval. The default ``ask()`` still buffers
    and delivers on resume; only ``fail_if_suspended=True`` raises this."""


class ConfigurationError(CivitasError):
    """Invalid or missing runtime configuration."""


class StateDecryptionError(CivitasError):
    """Raised when persisted agent state cannot be decrypted.

    Covers tampered ciphertext (AEAD ``InvalidTag``), an unknown envelope
    ``key_id``, an unsupported envelope version, and unencrypted (legacy)
    state read in strict mode. The message references the agent name and
    ``key_id`` only — never the plaintext or the key.
    """


class DeserializationError(CivitasError):
    """Raised when incoming bytes cannot be decoded into a Message.

    Provides a stable exception contract regardless of whether msgpack or JSON
    is in use — callers never need to catch library-specific exceptions.
    """


class PluginError(CivitasError):
    """Raised when a plugin cannot be loaded or instantiated.

    Inherits from CivitasError so callers catching the Civitas error hierarchy
    also catch plugin load failures.
    """

    def __init__(self, plugin_type: str, name: str, reason: str) -> None:
        self.plugin_type = plugin_type
        self.name = name
        self.reason = reason
        super().__init__(
            f"Failed to load {plugin_type} plugin '{name}': {reason}\n"
            f"  Hint: pip install civitas[{name}]"
        )


class SpawnError(CivitasError):
    """Raised when a dynamic agent spawn, despawn, or stop operation fails."""


class SignatureError(CivitasError):
    """Raised when a message signature is missing, invalid, or replayed."""


class CapabilityNotFoundError(CivitasError):
    """Raised when no registered agent declares the requested capability."""

    def __init__(self, capability: str) -> None:
        self.capability = capability
        super().__init__(f"No agent registered with capability '{capability}'")


class StreamError(CivitasError):
    """Base error for bus-native streaming failures (R7)."""


class SlowConsumerError(StreamError):
    """Raised when a stream consumer falls behind and the bounded sink overflows."""


class StreamInterrupted(StreamError):
    """Raised when an in-flight stream is torn down (e.g. the consuming agent stops)."""


class StreamTimeout(StreamError):
    """Raised when a stream exceeds its idle timeout or maximum duration."""
