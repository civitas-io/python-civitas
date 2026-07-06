"""Audit sink implementations.

NullSink       — no-op, for tests and disabled audit.
JsonlFileSink  — append-only JSONL file with batched fsync and SIGHUP rotation.
SyslogSink     — emit via Python's SysLogHandler.
OtlpSink       — emit as OTEL log records (requires civitas[otel]).
"""

from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from civitas.audit.types import AuditEvent


def _now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# NullSink
# ---------------------------------------------------------------------------


class NullSink:
    """No-op sink — discards every event. Use in tests or when auditing is off."""

    async def emit(self, event: AuditEvent) -> None:
        pass

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# JsonlFileSink
# ---------------------------------------------------------------------------


class JsonlFileSink:
    """Append-only JSONL audit log with async batching and fsync.

    Events are queued and written by a background drain task in batches.
    Two flush triggers:
      - ``batch_size`` events accumulated
      - ``flush_interval`` seconds elapsed since last write

    With ``sync_writes=True`` each batch is fsynced before the drain loop
    continues — slower but durable against OS crashes.

    Send SIGHUP to rotate the log file (reopen at the same path).
    """

    def __init__(
        self,
        path: str | Path,
        *,
        sync_writes: bool = False,
        batch_size: int = 100,
        flush_interval: float = 0.1,
    ) -> None:
        self._path = Path(path)
        self._sync_writes = sync_writes
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._rotate_flag = False
        self._file = open(self._path, "a", encoding="utf-8")  # noqa: SIM115

    def _setup_sighup(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGHUP, self._on_sighup)
        except (RuntimeError, OSError, AttributeError):
            pass  # not on Unix, or no running loop

    def _on_sighup(self) -> None:
        self._rotate_flag = True

    def _rotate(self) -> None:
        self._file.close()
        self._file = open(self._path, "a", encoding="utf-8")  # noqa: SIM115
        self._rotate_flag = False

    def _write_batch(self, lines: list[str]) -> None:
        self._file.write("\n".join(lines) + "\n")
        self._file.flush()
        if self._sync_writes:
            os.fsync(self._file.fileno())

    async def _drain_loop(self) -> None:
        self._setup_sighup()
        while True:
            batch: list[str] = []
            deadline = asyncio.get_event_loop().time() + self._flush_interval

            # Collect up to batch_size items or until flush_interval elapses
            while len(batch) < self._batch_size:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    if item is None:  # close sentinel
                        if batch:
                            self._write_batch(batch)
                        return
                    batch.append(item)
                except TimeoutError:
                    break

            if batch:
                self._write_batch(batch)

            if self._rotate_flag:
                self._rotate()

    async def emit(self, event: AuditEvent) -> None:
        await self._queue.put(json.dumps(event))
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._drain_loop())

    async def flush(self) -> None:
        """Synchronously drain whatever is currently queued."""
        lines: list[str] = []
        while True:
            try:
                item = self._queue.get_nowait()
                if item is None:
                    await self._queue.put(None)  # re-queue sentinel
                    break
                lines.append(item)
            except asyncio.QueueEmpty:
                break
        if lines:
            self._write_batch(lines)

    async def close(self) -> None:
        await self._queue.put(None)  # sentinel stops drain loop
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except TimeoutError:
                self._task.cancel()
        await self.flush()  # drain any items that arrived after the sentinel
        self._file.close()


# ---------------------------------------------------------------------------
# SyslogSink
# ---------------------------------------------------------------------------


class SyslogSink:
    """Emit audit events as JSON to the local syslog daemon.

    Uses Python's ``logging.handlers.SysLogHandler`` so no external
    dependencies are required.

    Args:
        address:  ``(host, port)`` tuple or filesystem socket path.
                  Defaults to ``"/dev/log"`` (Linux) or
                  ``"/var/run/syslog"`` (macOS).
        facility: syslog facility constant.  Defaults to ``LOG_LOCAL0``.
    """

    def __init__(
        self,
        address: str | tuple[str, int] | None = None,
        facility: int = logging.handlers.SysLogHandler.LOG_LOCAL0,
    ) -> None:
        if address is None:
            # Detect platform default
            if os.path.exists("/dev/log"):
                address = "/dev/log"
            elif os.path.exists("/var/run/syslog"):
                address = "/var/run/syslog"
            else:
                address = ("localhost", logging.handlers.SYSLOG_UDP_PORT)
        self._handler = logging.handlers.SysLogHandler(address=address, facility=facility)

    async def emit(self, event: AuditEvent) -> None:
        record = logging.LogRecord(
            name="civitas.audit",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=json.dumps(event),
            args=(),
            exc_info=None,
        )
        self._handler.emit(record)

    async def flush(self) -> None:
        self._handler.flush()

    async def close(self) -> None:
        self._handler.close()


# ---------------------------------------------------------------------------
# OtlpSink
# ---------------------------------------------------------------------------


class OtlpSink:
    """Emit audit events as OTEL log records via OTLP/gRPC.

    Requires ``civitas[otel]``:
        pip install 'civitas[otel]'

    Args:
        endpoint:     OTLP collector endpoint (default: ``http://localhost:4317``).
        service_name: Reported ``service.name`` resource attribute.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:4317",
        service_name: str = "civitas",
    ) -> None:
        try:
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
            from opentelemetry.sdk._logs import LoggerProvider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
            from opentelemetry.sdk.resources import Resource
        except ImportError as exc:
            raise ImportError(
                "civitas[otel] is required for OtlpSink. Install with: pip install 'civitas[otel]'"
            ) from exc

        resource = Resource.create({"service.name": service_name})
        provider = LoggerProvider(resource=resource)
        provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint))
        )
        self._logger = provider.get_logger("civitas.audit")
        self._provider = provider

    async def emit(self, event: AuditEvent) -> None:
        from opentelemetry._logs.severity import SeverityNumber
        from opentelemetry.sdk._logs import LogRecord  # type: ignore[attr-defined]

        record = LogRecord(
            timestamp=int(time.time() * 1e9),
            severity_number=SeverityNumber.INFO,
            severity_text="INFO",
            body=json.dumps(event),
            attributes={k: str(v) for k, v in event.items() if k != "details"},
        )
        self._logger.emit(record)

    async def flush(self) -> None:
        self._provider.force_flush()

    async def close(self) -> None:
        self._provider.shutdown()  # type: ignore[no-untyped-call]


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def sink_from_config(cfg: dict[str, Any]) -> NullSink | JsonlFileSink | SyslogSink | OtlpSink:
    """Build an audit sink from a config dict.

    Expected shape::

        audit:
          sink: jsonl          # jsonl | syslog | otlp | null
          path: /var/log/civitas/audit.jsonl   # jsonl only
          sync_writes: false   # jsonl only
          endpoint: http://localhost:4317      # otlp only
          service_name: civitas               # otlp only
    """
    kind = cfg.get("sink", "null")
    if kind == "jsonl":
        path = cfg.get("path", "civitas_audit.jsonl")
        return JsonlFileSink(
            path=path,
            sync_writes=cfg.get("sync_writes", False),
            batch_size=cfg.get("batch_size", 100),
            flush_interval=cfg.get("flush_interval", 0.1),
        )
    if kind == "syslog":
        address = cfg.get("address")
        if isinstance(address, list):
            address = tuple(address)
        return SyslogSink(address=address or None)
    if kind == "otlp":
        return OtlpSink(
            endpoint=cfg.get("endpoint", "http://localhost:4317"),
            service_name=cfg.get("service_name", "civitas"),
        )
    return NullSink()
