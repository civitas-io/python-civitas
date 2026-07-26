"""Process resource sampling (v0.9.1, dashboard-v2 D-DASH-3).

Shared by ``Worker`` (self-measurement, included in its ``_agency.health_ack``
reply) and ``TopologyServer`` (self-measurement of the Runtime's own process).
``psutil`` is optional — neither Worker nor Runtime requires it to function;
resource stats are simply omitted when it isn't installed
(``pip install 'civitas[dashboard]'``).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)


def try_start_process_sampler() -> Any | None:
    """Return a primed ``psutil.Process`` handle for THIS process, or ``None``.

    Must be created ONCE and reused across every subsequent sample — this is
    not an arbitrary choice: ``psutil.Process.cpu_percent()``'s first-ever
    call on a given handle has no prior reading to compare against and
    returns a meaningless value (typically ``0.0``), by psutil's own design.
    A fresh ``psutil.Process()`` per probe would make EVERY reading the
    meaningless first one — a real, easy-to-miss correctness bug, not a
    cosmetic one. Priming here (one throwaway call) means every reading a
    caller actually uses via :func:`sample_process` is a real delta.
    """
    try:
        import psutil
    except ImportError:
        return None
    proc = psutil.Process(os.getpid())
    proc.cpu_percent()  # prime the baseline; this specific reading is discarded
    return proc


def sample_process(proc: Any | None) -> dict[str, Any] | None:
    """One resource snapshot from a primed handle, or ``None`` if unavailable.

    Never raises — a process that exits mid-sample (or any other psutil
    error) yields ``None`` rather than crashing the caller (matches this
    codebase's F03-7 containment convention for background/reporting paths).
    """
    if proc is None:
        return None
    try:
        return {
            "pid": proc.pid,
            "cpu_percent": proc.cpu_percent(),
            "rss_bytes": proc.memory_info().rss,
            "uptime_seconds": time.time() - proc.create_time(),
        }
    except Exception:
        logger.debug("process resource sample failed", exc_info=True)
        return None
