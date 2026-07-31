"""Unit tests for civitas.dashboard.resources (v0.9.1, dashboard-v2 D-DASH-3).

psutil is mocked here — proving the wiring is correct doesn't need real
process introspection. One deliberately un-mocked smoke test guards against
psutil API drift (matches the design doc's §10 testing strategy).
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from civitas.dashboard.resources import sample_process, try_start_process_sampler


class TestTryStartProcessSampler:
    def test_returns_none_when_psutil_not_installed(self) -> None:
        with patch.dict(sys.modules, {"psutil": None}):
            assert try_start_process_sampler() is None

    def test_primes_cpu_percent_once(self) -> None:
        """The first cpu_percent() call is discarded (priming the baseline) —
        the caller never sees that meaningless reading."""
        fake_psutil = MagicMock()
        fake_process = MagicMock()
        fake_psutil.Process.return_value = fake_process

        with patch.dict(sys.modules, {"psutil": fake_psutil}):
            result = try_start_process_sampler()

        assert result is fake_process
        fake_process.cpu_percent.assert_called_once_with()  # primed exactly once


class TestSampleProcess:
    def test_returns_none_for_none_handle(self) -> None:
        assert sample_process(None) is None

    def test_returns_shape_with_real_values(self) -> None:
        fake_process = MagicMock()
        fake_process.pid = 1234
        fake_process.cpu_percent.return_value = 12.5
        fake_process.memory_info.return_value.rss = 104857600
        fake_process.create_time.return_value = 1000.0

        # v0.9.6: patch detect_container so this shape assertion is deterministic
        # regardless of environment (it returns True under Docker CI, False on a
        # host box -- the container hint's own detection is tested separately).
        with (
            patch("time.time", return_value=1100.0),
            patch(
                "civitas.dashboard.resources.detect_container",
                return_value={"containerized": False, "orchestrator": None},
            ),
        ):
            result = sample_process(fake_process)

        assert result == {
            "pid": 1234,
            "cpu_percent": 12.5,
            "rss_bytes": 104857600,
            "uptime_seconds": 100.0,
            "container": {"containerized": False, "orchestrator": None},
        }

    def test_returns_none_on_any_exception_never_raises(self) -> None:
        """A process that exits mid-sample (or any other psutil error)
        yields None rather than crashing the caller (F03-7 containment)."""
        fake_process = MagicMock()
        fake_process.cpu_percent.side_effect = RuntimeError("process gone")

        assert sample_process(fake_process) is None


def test_real_psutil_smoke_test() -> None:
    """Deliberately UN-mocked: guards against psutil API drift across
    versions/platforms (design §10) — proves the real library still has the
    shape this module depends on, not just that our mocks agree with
    themselves."""
    sampler = try_start_process_sampler()
    assert sampler is not None, "psutil must be installed for this test (civitas[dashboard])"
    result = sample_process(sampler)
    assert result is not None
    assert result["pid"] > 0
    assert result["cpu_percent"] >= 0.0
    assert result["rss_bytes"] > 0
    assert result["uptime_seconds"] >= 0.0


class TestDetectContainer:
    """v0.9.6: read-only container detection (reporting, not management)."""

    def _reset_cache(self) -> None:
        import civitas.dashboard.resources as res

        res._container_info = None

    def test_shape_and_types(self) -> None:
        from civitas.dashboard.resources import detect_container

        self._reset_cache()
        info = detect_container()
        assert set(info) == {"containerized", "orchestrator"}
        assert isinstance(info["containerized"], bool)
        assert info["orchestrator"] is None or isinstance(info["orchestrator"], str)

    def test_kubernetes_env_detected(self) -> None:
        import civitas.dashboard.resources as res

        self._reset_cache()
        with patch.dict("os.environ", {"KUBERNETES_SERVICE_HOST": "10.0.0.1"}):
            info = res.detect_container()
        assert info == {"containerized": True, "orchestrator": "kubernetes"}
        self._reset_cache()  # don't leak the cached True into other tests

    def test_cached_after_first_call(self) -> None:
        import civitas.dashboard.resources as res

        self._reset_cache()
        first = res.detect_container()
        assert res.detect_container() is first  # same cached object
        self._reset_cache()

    def test_never_raises_without_proc_or_dockerenv(self) -> None:
        """On a host box (no /.dockerenv, no /proc/1/cgroup) detection returns
        False, never crashes -- cross-platform safety."""
        import civitas.dashboard.resources as res

        self._reset_cache()
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("os.path.exists", return_value=False),
            patch("builtins.open", side_effect=OSError("no such file")),
        ):
            info = res.detect_container()
        assert info == {"containerized": False, "orchestrator": None}
        self._reset_cache()
