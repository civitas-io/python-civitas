"""Packaging metadata tests — py.typed marker (GH #7)."""

from __future__ import annotations

import importlib.resources

import civitas


class TestPyTyped:
    def test_py_typed_marker_present(self) -> None:
        marker = importlib.resources.files(civitas) / "py.typed"
        assert marker.is_file()

    def test_py_typed_marker_is_empty(self) -> None:
        marker = importlib.resources.files(civitas) / "py.typed"
        assert marker.read_text() == ""
