"""Docs code-fence checker (milestone item 3, docs/milestones.md "Public documentation
reliability") -- extends test_examples_smoke.py's exact philosophy (proof, not trust)
to markdown.

The audit that produced docs/milestones.md's "Public documentation reliability" item
found the public docs extensively stale relative to source: `TopologyServer` (removed at
v0.9.5) still imported in prose examples, a fabricated `civitas.mcp.server.MCPServer`
class, a stale `civitas/plugins/anthropic.py` file listing, etc. Every one of those was a
```python fence claiming an import path or symbol that doesn't exist -- exactly the kind
of checkable claim `tests/integration/test_examples_smoke.py` already proves for
`examples/*.py`. This file is the same structural fix applied to markdown: not full
execution of every fragment (most doc snippets are illustrative partial classes, not
complete runnable scripts -- that's what `examples/*.py` is for), but proof that every
`civitas.*` import shown in a doc actually resolves against real, installed source.

Scope, deliberately narrow:
- Only `civitas.*` imports are a hard failure on a missing module/symbol -- this repo's
  own dev environment always has civitas importable, so there's no excuse for drift.
- `civitas_contrib.*` / `fabrica.*` imports are best-effort: verified if that package
  happens to be installed, skipped (not failed) if not -- this repo's own CI
  deliberately never installs those (see AGENTS.md's dependency-direction rule), so a
  hard requirement here would either be a false failure or require installing packages
  from other repos into this one's CI, which is the cross-repo blind spot this org's own
  monorepo-vs-separate-repos council decision (docs/milestones.md) explicitly chose not
  to solve this way.
- Everything else (`myapp.*`, `mypkg.*`, third-party SDKs like `aioredis`) is completely
  unchecked -- those are illustrative names in "writing a custom X" examples, not real
  importable code, and were never a source of any breakage found in the audit.
- A ```python fence with invalid syntax is always a hard failure, regardless of prefix.

This does not replace human/LLM review of doc *prose* or *behavioral* claims (a version
number, a "the default is X" sentence) -- only the mechanically checkable subset: does
this import path exist, does this class have this attribute.
"""

from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

_FENCE_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)

# Historical/narrative logs, not current reference docs -- excluded for the same reason
# docs/design/*.md is out of scope for the doc-accuracy audit itself (see
# docs/milestones.md, "Public documentation reliability").
EXCLUDED_DOCS = {
    "docs/milestones.md": "historical changelog/plan log, not current reference",
}

STRICT_PREFIX = "civitas"
BEST_EFFORT_PREFIXES = ("civitas_contrib", "fabrica")


def _target_docs() -> list[str]:
    docs = {str(p.relative_to(REPO_ROOT)) for p in (REPO_ROOT / "docs").glob("*.md")}
    docs |= {"README.md", "AGENTS.md", "CONTRIBUTING.md"}
    return sorted(docs - set(EXCLUDED_DOCS))


TARGET_DOCS = _target_docs()


def _extract_python_blocks(doc_path: Path) -> list[tuple[int, str]]:
    """Return (1-indexed start line, code) for every ```python fence in doc_path."""
    text = doc_path.read_text(encoding="utf-8")
    blocks = []
    for m in _FENCE_RE.finditer(text):
        start_line = text.count("\n", 0, m.start()) + 1
        blocks.append((start_line, m.group(1)))
    return blocks


def _iter_import_targets(tree: ast.Module) -> list[tuple[str, str | None]]:
    """Yield (module, symbol) for every import in the block; symbol is None for a
    bare `import module` (module-only existence check)."""
    targets: list[tuple[str, str | None]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                targets.append((alias.name, None))
        elif isinstance(node, ast.ImportFrom):
            if node.module is None or node.level > 0:
                continue  # relative import -- not a real installable path
            for alias in node.names:
                if alias.name == "*":
                    continue
                targets.append((node.module, alias.name))
    return targets


def _collect_cases() -> list[tuple[str, int, str, str | None]]:
    """One case per (module, symbol) import target found across every doc's python
    fences, plus one SYNTAX_ERROR case per fence that fails to parse."""
    cases: list[tuple[str, int, str, str | None]] = []
    for rel in TARGET_DOCS:
        doc_path = REPO_ROOT / rel
        for start_line, code in _extract_python_blocks(doc_path):
            try:
                tree = ast.parse(code)
            except SyntaxError as exc:
                cases.append((rel, start_line, "SYNTAX_ERROR", str(exc)))
                continue
            for module, symbol in _iter_import_targets(tree):
                top = module.split(".")[0]
                if top != STRICT_PREFIX and top not in BEST_EFFORT_PREFIXES:
                    continue  # third-party / user-app names -- illustrative only
                cases.append((rel, start_line, module, symbol))
    return cases


CASES = _collect_cases()


def _case_id(case: tuple[str, int, str, str | None]) -> str:
    rel, line, module, symbol = case
    return f"{rel}:{line}:{module}" + (f".{symbol}" if symbol else "")


@pytest.mark.parametrize("case", CASES, ids=[_case_id(c) for c in CASES])
def test_doc_python_fence_import_resolves(case: tuple[str, int, str, str | None]) -> None:
    rel, line, module, symbol = case
    if module == "SYNTAX_ERROR":
        pytest.fail(f"{rel}:{line} -- python code fence has invalid syntax: {symbol}")

    is_best_effort = module.split(".")[0] in BEST_EFFORT_PREFIXES
    try:
        mod = importlib.import_module(module)
    except ModuleNotFoundError as exc:
        if is_best_effort:
            pytest.skip(
                f"{module} not installed in this repo's own dev env "
                "(expected -- it lives in a separate repo with its own CI)"
            )
        pytest.fail(f"{rel}:{line} -- `{module}` does not exist: {exc}")
        return

    if symbol is not None and not hasattr(mod, symbol):
        pytest.fail(
            f"{rel}:{line} -- `from {module} import {symbol}` -- "
            f"{module} has no attribute {symbol!r}"
        )


def test_all_top_level_docs_are_covered() -> None:
    """A new docs/*.md file (or README.md/AGENTS.md/CONTRIBUTING.md) can't silently
    skip this checker without an explicit, documented exclusion reason."""
    all_docs = {str(p.relative_to(REPO_ROOT)) for p in (REPO_ROOT / "docs").glob("*.md")}
    all_docs |= {"README.md", "AGENTS.md", "CONTRIBUTING.md"}
    covered = set(TARGET_DOCS) | set(EXCLUDED_DOCS)
    missing = all_docs - covered
    assert not missing, (
        f"New doc(s) not covered by the code-fence checker and not in EXCLUDED_DOCS: "
        f"{sorted(missing)}"
    )
