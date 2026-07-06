"""YAML env-var substitution — resolves ``${VAR_NAME}`` patterns against os.environ."""

from __future__ import annotations

import os
import re
from typing import Any

from civitas.errors import ConfigurationError

_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def substitute_vars(obj: Any, env: dict[str, str] | None = None) -> Any:
    """Recursively resolve ``${VAR_NAME}`` patterns in a parsed YAML structure.

    Walks dicts, lists, and strings. Numbers, booleans, and None pass through
    unchanged. Raises ``ConfigurationError`` for any unset variable.

    Args:
        obj:  Parsed YAML value (dict, list, str, int, float, bool, None).
        env:  Environment mapping. Defaults to ``os.environ``.

    Returns:
        The input structure with all ``${VAR}`` strings replaced.

    Raises:
        ConfigurationError: if a referenced variable is not present in ``env``.
    """
    resolved_env: dict[str, str] = dict(os.environ) if env is None else env

    def _resolve(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_resolve(item) for item in value]
        if isinstance(value, str):
            return _VAR_RE.sub(_replace_var, value)
        return value

    def _replace_var(match: re.Match[str]) -> str:
        var_name = match.group(1)
        if var_name not in resolved_env:
            raise ConfigurationError(
                f"Environment variable '${{{var_name}}}' is not set. "
                f"Set it before starting the runtime."
            )
        return resolved_env[var_name]

    return _resolve(obj)
