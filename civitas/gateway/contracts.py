"""@contract decorator and Pydantic validation helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def contract(
    *,
    request: type[Any] | None = None,
    response: type[Any] | None = None,
) -> Callable[..., Any]:
    """Annotate a handler method with request/response Pydantic schemas.

    Schemas are stored as ``fn._civitas_contract`` and read by
    ``RouteTable.merge_contracts_from()`` to wire validation into the gateway.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn._civitas_contract = {"request": request, "response": response}  # type: ignore[attr-defined]
        return fn

    return decorator


def validate_request(schema: type[Any], body: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
    """Validate *body* against a Pydantic model.

    Returns ``(True, None)`` on success or ``(False, error_dict)`` on failure.
    The error dict follows the FastAPI-compatible 422 shape.
    """
    try:
        schema.model_validate(body)
        return True, None
    except Exception as exc:
        errors: list[dict[str, Any]] = []
        if hasattr(exc, "errors"):
            for e in exc.errors():
                errors.append(
                    {
                        "loc": list(e.get("loc", [])),
                        "msg": e.get("msg", str(e)),
                        "type": e.get("type", "value_error"),
                    }
                )
        else:
            errors = [{"loc": [], "msg": str(exc), "type": "value_error"}]
        return False, {"detail": errors}


def validate_response(schema: type[Any], payload: dict[str, Any]) -> tuple[bool, str | None]:
    """Validate *payload* against a Pydantic model.

    Returns ``(True, None)`` on success or ``(False, error_msg)`` on failure.
    """
    try:
        schema.model_validate(payload)
        return True, None
    except Exception as exc:
        return False, str(exc)
