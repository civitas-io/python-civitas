"""Unit tests for the ModelProvider protocol types (V4, #42).

civitas/plugins/model.py measured 0% — these dataclasses are the wire contract
between every contrib provider and every agent's ``self.llm`` usage, and had
never been imported by a test.
"""

from __future__ import annotations

from typing import Any

from civitas.plugins.model import ModelProvider, ModelResponse, ToolCall


def test_tool_call_shape():
    tc = ToolCall(id="tc_1", name="web_search", input={"query": "civitas"})
    assert (tc.id, tc.name, tc.input) == ("tc_1", "web_search", {"query": "civitas"})


def test_model_response_defaults():
    """cost_usd and tool_calls are optional — the documented ModelResponse contract."""
    r = ModelResponse(content="hi", model="test-model", tokens_in=10, tokens_out=2)
    assert r.cost_usd is None
    assert r.tool_calls is None


def test_model_response_with_tool_calls():
    r = ModelResponse(
        content="",
        model="test-model",
        tokens_in=100,
        tokens_out=20,
        cost_usd=0.0012,
        tool_calls=[ToolCall(id="1", name="t", input={})],
    )
    assert r.tool_calls is not None and r.tool_calls[0].name == "t"
    assert r.cost_usd == 0.0012


def test_slots_reject_unknown_attributes():
    """slots=True: typos on response fields fail loudly instead of silently."""
    r = ModelResponse(content="x", model="m", tokens_in=1, tokens_out=1)
    try:
        r.token_in = 5  # type: ignore[attr-defined]
    except AttributeError:
        return
    raise AssertionError("slots did not reject unknown attribute")


async def test_mock_provider_satisfies_protocol():
    """The unit-testing pattern AGENTS.md mandates: mock self.llm, never real APIs."""

    class MockProvider:
        async def chat(
            self,
            model: str,
            messages: list[dict[str, Any]],
            tools: list[Any] | None = None,
        ) -> ModelResponse:
            return ModelResponse(
                content="mocked", model=model, tokens_in=len(messages), tokens_out=1
            )

    provider: ModelProvider = MockProvider()
    response = await provider.chat("test-model", [{"role": "user", "content": "hi"}])
    assert response.content == "mocked"
    assert response.model == "test-model"
