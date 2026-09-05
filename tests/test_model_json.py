from __future__ import annotations

from hashlib import sha256

import pytest

from market_impact_agent.model_json import (
    JSON_REPAIR_VERSION,
    MODEL_JSON_REPAIR_POLICY_ID,
    load_model_json,
)


def test_standard_json_uses_compatible_direct_parser_without_repair() -> None:
    result = load_model_json('{"value":1,"items":["a","b"]}')

    assert result.value == {"value": 1, "items": ["a", "b"]}
    assert result.evidence.parser_id == f"json-repair-{JSON_REPAIR_VERSION}"
    assert result.evidence.policy_id == MODEL_JSON_REPAIR_POLICY_ID
    assert result.evidence.source_was_strict_json is True
    assert result.evidence.repair_applied is False
    assert result.evidence.structural_edits == ()


def test_one_extra_closing_bracket_is_repaired_without_semantic_change() -> None:
    result = load_model_json('{"value":[1,2]],"confidence":0.7}')

    assert result.value == {"value": [1, 2], "confidence": 0.7}
    assert result.evidence.source_was_strict_json is False
    assert result.evidence.repair_applied is True
    assert result.evidence.structural_edits == (
        {"operation": "delete", "token": "]", "punctuation_index": 5},
    )


def test_one_missing_comma_is_repaired_without_semantic_change() -> None:
    result = load_model_json('{"left":1 "right":2}')

    assert result.value == {"left": 1, "right": 2}
    assert result.evidence.structural_edits == (
        {"operation": "insert", "token": ",", "punctuation_index": 2},
    )


@pytest.mark.parametrize(
    "content",
    (
        "{'value': 1}",
        '{"value":None}',
        '{"value":.5}',
        '{"a":[1,2]],"b":[3,4]]}',
    ),
)
def test_semantic_or_multi_edit_repair_is_rejected(content: str) -> None:
    with pytest.raises(ValueError):
        load_model_json(content)


def test_parser_version_drift_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def drifted_version(_: str) -> str:
        return "0.63.5"

    monkeypatch.setattr("market_impact_agent.model_json.version", drifted_version)

    with pytest.raises(RuntimeError, match="parser version drifted"):
        load_model_json('{"value":1}')


@pytest.mark.parametrize("wrapper", ["```json", "```"])
def test_whole_answer_fence_preserves_original_evidence(wrapper: str) -> None:
    source = f'  \n{wrapper}\n{{"fact":"18 hours","value":1}}\n```\n'
    parsed = load_model_json(source)
    assert parsed.value == {"fact": "18 hours", "value": 1}
    assert parsed.evidence.wrapper_removed
    assert parsed.evidence.source_content_hash == sha256(source.encode()).hexdigest()
    assert not parsed.evidence.repair_applied


@pytest.mark.parametrize(
    "source",
    [
        '<think>answer</think>\n```json\n{"x":1}\n```',
        'Here is JSON:\n```json\n{"x":1}\n```',
        '```json\n{"x":1}\n```\n```json\n{"x":2}\n```',
        '```json\n{"x":1}',
    ],
)
def test_wrapper_normalization_never_mines_reasoning_or_prose(source: str) -> None:
    with pytest.raises(ValueError):
        load_model_json(source)
