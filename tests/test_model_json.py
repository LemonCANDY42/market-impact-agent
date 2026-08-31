from __future__ import annotations

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
