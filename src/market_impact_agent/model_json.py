from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from importlib.metadata import version
from typing import cast

import json_repair

from market_impact_agent.agent_contracts import canonical_hash

JSON_REPAIR_VERSION = "0.63.4"
MODEL_JSON_REPAIR_POLICY_ID = "json-repair-0.63.4-answer-wrapper-v2"
_EVIDENCE_SCHEMA = "market-impact.model-json-parse-evidence.v1"
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
_PUNCTUATION = frozenset("{}[],: ") - {" "}


@dataclass(frozen=True, slots=True)
class ModelJsonParseEvidence:
    source_content_hash: str
    parsed_content_hash: str
    source_was_strict_json: bool
    repair_applied: bool
    structural_edits: tuple[dict[str, object], ...]
    wrapper_removed: bool = False
    parser_id: str = f"json-repair-{JSON_REPAIR_VERSION}"
    policy_id: str = MODEL_JSON_REPAIR_POLICY_ID
    schema_version: str = _EVIDENCE_SCHEMA

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "parser_id": self.parser_id,
            "policy_id": self.policy_id,
            "source_content_hash": self.source_content_hash,
            "parsed_content_hash": self.parsed_content_hash,
            "source_was_strict_json": self.source_was_strict_json,
            "repair_applied": self.repair_applied,
            "structural_edits": [dict(item) for item in self.structural_edits],
            "wrapper_removed": self.wrapper_removed,
        }


@dataclass(frozen=True, slots=True)
class ParsedModelJson:
    value: object
    evidence: ModelJsonParseEvidence


@dataclass(frozen=True, slots=True)
class _Tokens:
    semantic: tuple[tuple[str, object], ...]
    punctuation: tuple[str, ...]


def load_model_json(content: str) -> ParsedModelJson:
    """Normalize one whole-answer wrapper, then use the pinned semantics-preserving parser.

    This never extracts an answer from reasoning or surrounding prose. The original
    bytes remain the parse-evidence identity, including whitespace and a code fence.
    """

    source = content
    content = content.strip()
    wrapper_removed = False
    if content.startswith("```"):
        lines = content.splitlines()
        if (
            len(lines) < 3
            or lines[0] not in {"```", "```json"}
            or lines[-1] != "```"
            or any(line.strip().startswith("```") for line in lines[1:-1])
        ):
            raise ValueError("model JSON must contain exactly one whole-answer JSON fence")
        content = "\n".join(lines[1:-1]).strip()
        wrapper_removed = True
    if not content:
        raise ValueError("model JSON content must be non-empty")
    installed = version("json-repair")
    if installed != JSON_REPAIR_VERSION:
        raise RuntimeError(
            f"model JSON parser version drifted: expected {JSON_REPAIR_VERSION}, got {installed}"
        )
    decoded = cast(object, json_repair.loads(content, skip_json_loads=False, strict=True))
    source_was_strict_json = _is_strict_json(content)
    edits: tuple[dict[str, object], ...] = ()
    if not source_was_strict_json:
        repaired = json_repair.repair_json(
            content,
            skip_json_loads=True,
            strict=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        reparsed = json.loads(repaired)
        if reparsed != decoded:
            raise ValueError("json-repair object differs from its repaired JSON text")
        edits = (_single_structural_edit(_tokenize(content), _tokenize(repaired)),)
    evidence = ModelJsonParseEvidence(
        source_content_hash=sha256(source.encode()).hexdigest(),
        parsed_content_hash=canonical_hash(decoded),
        source_was_strict_json=source_was_strict_json,
        repair_applied=not source_was_strict_json,
        structural_edits=edits,
        wrapper_removed=wrapper_removed,
    )
    return ParsedModelJson(value=decoded, evidence=evidence)


def _is_strict_json(content: str) -> bool:
    try:
        json.loads(content)
    except json.JSONDecodeError:
        return False
    return True


def _tokenize(content: str) -> _Tokens:
    semantic: list[tuple[str, object]] = []
    punctuation: list[str] = []
    index = 0
    while index < len(content):
        char = content[index]
        if char.isspace():
            index += 1
            continue
        if char in _PUNCTUATION:
            punctuation.append(char)
            index += 1
            continue
        if char == '"':
            end = _string_end(content, index)
            token = content[index:end]
            try:
                decoded = json.loads(token)
            except json.JSONDecodeError as exc:
                raise ValueError("model JSON repair would alter a string token") from exc
            semantic.append(("string", decoded))
            index = end
            continue
        number = _NUMBER.match(content, index)
        if number is not None:
            token = number.group(0)
            semantic.append(("number", token))
            index = number.end()
            continue
        literal = next(
            (item for item in ("true", "false", "null") if content.startswith(item, index)),
            None,
        )
        if literal is None:
            raise ValueError("model JSON repair requires changing a semantic token")
        semantic.append(("literal", literal))
        index += len(literal)
    return _Tokens(tuple(semantic), tuple(punctuation))


def _string_end(content: str, start: int) -> int:
    escaped = False
    for index in range(start + 1, len(content)):
        char = content[index]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            return index + 1
    raise ValueError("model JSON repair requires closing an unterminated string")


def _single_structural_edit(source: _Tokens, repaired: _Tokens) -> dict[str, object]:
    if source.semantic != repaired.semantic:
        raise ValueError("model JSON repair changed a string, number, literal, or field identity")
    source_tokens = source.punctuation
    repaired_tokens = repaired.punctuation
    if abs(len(source_tokens) - len(repaired_tokens)) != 1:
        raise ValueError("model JSON repair exceeds the single structural edit policy")
    if len(source_tokens) > len(repaired_tokens):
        operation = "delete"
        longer, shorter = source_tokens, repaired_tokens
    else:
        operation = "insert"
        longer, shorter = repaired_tokens, source_tokens
    index = 0
    while index < len(shorter) and longer[index] == shorter[index]:
        index += 1
    if longer[:index] + longer[index + 1 :] != shorter:
        raise ValueError("model JSON repair is not one punctuation insertion or deletion")
    return {
        "operation": operation,
        "token": longer[index],
        "punctuation_index": index,
    }
