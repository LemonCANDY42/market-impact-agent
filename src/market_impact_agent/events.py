from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from typing import cast

from market_impact_agent.domain import require_aware

_DIRECTNESS_BY_POSITION = ("direct", "second_order", "third_order", "fourth_order")


def event_transmission_chronology_errors(payload: object) -> tuple[str, ...]:
    """Validate cross-field point-in-time rules that JSON Schema cannot express."""
    fields = _object(payload, "event assessment")
    envelope = _object_field(fields, "envelope")
    as_of = _timestamp_field(envelope, "as_of")
    evidence = _object_array(envelope, "evidence")

    errors: list[str] = []
    if fields.get("event_id") != envelope.get("event_id"):
        errors.append("event_id must match envelope.event_id")

    evidence_ids: list[str] = []
    for index, item in enumerate(evidence):
        prefix = f"envelope.evidence[{index}]"
        evidence_id = _string_field(item, "evidence_id")
        evidence_ids.append(evidence_id)
        claim = _string_field(item, "claim")
        claim_hash = _string_field(item, "claim_hash")
        published_at = _timestamp_field(item, "published_at")
        visible_at = _timestamp_field(item, "visible_at")
        retrieved_at = _timestamp_field(item, "retrieved_at")
        _timestamp_field(item, "occurred_at")

        if published_at > visible_at:
            errors.append(f"{prefix}.published_at must not be after visible_at")
        if visible_at > retrieved_at:
            errors.append(f"{prefix}.visible_at must not be after retrieved_at")
        if visible_at > as_of:
            errors.append(f"{prefix}.visible_at must not be after envelope.as_of")
        if item.get("supersedes_id") == evidence_id:
            errors.append(f"{prefix} must not supersede itself")
        if claim_hash != sha256(claim.encode()).hexdigest():
            errors.append(f"{prefix}.claim_hash must match claim")

    if len(evidence_ids) != len(set(evidence_ids)):
        errors.append("envelope evidence_id values must be unique")

    known_evidence = set(evidence_ids)
    expectation_delta = _object_field(fields, "expectation_delta")
    baseline_ref = _nullable_string_field(expectation_delta, "baseline_source_ref")
    expected = _nullable_string_field(expectation_delta, "expected")
    observed = _nullable_string_field(expectation_delta, "observed")
    direction = _string_field(expectation_delta, "direction")
    if direction != "unknown" and (baseline_ref is None or expected is None or observed is None):
        errors.append(
            "known expectation_delta requires non-null baseline_source_ref, expected, and observed"
        )
    if baseline_ref is not None and baseline_ref not in known_evidence:
        errors.append("expectation_delta has an unknown baseline_source_ref")

    routing = _object_field(fields, "routing")
    mode = _string_field(routing, "mode")
    max_depth = _positive_integer_field(routing, "max_depth")
    max_branches = _positive_integer_field(routing, "max_branches")
    if mode == "fast" and max_depth > 2:
        errors.append("fast routing.max_depth must not exceed 2")
    paths = _object_array(fields, "transmission_paths")
    if len(paths) > max_branches:
        errors.append("transmission_paths exceed routing.max_branches")

    path_ids: list[str] = []
    for path_index, path in enumerate(paths):
        path_prefix = f"transmission_paths[{path_index}]"
        path_ids.append(_string_field(path, "path_id"))
        steps = _object_array(path, "steps")
        if len(steps) > max_depth:
            errors.append(f"{path_prefix}.steps exceed routing.max_depth")
        if len(steps) > len(_DIRECTNESS_BY_POSITION):
            errors.append(f"{path_prefix}.steps must not exceed fourth-order directness")
        referenced = _string_array(path, "counterevidence_refs")
        previous_to: str | None = None
        for step_index, step in enumerate(steps):
            step_prefix = f"{path_prefix}.steps[{step_index}]"
            from_node = _string_field(step, "from")
            if previous_to is not None and from_node != previous_to:
                errors.append(f"{step_prefix}.from must match the previous step.to")
            previous_to = _string_field(step, "to")
            if step_index < len(_DIRECTNESS_BY_POSITION):
                expected_directness = _DIRECTNESS_BY_POSITION[step_index]
                if _string_field(step, "directness") != expected_directness:
                    errors.append(
                        f"{step_prefix}.directness must be {expected_directness} for its position"
                    )
            referenced.extend(_string_array(step, "evidence_refs"))
        unknown = sorted(set(referenced) - known_evidence)
        if unknown:
            errors.append(f"{path_prefix} has unknown evidence references: {', '.join(unknown)}")

    if len(path_ids) != len(set(path_ids)):
        errors.append("transmission path_id values must be unique")
    return tuple(errors)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a JSON object")
    raw = cast(dict[object, object], value)
    result: dict[str, object] = {}
    for key, item in raw.items():
        if not isinstance(key, str):
            raise TypeError(f"{name} field names must be strings")
        result[key] = item
    return result


def _object_field(payload: dict[str, object], name: str) -> dict[str, object]:
    return _object(payload.get(name), name)


def _object_array(payload: dict[str, object], name: str) -> list[dict[str, object]]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return [_object(item, f"{name} item") for item in cast(list[object], value)]


def _string_field(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _nullable_string_field(payload: dict[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a string or null")
    return value


def _string_array(payload: dict[str, object], name: str) -> list[str]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} items must be strings")
    return cast(list[str], items)


def _positive_integer_field(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise TypeError(f"{name} must be a positive integer")
    return value


def _timestamp_field(payload: dict[str, object], name: str) -> datetime:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a date-time string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid date-time") from exc
    require_aware(parsed, name)
    return parsed
