from __future__ import annotations

from datetime import datetime
from typing import cast

from market_impact_agent.domain import require_aware


def event_transmission_chronology_errors(payload: object) -> tuple[str, ...]:
    """Validate point-in-time chronology that JSON Schema cannot express."""
    if not isinstance(payload, dict):
        raise TypeError("event transmission record must be a JSON object")
    fields = cast(dict[object, object], payload)

    event_time = _timestamp_field(fields, "event_time")
    first_publication_time = _timestamp_field(fields, "first_publication_time")
    as_of = _timestamp_field(fields, "as_of")

    errors: list[str] = []
    if event_time > as_of:
        errors.append("event_time must not be after as_of")
    if first_publication_time > as_of:
        errors.append("first_publication_time must not be after as_of")
    return tuple(errors)


def _timestamp_field(payload: dict[object, object], name: str) -> datetime:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a date-time string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid date-time") from exc
    require_aware(parsed, name)
    return parsed
