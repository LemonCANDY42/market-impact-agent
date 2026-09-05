"""Versioned research-thesis and review-cadence contracts.

The model chooses only from Harness-registered session horizons.  Concrete
timestamps are calculated from a supplied trading calendar; they are never
invented by the model.  Historical five-session Judgments remain separate and
replay unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware


class HorizonBand(StrEnum):
    IMMEDIATE = "immediate"
    TACTICAL = "tactical"
    SWING = "swing"


class BaseCaseDirection(StrEnum):
    UP = "up"
    DOWN = "down"
    RANGEBOUND = "rangebound"


class ReviewCadence(StrEnum):
    ONE_SHOT = "one_shot"
    SCHEDULED = "scheduled"
    MATERIAL_EVENT_DRIVEN = "material_event_driven"


class ReviewTrigger(StrEnum):
    SCHEDULED = "scheduled"
    MATERIAL_EVENT = "material_event"
    INVALIDATION = "invalidation"
    MARKET_THRESHOLD = "market_threshold"
    TERMINAL = "terminal"


HORIZONS_BY_BAND: Mapping[HorizonBand, frozenset[int]] = {
    HorizonBand.IMMEDIATE: frozenset({1, 3}),
    HorizonBand.TACTICAL: frozenset({5, 10}),
    HorizonBand.SWING: frozenset({20, 60}),
}

_SCHEDULED_OFFSETS: Mapping[int, tuple[int, ...]] = {
    1: (1,),
    3: (1, 2, 3),
    5: (1, 3, 5),
    10: (1, 3, 5, 10),
    20: (5, 10, 20),
    60: (5, 10, 20, 40, 60),
}


def validate_horizon(band: HorizonBand, sessions: int) -> None:
    if isinstance(sessions, bool) or sessions not in HORIZONS_BY_BAND[band]:
        allowed = ", ".join(str(item) for item in sorted(HORIZONS_BY_BAND[band]))
        raise ValueError(f"{band.value} horizon must be one of: {allowed}")


def horizon_band_for_sessions(sessions: int) -> HorizonBand:
    """Derive the descriptive band from the Harness-registered horizon."""

    if isinstance(sessions, bool):
        raise ValueError("research thesis chose an unregistered horizon")
    for band, horizons in HORIZONS_BY_BAND.items():
        if sessions in horizons:
            return band
    raise ValueError("research thesis chose an unregistered horizon")


@dataclass(frozen=True, slots=True)
class ResearchThesisV1:
    root_event_id: str
    thesis_epoch: str
    as_of: datetime
    horizon_band: HorizonBand
    primary_horizon_sessions: int
    base_case_direction: BaseCaseDirection
    thesis: str
    priced_in_assessment: str
    transmission: tuple[str, ...]
    counter_scenario: str
    evidence_refs: tuple[str, ...]
    counterevidence_refs: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    review_after_sessions: int
    typed_unknowns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, name in (
            (self.root_event_id, "root_event_id"),
            (self.thesis_epoch, "thesis_epoch"),
            (self.thesis, "thesis"),
            (self.priced_in_assessment, "priced_in_assessment"),
            (self.counter_scenario, "counter_scenario"),
        ):
            _text(value, name)
        require_aware(self.as_of, "research thesis as_of")
        validate_horizon(self.horizon_band, self.primary_horizon_sessions)
        if (
            isinstance(self.review_after_sessions, bool)
            or not 1 <= self.review_after_sessions <= self.primary_horizon_sessions
        ):
            raise ValueError("review_after_sessions must fit the primary horizon")
        _unique_text(self.transmission, "transmission", required=True)
        _unique_text(self.evidence_refs, "evidence_refs", required=True)
        _unique_text(self.counterevidence_refs, "counterevidence_refs")
        _unique_text(self.invalidation_conditions, "invalidation_conditions", required=True)
        _unique_text(self.typed_unknowns, "typed_unknowns")

    @property
    def thesis_id(self) -> str:
        return "research-thesis-v1-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.research-thesis.v1",
            "root_event_id": self.root_event_id,
            "thesis_epoch": self.thesis_epoch,
            "as_of": _timestamp(self.as_of),
            "horizon_band": self.horizon_band.value,
            "primary_horizon_sessions": self.primary_horizon_sessions,
            "base_case_direction": self.base_case_direction.value,
            "thesis": self.thesis,
            "priced_in_assessment": self.priced_in_assessment,
            "transmission": list(self.transmission),
            "counter_scenario": self.counter_scenario,
            "evidence_refs": list(self.evidence_refs),
            "counterevidence_refs": list(self.counterevidence_refs),
            "invalidation_conditions": list(self.invalidation_conditions),
            "review_after_sessions": self.review_after_sessions,
            "typed_unknowns": list(self.typed_unknowns),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "thesis_id": self.thesis_id}


def parse_research_thesis(
    value: object,
    *,
    root_event_id: str,
    thesis_epoch: str,
    as_of: datetime,
    evidence_ids: frozenset[str],
    allowed_horizons: frozenset[int] = frozenset({1, 3, 5, 10, 20, 60}),
) -> ResearchThesisV1:
    """Parse model-authored analysis while injecting all Harness-owned identity."""

    fields = _object(value)
    allowed = {
        "horizon_band",
        "primary_horizon_sessions",
        "base_case_direction",
        "thesis",
        "priced_in_assessment",
        "transmission",
        "counter_scenario",
        "evidence_refs",
        "counterevidence_refs",
        "invalidation_conditions",
        "review_after_sessions",
        "typed_unknowns",
    }
    forbidden = set(fields) - allowed
    if forbidden:
        raise ValueError("research thesis contains unauthorized fields")
    horizon = _integer(fields, "primary_horizon_sessions")
    if horizon not in allowed_horizons:
        raise ValueError("research thesis chose an unregistered horizon")
    # ``horizon_band`` was model-authored in the first v1 candidates. It is a
    # pure projection of the registered session horizon, so current runs no
    # longer ask the model to echo it. Accept a valid legacy value for replay,
    # but keep the Harness-derived value authoritative.
    if "horizon_band" in fields:
        HorizonBand(_required_string(fields, "horizon_band"))
    band = horizon_band_for_sessions(horizon)
    evidence = _string_tuple(fields.get("evidence_refs"), "evidence_refs", required=True)
    counter = _string_tuple(fields.get("counterevidence_refs", []), "counterevidence_refs")
    if not set(evidence + counter) <= evidence_ids:
        raise ValueError("research thesis cites evidence outside the frozen input")
    return ResearchThesisV1(
        root_event_id=root_event_id,
        thesis_epoch=thesis_epoch,
        as_of=as_of,
        horizon_band=band,
        primary_horizon_sessions=horizon,
        base_case_direction=BaseCaseDirection(_required_string(fields, "base_case_direction")),
        thesis=_narrative_string(fields, "thesis"),
        priced_in_assessment=_narrative_string(fields, "priced_in_assessment"),
        transmission=_string_tuple(
            fields.get("transmission"),
            "transmission",
            required=True,
            allow_singleton=True,
            trim_items=True,
        ),
        counter_scenario=_narrative_string(fields, "counter_scenario"),
        evidence_refs=evidence,
        counterevidence_refs=counter,
        invalidation_conditions=_string_tuple(
            fields.get("invalidation_conditions"),
            "invalidation_conditions",
            required=True,
            allow_singleton=True,
            trim_items=True,
        ),
        review_after_sessions=_integer(fields, "review_after_sessions"),
        typed_unknowns=_string_tuple(
            fields.get("typed_unknowns", []), "typed_unknowns", trim_items=True
        ),
    )


def research_thesis_text_normalizations(value: object) -> tuple[dict[str, str], ...]:
    """Describe harmless narrative whitespace normalization without exposing content."""

    fields = _object(value)
    edits: list[dict[str, str]] = []
    for name in ("thesis", "priced_in_assessment", "counter_scenario"):
        item = fields.get(name)
        if isinstance(item, str) and item != item.strip():
            edits.append({"path": name, "operation": "trim_surrounding_whitespace"})
    for name in ("transmission", "invalidation_conditions", "typed_unknowns"):
        item = fields.get(name)
        values: list[object]
        if isinstance(item, str):
            values = [item]
        elif isinstance(item, list):
            values = cast(list[object], item)
        else:
            values = []
        for index, entry in enumerate(values):
            if isinstance(entry, str) and entry != entry.strip():
                edits.append(
                    {
                        "path": f"{name}[{index}]",
                        "operation": "trim_surrounding_whitespace",
                    }
                )
    return tuple(edits)


@dataclass(frozen=True, slots=True)
class ReviewPoint:
    session_offset: int
    session_date: date
    trigger: ReviewTrigger

    def to_dict(self) -> dict[str, object]:
        return {
            "session_offset": self.session_offset,
            "session_date": self.session_date.isoformat(),
            "trigger": self.trigger.value,
        }


@dataclass(frozen=True, slots=True)
class ReviewScheduleV1:
    root_event_id: str
    thesis_epoch: str
    cadence: ReviewCadence
    primary_horizon_sessions: int
    created_at: datetime
    review_points: tuple[ReviewPoint, ...]
    maximum_intermediate_wakes: int

    def __post_init__(self) -> None:
        _text(self.root_event_id, "root_event_id")
        _text(self.thesis_epoch, "thesis_epoch")
        require_aware(self.created_at, "review schedule created_at")
        if self.primary_horizon_sessions not in _SCHEDULED_OFFSETS:
            raise ValueError("review schedule horizon is not registered")
        if self.maximum_intermediate_wakes not in {0, 3}:
            raise ValueError("review schedule wake budget must be zero or three")
        offsets = tuple(point.session_offset for point in self.review_points)
        if offsets != tuple(sorted(set(offsets))):
            raise ValueError("review points must be unique and ordered")
        if not self.review_points or offsets[-1] != self.primary_horizon_sessions:
            raise ValueError("review schedule requires a terminal horizon review")
        if self.review_points[-1].trigger is not ReviewTrigger.TERMINAL:
            raise ValueError("last review point must be terminal")

    @property
    def schedule_id(self) -> str:
        return "decision-review-schedule-v1-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.decision-review-schedule.v1",
            "root_event_id": self.root_event_id,
            "thesis_epoch": self.thesis_epoch,
            "cadence": self.cadence.value,
            "primary_horizon_sessions": self.primary_horizon_sessions,
            "created_at": _timestamp(self.created_at),
            "review_points": [item.to_dict() for item in self.review_points],
            "maximum_intermediate_wakes": self.maximum_intermediate_wakes,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "schedule_id": self.schedule_id}

    @classmethod
    def build(
        cls,
        *,
        root_event_id: str,
        thesis_epoch: str,
        cadence: ReviewCadence,
        primary_horizon_sessions: int,
        future_trading_sessions: tuple[date, ...],
        created_at: datetime,
    ) -> ReviewScheduleV1:
        if len(future_trading_sessions) < primary_horizon_sessions:
            raise ValueError("trading calendar does not cover the thesis horizon")
        selected = future_trading_sessions[:primary_horizon_sessions]
        if selected != tuple(sorted(set(selected))):
            raise ValueError("future trading sessions must be unique and ordered")
        if cadence is ReviewCadence.SCHEDULED:
            offsets = _SCHEDULED_OFFSETS[primary_horizon_sessions]
        else:
            offsets = (primary_horizon_sessions,)
        points = tuple(
            ReviewPoint(
                offset,
                selected[offset - 1],
                ReviewTrigger.TERMINAL
                if offset == primary_horizon_sessions
                else ReviewTrigger.SCHEDULED,
            )
            for offset in offsets
        )
        return cls(
            root_event_id=root_event_id,
            thesis_epoch=thesis_epoch,
            cadence=cadence,
            primary_horizon_sessions=primary_horizon_sessions,
            created_at=created_at,
            review_points=points,
            maximum_intermediate_wakes=(3 if cadence is ReviewCadence.MATERIAL_EVENT_DRIVEN else 0),
        )


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("research thesis must be a JSON object")
    mapping = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in mapping):
        raise TypeError("research thesis must be a JSON object")
    return cast(dict[str, object], mapping)


def _required_string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise ValueError(f"research thesis {key} must be text")
    _text(item, key)
    return item


def _narrative_string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"research thesis {key} must be text")
    return item.strip()


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"research thesis {key} must be an integer")
    return item


def _string_tuple(
    value: object,
    name: str,
    *,
    required: bool = False,
    allow_singleton: bool = False,
    trim_items: bool = False,
) -> tuple[str, ...]:
    if allow_singleton and isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        raise ValueError(f"research thesis {name} must be a string array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise ValueError(f"research thesis {name} must be a string array")
    result = tuple(item.strip() if trim_items else item for item in cast(list[str], items))
    _unique_text(result, name, required=required)
    return result


def _unique_text(values: tuple[str, ...], name: str, *, required: bool = False) -> None:
    if required and not values:
        raise ValueError(f"{name} must not be empty")
    if values != tuple(dict.fromkeys(values)):
        raise ValueError(f"{name} must be unique and retain declared order")
    for value in values:
        _text(value, name)


def _text(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
