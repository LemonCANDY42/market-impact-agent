from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from market_impact_agent.account_state import PositionSnapshot
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.domain import require_aware

AUTHORIZED_DECISION_VIEW_SCHEMA = "market-impact.authorized-decision-view.v1"
POSITION_SNAPSHOT_TOOL_NAME = "read_position_snapshot"
POSITION_SNAPSHOT_TOOL_CAPABILITY = "account.read"


@dataclass(frozen=True, slots=True)
class AuthorizedDecisionView:
    view_id: str
    cutoff: datetime
    frozen_at: datetime
    data_snapshot_ids: tuple[str, ...]
    decision_input_ids: tuple[str, ...]
    position_snapshot_id: str
    position_snapshot_tool_manifest_hash: str
    observation_gaps: tuple[str, ...]
    risk_observation_ready: bool
    exposure_increase_ready: bool
    execution_capability: bool = False
    schema_version: str = AUTHORIZED_DECISION_VIEW_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != AUTHORIZED_DECISION_VIEW_SCHEMA:
            raise ValueError("unsupported Authorized Decision View schema")
        _strict_utc(self.cutoff, "Authorized Decision View cutoff")
        _strict_utc(self.frozen_at, "Authorized Decision View frozen_at")
        if self.cutoff > self.frozen_at:
            raise ValueError("Authorized Decision View cutoff must not be after frozen_at")
        _sorted_unique(self.data_snapshot_ids, "data_snapshot_ids")
        _sorted_unique(self.decision_input_ids, "decision_input_ids")
        _prefixed_hash(self.position_snapshot_id, "position-snapshot-", "position_snapshot_id")
        _sha256(self.position_snapshot_tool_manifest_hash, "position tool manifest hash")
        _sorted_unique(self.observation_gaps, "observation_gaps")
        if self.exposure_increase_ready and not self.risk_observation_ready:
            raise ValueError("exposure increase requires risk-observation readiness")
        if self.exposure_increase_ready and self.observation_gaps:
            raise ValueError("exposure increase requires a gap-free Authorized Decision View")
        if self.execution_capability:
            raise ValueError("Authorized Decision View must never grant execution capability")
        if self.view_id != self.expected_view_id:
            raise ValueError("Authorized Decision View ID does not match content")

    @property
    def expected_view_id(self) -> str:
        return f"authorized-decision-view-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "cutoff": _timestamp(self.cutoff),
            "frozen_at": _timestamp(self.frozen_at),
            "data_snapshot_ids": list(self.data_snapshot_ids),
            "decision_input_ids": list(self.decision_input_ids),
            "position_snapshot_id": self.position_snapshot_id,
            "position_snapshot_tool_manifest_hash": self.position_snapshot_tool_manifest_hash,
            "observation_gaps": list(self.observation_gaps),
            "risk_observation_ready": self.risk_observation_ready,
            "exposure_increase_ready": self.exposure_increase_ready,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "view_id": self.view_id}

    @classmethod
    def build(
        cls,
        *,
        cutoff: datetime,
        frozen_at: datetime,
        data_snapshot_ids: tuple[str, ...],
        decision_input_ids: tuple[str, ...],
        position_snapshot: PositionSnapshot,
    ) -> AuthorizedDecisionView:
        require_aware(cutoff, "Authorized Decision View cutoff")
        require_aware(frozen_at, "Authorized Decision View frozen_at")
        cutoff = cutoff.astimezone(UTC)
        frozen_at = frozen_at.astimezone(UTC)
        if cutoff > frozen_at:
            raise ValueError("Authorized Decision View cutoff must not be after frozen_at")
        if position_snapshot.as_of > cutoff:
            raise ValueError("Position Snapshot must not be newer than the decision cutoff")
        if position_snapshot.reconciled_at > cutoff:
            raise ValueError("Position Snapshot must be reconciled by the decision cutoff")
        if position_snapshot.evaluated_at > frozen_at:
            raise ValueError("Position Snapshot must be evaluated by the view freeze time")
        observation_gaps = set(position_snapshot.observation_gaps)
        if cutoff - position_snapshot.as_of > timedelta(seconds=position_snapshot.max_age_seconds):
            observation_gaps.add("stale")
        ordered_gaps = tuple(sorted(observation_gaps))
        exposure_increase_ready = position_snapshot.risk_observation_ready and not ordered_gaps
        position_snapshot_tool = _build_position_snapshot_tool(position_snapshot)
        ordered_data = tuple(sorted(set(data_snapshot_ids)))
        ordered_inputs = tuple(sorted(set(decision_input_ids)))
        core = {
            "schema_version": AUTHORIZED_DECISION_VIEW_SCHEMA,
            "cutoff": _timestamp(cutoff),
            "frozen_at": _timestamp(frozen_at),
            "data_snapshot_ids": list(ordered_data),
            "decision_input_ids": list(ordered_inputs),
            "position_snapshot_id": position_snapshot.snapshot_id,
            "position_snapshot_tool_manifest_hash": position_snapshot_tool.manifest_hash,
            "observation_gaps": list(ordered_gaps),
            "risk_observation_ready": position_snapshot.risk_observation_ready,
            "exposure_increase_ready": exposure_increase_ready,
            "execution_capability": False,
        }
        return cls(
            view_id=f"authorized-decision-view-{canonical_hash(core)}",
            cutoff=cutoff,
            frozen_at=frozen_at,
            data_snapshot_ids=ordered_data,
            decision_input_ids=ordered_inputs,
            position_snapshot_id=position_snapshot.snapshot_id,
            position_snapshot_tool_manifest_hash=position_snapshot_tool.manifest_hash,
            observation_gaps=ordered_gaps,
            risk_observation_ready=position_snapshot.risk_observation_ready,
            exposure_increase_ready=exposure_increase_ready,
        )


def authorized_decision_view_from_dict(value: object) -> AuthorizedDecisionView:
    if not isinstance(value, dict):
        raise TypeError("Authorized Decision View must be a JSON object")
    payload = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in payload):
        raise TypeError("Authorized Decision View field names must be strings")
    fields = cast(dict[str, object], payload)
    expected = {
        "schema_version",
        "view_id",
        "cutoff",
        "frozen_at",
        "data_snapshot_ids",
        "decision_input_ids",
        "position_snapshot_id",
        "position_snapshot_tool_manifest_hash",
        "observation_gaps",
        "risk_observation_ready",
        "exposure_increase_ready",
        "execution_capability",
    }
    if fields.keys() != expected:
        missing = sorted(expected - fields.keys())
        unknown = sorted(fields.keys() - expected)
        raise ValueError(
            "Authorized Decision View fields differ from the contract: "
            f"missing={missing}, unknown={unknown}"
        )
    view = AuthorizedDecisionView(
        view_id=_string(fields, "view_id"),
        cutoff=_datetime(fields, "cutoff"),
        frozen_at=_datetime(fields, "frozen_at"),
        data_snapshot_ids=_string_tuple(fields, "data_snapshot_ids"),
        decision_input_ids=_string_tuple(fields, "decision_input_ids"),
        position_snapshot_id=_string(fields, "position_snapshot_id"),
        position_snapshot_tool_manifest_hash=_string(
            fields, "position_snapshot_tool_manifest_hash"
        ),
        observation_gaps=_string_tuple(fields, "observation_gaps"),
        risk_observation_ready=_bool(fields, "risk_observation_ready"),
        exposure_increase_ready=_bool(fields, "exposure_increase_ready"),
        execution_capability=_bool(fields, "execution_capability"),
        schema_version=_string(fields, "schema_version"),
    )
    if view.to_dict() != fields:
        raise ValueError("Authorized Decision View does not match canonical serialization")
    return view


def build_position_snapshot_tool(
    view: AuthorizedDecisionView,
    position_snapshot: PositionSnapshot,
) -> ToolDescriptor:
    """Mint the only account-read tool accepted for this exact durable view."""

    if position_snapshot.snapshot_id != view.position_snapshot_id:
        raise ValueError("Position Snapshot tool input differs from its Authorized Decision View")
    tool = _build_position_snapshot_tool(position_snapshot)
    if tool.manifest_hash != view.position_snapshot_tool_manifest_hash:
        raise ValueError(
            "Position Snapshot tool manifest differs from its Authorized Decision View"
        )
    return tool


def _build_position_snapshot_tool(position_snapshot: PositionSnapshot) -> ToolDescriptor:
    async def read_position_snapshot(arguments: dict[str, object]) -> object:
        if arguments:
            raise ValueError("read_position_snapshot takes no arguments")
        return position_snapshot.to_dict()

    return ToolDescriptor(
        name=POSITION_SNAPSHOT_TOOL_NAME,
        version="1.0.0",
        description=(
            "Read the exact credential-free account, position, open-order, fill, and gap snapshot "
            "frozen for this decision."
        ),
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        required_capabilities=frozenset({POSITION_SNAPSHOT_TOOL_CAPABILITY}),
        side_effect=ToolSideEffect.READ_ONLY,
        timeout_seconds=1.0,
        max_result_bytes=262_144,
        handler=read_position_snapshot,
    )


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")
    if any(not value or value != value.strip() for value in values):
        raise ValueError(f"{name} values must be non-empty and trimmed")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _string(fields: dict[str, object], name: str) -> str:
    value = fields[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _string_tuple(fields: dict[str, object], name: str) -> tuple[str, ...]:
    value = fields[name]
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    raw_items = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw_items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], raw_items))


def _datetime(fields: dict[str, object], name: str) -> datetime:
    value = _string(fields, name)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(parsed, name)
    return parsed


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _bool(fields: dict[str, object], name: str) -> bool:
    value = fields[name]
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value
