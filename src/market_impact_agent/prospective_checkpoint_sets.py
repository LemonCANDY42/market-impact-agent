from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.checkpoint_decision_inputs import project_checkpoint_observation
from market_impact_agent.data_inputs import (
    DataPITLane,
    DataSourceBinding,
    FrozenDataSnapshotInput,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
)
from market_impact_agent.prospective_diagnostic import (
    REQUIRED_DIAGNOSTIC_CAPABILITIES,
    CapabilityApplicability,
    ProspectiveDiagnosticRegistration,
)
from market_impact_agent.source_acceptance import SourceRouteAcceptanceReport

PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA = "market-impact.prospective-checkpoint-snapshot-set.v2"

_TOOL_NAMES = {
    ObservationCapability.EVENT_REVELATION: "lookup_event_revelation",
    ObservationCapability.PRIOR_EXPECTATION: "lookup_prior_expectation",
    ObservationCapability.MARKET_CONTEXT: "lookup_market_context",
    ObservationCapability.EXPOSURE_CANDIDATES: "lookup_exposure_candidates",
    ObservationCapability.POSITIONING: "lookup_positioning",
    ObservationCapability.MACRO_VINTAGE: "lookup_macro_vintage",
}

_FILTER_FIELDS = {
    ObservationCapability.EVENT_REVELATION: (
        "event_type",
        "headline",
        "industry",
        "instrument_code",
        "publisher",
    ),
    ObservationCapability.PRIOR_EXPECTATION: (
        "analyst",
        "instrument_code",
        "publisher",
        "report_date",
    ),
    ObservationCapability.MARKET_CONTEXT: (
        "index_code",
        "instrument_code",
        "market",
        "trade_date",
    ),
    ObservationCapability.EXPOSURE_CANDIDATES: (
        "index_code",
        "industry_code",
        "industry_name",
        "instrument_class",
        "instrument_code",
        "venue",
    ),
    ObservationCapability.POSITIONING: (
        "instrument_code",
        "market",
        "trade_date",
    ),
    ObservationCapability.MACRO_VINTAGE: (
        "indicator",
        "publisher",
        "reference_period",
        "release_date",
    ),
}

_FILTER_FIELD_ALIASES = {
    "analyst": ("analyst", "author_name"),
    "event_type": ("event_type", "channels"),
    "headline": ("headline", "title"),
    "index_code": ("index_code", "ts_code"),
    "indicator": ("indicator", "data_api"),
    "industry": ("industry", "industry_name"),
    "instrument_code": ("instrument_code", "ts_code"),
    "market": ("market", "exchange", "exchange_id"),
    "publisher": ("publisher", "upstream_publisher"),
    "reference_period": ("reference_period", "month", "quarter"),
    "release_date": ("release_date", "publish_date"),
    "venue": ("venue", "exchange"),
}


@dataclass(frozen=True, slots=True)
class CheckpointRouteSelection:
    capability: ObservationCapability
    route_kind: str
    snapshot_id: str
    collection_policy_id: str
    source_acceptance_report_id: str

    def __post_init__(self) -> None:
        if self.capability not in REQUIRED_DIAGNOSTIC_CAPABILITIES:
            raise ValueError("checkpoint route capability is outside the diagnostic contract")
        _trimmed(self.route_kind, "checkpoint route kind")
        _prefixed(self.snapshot_id, "data-snapshot-", "checkpoint route snapshot_id")
        _prefixed(
            self.collection_policy_id,
            "prospective-collection-policy-",
            "checkpoint route collection_policy_id",
        )
        _prefixed(
            self.source_acceptance_report_id,
            "source-route-acceptance-report-",
            "checkpoint route source_acceptance_report_id",
        )


@dataclass(frozen=True, slots=True)
class CheckpointRouteReconciliation:
    route_kind: str
    snapshot_id: str
    collection_policy_id: str
    source_acceptance_report_id: str
    provider_id: str
    provider_version: str
    upstream_source: str
    provider_manifest_hash: str
    source_config_hash: str
    raw_response_hash: str

    def to_dict(self) -> dict[str, str]:
        return {
            "route_kind": self.route_kind,
            "snapshot_id": self.snapshot_id,
            "collection_policy_id": self.collection_policy_id,
            "source_acceptance_report_id": self.source_acceptance_report_id,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "upstream_source": self.upstream_source,
            "provider_manifest_hash": self.provider_manifest_hash,
            "source_config_hash": self.source_config_hash,
            "raw_response_hash": self.raw_response_hash,
        }


@dataclass(frozen=True, slots=True)
class CheckpointToolManifest:
    name: str
    version: str
    snapshot_ids: tuple[str, ...]
    allowed_filter_fields: tuple[str, ...]
    side_effect: str = "read_only"

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "version": self.version,
            "snapshot_ids": list(self.snapshot_ids),
            "allowed_filter_fields": list(self.allowed_filter_fields),
            "side_effect": self.side_effect,
        }


@dataclass(frozen=True, slots=True)
class CheckpointCapabilityBinding:
    capability: ObservationCapability
    applicability: CapabilityApplicability
    not_applicable_reason: str | None
    routes: tuple[CheckpointRouteReconciliation, ...]
    tool_manifest: CheckpointToolManifest | None

    def to_dict(self) -> dict[str, object]:
        return {
            "capability": self.capability.value,
            "applicability": self.applicability.value,
            "not_applicable_reason": self.not_applicable_reason,
            "routes": [item.to_dict() for item in self.routes],
            "tool_manifest": (None if self.tool_manifest is None else self.tool_manifest.to_dict()),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveCheckpointSnapshotSet:
    snapshot_set_id: str
    registration_id: str
    checkpoint_key: str
    barrier_at: datetime
    reconciled_at: datetime
    capability_bindings: tuple[CheckpointCapabilityBinding, ...]
    authorized_snapshot_ids: tuple[str, ...]
    complete: bool
    historical_pit_claim: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA:
            raise ValueError("unsupported prospective checkpoint Snapshot Set schema")
        _prefixed(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "checkpoint set registration_id",
        )
        _trimmed(self.checkpoint_key, "checkpoint set checkpoint_key")
        _strict_utc(self.barrier_at, "checkpoint set barrier_at")
        _strict_utc(self.reconciled_at, "checkpoint set reconciled_at")
        if self.reconciled_at < self.barrier_at:
            raise ValueError("checkpoint set cannot reconcile before its barrier")
        capabilities = tuple(item.capability for item in self.capability_bindings)
        if frozenset(capabilities) != REQUIRED_DIAGNOSTIC_CAPABILITIES or len(capabilities) != len(
            REQUIRED_DIAGNOSTIC_CAPABILITIES
        ):
            raise ValueError("checkpoint set must bind the exact diagnostic capability set")
        if self.authorized_snapshot_ids != tuple(sorted(set(self.authorized_snapshot_ids))):
            raise ValueError("checkpoint set Snapshot IDs must be sorted and unique")
        expected_ids = tuple(
            sorted(
                {
                    route.snapshot_id
                    for binding in self.capability_bindings
                    for route in binding.routes
                }
            )
        )
        if self.authorized_snapshot_ids != expected_ids:
            raise ValueError("checkpoint set authorized Snapshot IDs do not reconcile")
        expected_complete = all(
            (
                binding.applicability is CapabilityApplicability.NOT_APPLICABLE
                and not binding.routes
                and binding.tool_manifest is None
            )
            or (
                binding.applicability is CapabilityApplicability.REQUIRED
                and bool(binding.routes)
                and binding.tool_manifest is not None
            )
            for binding in self.capability_bindings
        )
        if not self.complete or not expected_complete:
            raise ValueError("checkpoint set requires complete registered capability coverage")
        if self.historical_pit_claim or self.execution_capability:
            raise ValueError("checkpoint set cannot grant historical PIT or execution authority")
        if self.snapshot_set_id != self.expected_snapshot_set_id:
            raise ValueError("checkpoint Snapshot Set ID does not match content")

    @property
    def frozen_input(self) -> FrozenDataSnapshotInput:
        return FrozenDataSnapshotInput(
            authorized_snapshot_ids=frozenset(self.authorized_snapshot_ids)
        )

    @property
    def expected_snapshot_set_id(self) -> str:
        return f"prospective-checkpoint-snapshot-set-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "barrier_at": _timestamp(self.barrier_at),
            "reconciled_at": _timestamp(self.reconciled_at),
            "capability_bindings": [item.to_dict() for item in self.capability_bindings],
            "authorized_snapshot_ids": list(self.authorized_snapshot_ids),
            "complete": self.complete,
            "historical_pit_claim": self.historical_pit_claim,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_set_id": self.snapshot_set_id}


def reconcile_prospective_checkpoint_snapshot_set(
    *,
    registration: ProspectiveDiagnosticRegistration,
    checkpoint_key: str,
    barrier_at: datetime,
    selections: tuple[CheckpointRouteSelection, ...],
    store: LocalDataSnapshotStore,
    journal: ProspectiveDataJournal,
    policies: Mapping[str, ProspectiveCollectionPolicy],
    acceptance_reports: Mapping[str, SourceRouteAcceptanceReport],
    reconciled_at: datetime,
) -> ProspectiveCheckpointSnapshotSet:
    _strict_utc(barrier_at, "checkpoint reconciliation barrier_at")
    _strict_utc(reconciled_at, "checkpoint reconciliation reconciled_at")
    if barrier_at <= registration.registered_at:
        raise ValueError("checkpoint barrier must follow prospective registration")
    if reconciled_at < barrier_at:
        raise ValueError("checkpoint reconciliation cannot precede the barrier")
    checkpoint = registration.checkpoint(checkpoint_key)
    selection_keys = tuple((item.capability, item.route_kind) for item in selections)
    if len(selection_keys) != len(set(selection_keys)):
        raise ValueError("checkpoint route selections must be unique")

    bindings: list[CheckpointCapabilityBinding] = []
    for capability in sorted(REQUIRED_DIAGNOSTIC_CAPABILITIES, key=lambda item: item.value):
        slot = checkpoint.slot(capability)
        selected = tuple(item for item in selections if item.capability is capability)
        if slot.applicability is CapabilityApplicability.NOT_APPLICABLE:
            if selected:
                raise ValueError("not_applicable slot cannot receive post-hoc route selections")
            bindings.append(
                CheckpointCapabilityBinding(
                    capability=capability,
                    applicability=slot.applicability,
                    not_applicable_reason=slot.not_applicable_reason,
                    routes=(),
                    tool_manifest=None,
                )
            )
            continue
        if {item.route_kind for item in selected} != set(slot.required_route_kinds):
            raise ValueError("checkpoint selections do not match registered route kinds")

        reconciled_routes: list[CheckpointRouteReconciliation] = []
        observation_ids: set[str] = set()
        latest_available_at: datetime | None = None
        source_keys: set[str] = set()
        selected_source_identities_by_snapshot: dict[str, set[tuple[str, ...]]] = {}
        snapshot_source_identities: dict[str, set[tuple[str, ...]]] = {}
        for selection in selected:
            policy = policies.get(selection.collection_policy_id)
            if policy is None or policy.policy_id != selection.collection_policy_id:
                raise ValueError("checkpoint selection collection policy is unavailable")
            if policy.capability is not capability:
                raise ValueError("checkpoint collection policy capability mismatch")
            if policy.poll_interval_seconds > slot.poll_interval_seconds:
                raise ValueError("checkpoint collection policy polling cadence is too slow")
            if policy.maximum_gap_seconds > slot.maximum_gap_seconds:
                raise ValueError("checkpoint collection policy maximum gap is too large")
            snapshot = store.get(selection.snapshot_id)
            journal.assert_frozen_snapshot(snapshot)
            if snapshot.query.source_policy_id != policy.policy_id:
                raise ValueError("checkpoint Snapshot and collection policy do not match")
            if snapshot.query.sources != policy.sources:
                raise ValueError("checkpoint Snapshot and collection policy sources do not match")
            if snapshot.query.capability is not capability:
                raise ValueError("checkpoint Snapshot capability mismatch")
            if snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
                raise ValueError("checkpoint Snapshot must use prospective actual receipts")
            if not snapshot.coverage_complete:
                raise ValueError("checkpoint Snapshot coverage is incomplete")
            if snapshot.query.parameters.get("requested_not_after") != _timestamp(barrier_at):
                raise ValueError("checkpoint Snapshot barrier does not match registration")
            if snapshot.query.as_of > barrier_at:
                raise ValueError("checkpoint Snapshot effective cutoff exceeds its barrier")

            report = acceptance_reports.get(selection.source_acceptance_report_id)
            if report is None or not report.accepted:
                raise ValueError("checkpoint selection requires an accepted route report")
            if report.evaluated_at > reconciled_at:
                raise ValueError("accepted route report postdates checkpoint reconciliation")
            declaration = report.declaration
            if declaration.capability is not capability:
                raise ValueError("accepted route capability mismatch")
            matching_sources = tuple(
                item
                for item in snapshot.query.sources
                if item.provider_id == declaration.provider_id
                and item.provider_version == declaration.provider_version
                and item.upstream_source == declaration.upstream_source
                and item.manifest_hash == declaration.provider_manifest_hash
                and item.source_config_hash == declaration.source_config_hash
            )
            if len(matching_sources) != 1:
                raise ValueError("accepted route identity is not bound exactly once")
            selected_source_identities_by_snapshot.setdefault(snapshot.snapshot_id, set()).add(
                _source_identity(matching_sources[0])
            )
            snapshot_source_identities[snapshot.snapshot_id] = {
                _source_identity(item) for item in snapshot.query.sources
            }
            matching_attempts = tuple(
                item
                for item in snapshot.attempts
                if item.provider_id == declaration.provider_id
                and item.provider_version == declaration.provider_version
                and item.upstream_source == declaration.upstream_source
            )
            if len(matching_attempts) != 1:
                raise ValueError("checkpoint route raw response hash is unavailable")
            raw_response_hash = matching_attempts[0].raw_response_hash
            if raw_response_hash is None:
                raise ValueError("checkpoint route raw response hash is unavailable")
            matching_observations = tuple(
                item
                for item in snapshot.observations
                if item.provider_id == declaration.provider_id
                and item.provider_version == declaration.provider_version
                and item.upstream_source == declaration.upstream_source
            )
            if not matching_observations:
                raise ValueError("each selected route requires an observation at the barrier")
            route_latest_available_at: datetime | None = None
            for observation in matching_observations:
                observation_ids.add(observation.observation_id)
                available_at = observation.times.available_at
                if available_at is None or available_at > barrier_at:
                    raise ValueError("checkpoint observation is not visible at the barrier")
                if route_latest_available_at is None or available_at > route_latest_available_at:
                    route_latest_available_at = available_at
                if latest_available_at is None or available_at > latest_available_at:
                    latest_available_at = available_at
            if (
                route_latest_available_at is None
                or (barrier_at - route_latest_available_at).total_seconds()
                > slot.maximum_age_seconds
            ):
                raise ValueError("each selected route requires a fresh observation at the barrier")
            source_keys.add(matching_sources[0].source_key)
            reconciled_routes.append(
                CheckpointRouteReconciliation(
                    route_kind=selection.route_kind,
                    snapshot_id=snapshot.snapshot_id,
                    collection_policy_id=policy.policy_id,
                    source_acceptance_report_id=report.report_id,
                    provider_id=declaration.provider_id,
                    provider_version=declaration.provider_version,
                    upstream_source=declaration.upstream_source,
                    provider_manifest_hash=declaration.provider_manifest_hash,
                    source_config_hash=declaration.source_config_hash,
                    raw_response_hash=raw_response_hash,
                )
            )
        if any(
            selected_source_identities_by_snapshot[snapshot_id] != identities
            for snapshot_id, identities in snapshot_source_identities.items()
        ):
            raise ValueError("checkpoint Snapshot contains unselected or unaccepted sources")
        if len(source_keys) < slot.minimum_data_sources:
            raise ValueError("checkpoint source diversity minimum is not met")
        if len(observation_ids) < slot.minimum_observations:
            raise ValueError("checkpoint observation minimum is not met")
        if (
            latest_available_at is None
            or (barrier_at - latest_available_at).total_seconds() > slot.maximum_age_seconds
        ):
            raise ValueError("checkpoint observations are stale at the barrier")
        snapshot_ids = tuple(sorted({item.snapshot_id for item in reconciled_routes}))
        bindings.append(
            CheckpointCapabilityBinding(
                capability=capability,
                applicability=slot.applicability,
                not_applicable_reason=None,
                routes=tuple(sorted(reconciled_routes, key=lambda item: item.route_kind)),
                tool_manifest=CheckpointToolManifest(
                    name=_TOOL_NAMES[capability],
                    version="2",
                    snapshot_ids=snapshot_ids,
                    allowed_filter_fields=_FILTER_FIELDS[capability],
                ),
            )
        )

    authorized_snapshot_ids = tuple(
        sorted({route.snapshot_id for binding in bindings for route in binding.routes})
    )
    core = {
        "schema_version": PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA,
        "registration_id": registration.registration_id,
        "checkpoint_key": checkpoint_key,
        "barrier_at": _timestamp(barrier_at),
        "reconciled_at": _timestamp(reconciled_at),
        "capability_bindings": [item.to_dict() for item in bindings],
        "authorized_snapshot_ids": list(authorized_snapshot_ids),
        "complete": True,
        "historical_pit_claim": False,
        "execution_capability": False,
    }
    return ProspectiveCheckpointSnapshotSet(
        snapshot_set_id=(f"prospective-checkpoint-snapshot-set-{canonical_hash(core)}"),
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint_key,
        barrier_at=barrier_at,
        reconciled_at=reconciled_at,
        capability_bindings=tuple(bindings),
        authorized_snapshot_ids=authorized_snapshot_ids,
        complete=True,
    )


def build_checkpoint_tool_descriptors(
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    *,
    store: LocalDataSnapshotStore,
    frozen_input: FrozenDataSnapshotInput,
    required_capability: str,
) -> tuple[ToolDescriptor, ...]:
    _trimmed(required_capability, "checkpoint tool required_capability")
    if not set(snapshot_set.authorized_snapshot_ids) <= frozen_input.authorized_snapshot_ids:
        raise ValueError("checkpoint Snapshot is not declared by the enclosing run input")
    descriptors: list[ToolDescriptor] = []
    for binding in snapshot_set.capability_bindings:
        manifest = binding.tool_manifest
        if manifest is None:
            continue
        snapshots = tuple(store.get(item) for item in manifest.snapshot_ids)
        if any(not item.coverage_complete for item in snapshots):
            raise ValueError("checkpoint tool requires complete frozen Snapshots")
        if any(item.query.capability is not binding.capability for item in snapshots):
            raise ValueError("checkpoint tool Snapshot capability mismatch")
        route_kinds_by_source: dict[tuple[str, str, str, str], tuple[str, ...]] = {}
        for route in binding.routes:
            key = (
                route.snapshot_id,
                route.provider_id,
                route.provider_version,
                route.upstream_source,
            )
            route_kinds_by_source[key] = tuple(
                sorted({*route_kinds_by_source.get(key, ()), route.route_kind})
            )

        async def handler(
            arguments: dict[str, object],
            *,
            bound_manifest: CheckpointToolManifest = manifest,
            bound_snapshots: tuple[object, ...] = snapshots,
            bound_route_kinds: dict[
                tuple[str, str, str, str], tuple[str, ...]
            ] = route_kinds_by_source,
            bound_capability: ObservationCapability = binding.capability,
        ) -> object:
            return _handle_checkpoint_tool(
                arguments,
                snapshot_set=snapshot_set,
                manifest=bound_manifest,
                snapshots=bound_snapshots,
                route_kinds_by_source=bound_route_kinds,
                capability=bound_capability,
            )

        descriptors.append(
            ToolDescriptor(
                name=manifest.name,
                version=f"{manifest.version}+{snapshot_set.snapshot_set_id}",
                description=(
                    f"Read-only {binding.capability.value} decision inputs projected from "
                    f"observations frozen for checkpoint {snapshot_set.checkpoint_key}. "
                    "Arguments cannot change the cutoff, sources, policies, or Provider versions."
                ),
                input_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "query": {"type": "string", "minLength": 1},
                        "publisher": {"type": "string", "minLength": 1},
                        "filters": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                item: {"type": "string", "minLength": 1}
                                for item in manifest.allowed_filter_fields
                            },
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    },
                },
                required_capabilities=frozenset({required_capability}),
                side_effect=ToolSideEffect.READ_ONLY,
                timeout_seconds=5.0,
                max_result_bytes=100_000,
                handler=handler,
            )
        )
    return tuple(descriptors)


def _handle_checkpoint_tool(
    arguments: dict[str, object],
    *,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    manifest: CheckpointToolManifest,
    snapshots: tuple[object, ...],
    route_kinds_by_source: Mapping[tuple[str, str, str, str], tuple[str, ...]],
    capability: ObservationCapability,
) -> dict[str, object]:
    from market_impact_agent.data_inputs import DataSnapshot

    if not set(arguments) <= {"query", "publisher", "filters", "limit"}:
        raise ValueError("checkpoint tool arguments contain unsupported fields")
    query = _optional_trimmed(arguments.get("query"), "query")
    publisher = _optional_trimmed(arguments.get("publisher"), "publisher")
    filters_value = arguments.get("filters", {})
    if not isinstance(filters_value, dict):
        raise ValueError("checkpoint tool filters must be an object")
    untyped_filters = cast(dict[object, object], filters_value)
    if any(not isinstance(key, str) for key in untyped_filters):
        raise ValueError("checkpoint tool filters must be an object")
    filters = cast(dict[str, object], untyped_filters)
    if not set(filters) <= set(manifest.allowed_filter_fields):
        raise ValueError("checkpoint tool filter is not allowed for this capability")
    if any(not isinstance(value, str) or not value.strip() for value in filters.values()):
        raise ValueError("checkpoint tool filter values must be non-empty strings")
    limit_value = arguments.get("limit", 20)
    if (
        not isinstance(limit_value, int)
        or isinstance(limit_value, bool)
        or not 1 <= limit_value <= 100
    ):
        raise ValueError("checkpoint tool limit must be between 1 and 100")

    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw_snapshot in snapshots:
        snapshot = cast(DataSnapshot, raw_snapshot)
        for observation in snapshot.observations:
            if observation.observation_id in seen:
                continue
            route_kinds = route_kinds_by_source.get(
                _observation_route_key(snapshot.snapshot_id, observation)
            )
            if route_kinds is None:
                raise ValueError("checkpoint observation source has no selected route")
            projected = project_checkpoint_observation(
                checkpoint_snapshot_set_id=snapshot_set.snapshot_set_id,
                checkpoint_key=snapshot_set.checkpoint_key,
                barrier_at=snapshot_set.barrier_at,
                snapshot_id=snapshot.snapshot_id,
                route_kinds=route_kinds,
                observation=observation,
            )
            searchable_payload = canonical_json_bytes(projected["data"]).decode().casefold()
            if query is not None and query.casefold() not in searchable_payload:
                continue
            if publisher is not None:
                payload_publisher = _payload_field(
                    cast(Mapping[str, object], projected["data"]), "publisher"
                )
                if not isinstance(payload_publisher, str) or (
                    payload_publisher.casefold() != publisher.casefold()
                ):
                    continue
            if any(
                _payload_field(cast(Mapping[str, object], projected["data"]), key) != value
                for key, value in filters.items()
            ):
                continue
            seen.add(observation.observation_id)
            rows.append(projected)
            if len(rows) >= limit_value:
                break
        if len(rows) >= limit_value:
            break
    core = {
        "schema_version": "market-impact.checkpoint-data-tool-result.v2",
        "checkpoint_snapshot_set_id": snapshot_set.snapshot_set_id,
        "registration_id": snapshot_set.registration_id,
        "checkpoint_key": snapshot_set.checkpoint_key,
        "barrier_at": _timestamp(snapshot_set.barrier_at),
        "capability": capability.value,
        "snapshot_ids": list(manifest.snapshot_ids),
        "selection": {
            "query": query,
            "publisher": publisher,
            "filters": filters,
            "limit": limit_value,
        },
        "records": rows,
    }
    return {**core, "result_id": f"checkpoint-data-tool-result-{canonical_hash(core)}"}


def _optional_trimmed(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"checkpoint tool {name} must be a non-empty trimmed string")
    return value


def _payload_field(payload: Mapping[str, object], field: str) -> object | None:
    aliases = _FILTER_FIELD_ALIASES.get(field, (field,))
    for alias in aliases:
        if alias in payload:
            return payload[alias]
    record = payload.get("record")
    if isinstance(record, dict):
        nested = cast(dict[object, object], record)
        for alias in aliases:
            if alias in nested:
                return nested[alias]
    return None


def _source_identity(source: DataSourceBinding) -> tuple[str, ...]:
    return (
        source.provider_id,
        source.provider_version,
        source.upstream_source,
        source.manifest_hash,
        cast(str, source.source_config_hash),
    )


def _observation_route_key(
    snapshot_id: str,
    observation: SourceObservation,
) -> tuple[str, str, str, str]:
    return (
        snapshot_id,
        observation.provider_id,
        observation.provider_version,
        observation.upstream_source,
    )


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _prefixed(value: str, prefix: str, name: str) -> None:
    _trimmed(value, name)
    if not value.startswith(prefix):
        raise ValueError(f"{name} has an invalid identity prefix")
