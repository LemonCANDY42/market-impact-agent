from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataPITLane, DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability

SOURCE_ROUTE_ACCEPTANCE_DECLARATION_SCHEMA = "market-impact.source-route-acceptance-declaration.v1"
SOURCE_ROUTE_ACCEPTANCE_REPORT_SCHEMA = "market-impact.source-route-acceptance-report.v1"


class SourceAcceptanceGate(StrEnum):
    RIGHTS_AND_IDENTITY = "rights_and_identity"
    TRANSPORT = "transport"
    COMPLETENESS = "completeness"
    TIME_AND_REVISIONS = "time_and_revisions"
    MARKET_SEMANTICS = "market_semantics"
    DETERMINISM_AND_STORAGE = "determinism_and_storage"
    AGENT_ISOLATION = "agent_isolation"


class SourceAcceptanceStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class SourceRightsEvidence:
    evidence_id: str
    source_ref: str
    final_url: str
    retrieved_at: datetime
    raw_content_hash: str
    schema_version: str = "market-impact.source-rights-evidence.v1"

    def __post_init__(self) -> None:
        if self.schema_version != "market-impact.source-rights-evidence.v1":
            raise ValueError("unsupported source rights evidence schema")
        _https_url(self.source_ref, "source rights evidence source_ref")
        _https_url(self.final_url, "source rights evidence final_url")
        _strict_utc(self.retrieved_at, "source rights evidence retrieved_at")
        _sha256(self.raw_content_hash, "source rights evidence raw_content_hash")
        if self.evidence_id != self.expected_evidence_id:
            raise ValueError("source rights evidence_id does not match content")

    @property
    def expected_evidence_id(self) -> str:
        return f"source-rights-evidence-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_ref": self.source_ref,
            "final_url": self.final_url,
            "retrieved_at": _timestamp(self.retrieved_at),
            "raw_content_hash": self.raw_content_hash,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "evidence_id": self.evidence_id}

    @classmethod
    def build(
        cls,
        *,
        source_ref: str,
        final_url: str,
        retrieved_at: datetime,
        raw_content_hash: str,
    ) -> SourceRightsEvidence:
        core = {
            "schema_version": "market-impact.source-rights-evidence.v1",
            "source_ref": source_ref,
            "final_url": final_url,
            "retrieved_at": _timestamp(retrieved_at),
            "raw_content_hash": raw_content_hash,
        }
        return cls(
            evidence_id=f"source-rights-evidence-{canonical_hash(core)}",
            source_ref=source_ref,
            final_url=final_url,
            retrieved_at=retrieved_at,
            raw_content_hash=raw_content_hash,
        )


@dataclass(frozen=True, slots=True)
class SourceRouteReplayRequest:
    """Immutable source artifacts a provider must re-import for route acceptance."""

    source_snapshot_id: str
    raw_response_payload: bytes
    rights_payload: bytes

    def __post_init__(self) -> None:
        _trimmed(self.source_snapshot_id, "source replay source_snapshot_id")
        if not self.raw_response_payload:
            raise ValueError("source replay raw_response_payload must not be empty")
        if not self.rights_payload:
            raise ValueError("source replay rights_payload must not be empty")


@dataclass(frozen=True, slots=True)
class SourceRouteReplayResult:
    """Persisted replay output produced from one SourceRouteReplayRequest."""

    snapshot_id: str
    store: LocalDataSnapshotStore

    def __post_init__(self) -> None:
        _trimmed(self.snapshot_id, "source replay snapshot_id")


@dataclass(frozen=True, slots=True)
class SourceRouteAcceptanceDeclaration:
    declaration_id: str
    provider_id: str
    provider_version: str
    provider_manifest_hash: str
    source_config_hash: str
    upstream_source: str
    capability: ObservationCapability
    rights_basis_url: str
    rights_reviewed_at: datetime
    permitted_use: str
    retention_scope: str
    redistribution_allowed: bool
    semantic_scope: str
    revision_strategy: str
    schema_version: str = SOURCE_ROUTE_ACCEPTANCE_DECLARATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != SOURCE_ROUTE_ACCEPTANCE_DECLARATION_SCHEMA:
            raise ValueError("unsupported source route acceptance declaration schema")
        _identifier(self.provider_id, "source acceptance provider_id")
        for name in (
            "provider_version",
            "upstream_source",
            "permitted_use",
            "retention_scope",
            "semantic_scope",
            "revision_strategy",
        ):
            _trimmed(getattr(self, name), f"source acceptance {name}")
        _sha256(self.provider_manifest_hash, "source acceptance provider_manifest_hash")
        _sha256(self.source_config_hash, "source acceptance source_config_hash")
        _https_url(self.rights_basis_url, "source acceptance rights_basis_url")
        _strict_utc(self.rights_reviewed_at, "source acceptance rights_reviewed_at")
        if self.permitted_use != "private_research":
            raise ValueError("source acceptance permitted_use must be private_research")
        if self.retention_scope != "private_raw_and_normalized":
            raise ValueError("source acceptance retention_scope must be private_raw_and_normalized")
        if self.redistribution_allowed:
            raise ValueError("source acceptance cannot grant redistribution")
        if self.revision_strategy != "append_only_content_versions":
            raise ValueError(
                "source acceptance revision_strategy must be append_only_content_versions"
            )
        if self.declaration_id != self.expected_declaration_id:
            raise ValueError("source acceptance declaration_id does not match content")

    @property
    def expected_declaration_id(self) -> str:
        return f"source-route-acceptance-declaration-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "provider_manifest_hash": self.provider_manifest_hash,
            "source_config_hash": self.source_config_hash,
            "upstream_source": self.upstream_source,
            "capability": self.capability.value,
            "rights_basis_url": self.rights_basis_url,
            "rights_reviewed_at": _timestamp(self.rights_reviewed_at),
            "permitted_use": self.permitted_use,
            "retention_scope": self.retention_scope,
            "redistribution_allowed": self.redistribution_allowed,
            "semantic_scope": self.semantic_scope,
            "revision_strategy": self.revision_strategy,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "declaration_id": self.declaration_id}

    @classmethod
    def build(
        cls,
        *,
        provider_id: str,
        provider_version: str,
        provider_manifest_hash: str,
        source_config_hash: str,
        upstream_source: str,
        capability: ObservationCapability,
        rights_basis_url: str,
        rights_reviewed_at: datetime,
        permitted_use: str,
        retention_scope: str,
        redistribution_allowed: bool,
        semantic_scope: str,
        revision_strategy: str,
    ) -> SourceRouteAcceptanceDeclaration:
        core = {
            "schema_version": SOURCE_ROUTE_ACCEPTANCE_DECLARATION_SCHEMA,
            "provider_id": provider_id,
            "provider_version": provider_version,
            "provider_manifest_hash": provider_manifest_hash,
            "source_config_hash": source_config_hash,
            "upstream_source": upstream_source,
            "capability": capability.value,
            "rights_basis_url": rights_basis_url,
            "rights_reviewed_at": _timestamp(rights_reviewed_at),
            "permitted_use": permitted_use,
            "retention_scope": retention_scope,
            "redistribution_allowed": redistribution_allowed,
            "semantic_scope": semantic_scope,
            "revision_strategy": revision_strategy,
        }
        return cls(
            declaration_id=(f"source-route-acceptance-declaration-{canonical_hash(core)}"),
            provider_id=provider_id,
            provider_version=provider_version,
            provider_manifest_hash=provider_manifest_hash,
            source_config_hash=source_config_hash,
            upstream_source=upstream_source,
            capability=capability,
            rights_basis_url=rights_basis_url,
            rights_reviewed_at=rights_reviewed_at,
            permitted_use=permitted_use,
            retention_scope=retention_scope,
            redistribution_allowed=redistribution_allowed,
            semantic_scope=semantic_scope,
            revision_strategy=revision_strategy,
        )


@dataclass(frozen=True, slots=True)
class SourceAcceptanceGateResult:
    gate: str
    status: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        SourceAcceptanceGate(self.gate)
        SourceAcceptanceStatus(self.status)
        if self.status == SourceAcceptanceStatus.PASS and self.reasons:
            raise ValueError("passing source acceptance gate cannot carry reasons")
        if self.status == SourceAcceptanceStatus.FAIL and not self.reasons:
            raise ValueError("failing source acceptance gate requires reasons")
        if len(self.reasons) != len(set(self.reasons)):
            raise ValueError("source acceptance gate reasons must be unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "gate": self.gate,
            "status": self.status,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class SourceRouteAcceptanceReport:
    report_id: str
    declaration: SourceRouteAcceptanceDeclaration
    rights_evidence: SourceRightsEvidence | None
    data_snapshot_id: str
    deterministic_replay_snapshot_id: str | None
    evaluated_at: datetime
    gates: tuple[SourceAcceptanceGateResult, ...]
    accepted: bool
    historical_pit_claim: bool = False
    evidence_promoted: bool = False
    execution_capability: bool = False
    schema_version: str = SOURCE_ROUTE_ACCEPTANCE_REPORT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != SOURCE_ROUTE_ACCEPTANCE_REPORT_SCHEMA:
            raise ValueError("unsupported source route acceptance report schema")
        _strict_utc(self.evaluated_at, "source acceptance evaluated_at")
        expected_gates = tuple(item.value for item in SourceAcceptanceGate)
        if tuple(item.gate for item in self.gates) != expected_gates:
            raise ValueError("source acceptance report must contain the seven ordered gates")
        expected_accepted = all(item.status == SourceAcceptanceStatus.PASS for item in self.gates)
        if self.accepted != expected_accepted:
            raise ValueError("source acceptance accepted flag does not match gate results")
        if self.historical_pit_claim or self.evidence_promoted or self.execution_capability:
            raise ValueError("source acceptance cannot grant PIT, Evidence, or execution authority")
        if self.report_id != self.expected_report_id:
            raise ValueError("source acceptance report_id does not match content")

    @property
    def expected_report_id(self) -> str:
        return f"source-route-acceptance-report-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "declaration": self.declaration.to_dict(),
            "rights_evidence": (
                None if self.rights_evidence is None else self.rights_evidence.to_dict()
            ),
            "data_snapshot_id": self.data_snapshot_id,
            "deterministic_replay_snapshot_id": self.deterministic_replay_snapshot_id,
            "evaluated_at": _timestamp(self.evaluated_at),
            "gates": [item.to_dict() for item in self.gates],
            "accepted": self.accepted,
            "historical_pit_claim": self.historical_pit_claim,
            "evidence_promoted": self.evidence_promoted,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "report_id": self.report_id}


def source_route_acceptance_report_from_dict(value: object) -> SourceRouteAcceptanceReport:
    payload = _object(value, "source route acceptance report")
    _closed(
        payload,
        {
            "schema_version",
            "report_id",
            "declaration",
            "rights_evidence",
            "data_snapshot_id",
            "deterministic_replay_snapshot_id",
            "evaluated_at",
            "gates",
            "accepted",
            "historical_pit_claim",
            "evidence_promoted",
            "execution_capability",
        },
        "source route acceptance report",
    )
    if _string(payload, "schema_version") != SOURCE_ROUTE_ACCEPTANCE_REPORT_SCHEMA:
        raise ValueError("unsupported source route acceptance report schema_version")
    rights_value = payload["rights_evidence"]
    report = SourceRouteAcceptanceReport(
        report_id=_string(payload, "report_id"),
        declaration=_source_route_acceptance_declaration_from_dict(payload["declaration"]),
        rights_evidence=(
            None if rights_value is None else _source_rights_evidence_from_dict(rights_value)
        ),
        data_snapshot_id=_string(payload, "data_snapshot_id"),
        deterministic_replay_snapshot_id=_optional_string(
            payload["deterministic_replay_snapshot_id"],
            "deterministic_replay_snapshot_id",
        ),
        evaluated_at=_datetime(payload["evaluated_at"], "evaluated_at"),
        gates=tuple(
            _source_acceptance_gate_result_from_dict(item)
            for item in _list(payload["gates"], "gates")
        ),
        accepted=_boolean(payload, "accepted"),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        evidence_promoted=_boolean(payload, "evidence_promoted"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )
    if report.to_dict() != payload:
        raise ValueError("source route acceptance report does not match canonical contract")
    return report


def load_source_route_acceptance_report(path: Path) -> SourceRouteAcceptanceReport:
    return source_route_acceptance_report_from_dict(json.loads(path.read_text(encoding="utf-8")))


def qualify_source_route(
    *,
    declaration: SourceRouteAcceptanceDeclaration,
    rights_evidence: SourceRightsEvidence | None,
    snapshot: DataSnapshot,
    source_store: LocalDataSnapshotStore,
    replay_from_stored_artifacts: Callable[[SourceRouteReplayRequest], SourceRouteReplayResult]
    | None,
    evaluated_at: datetime,
) -> SourceRouteAcceptanceReport:
    _strict_utc(evaluated_at, "source acceptance evaluated_at")
    try:
        stored_snapshot = source_store.get(snapshot.snapshot_id)
    except (FileNotFoundError, KeyError, ValueError):
        stored_snapshot = None
    accepted_snapshot = snapshot if stored_snapshot is None else stored_snapshot
    matching_sources = tuple(
        item
        for item in accepted_snapshot.query.sources
        if item.provider_id == declaration.provider_id
        and item.provider_version == declaration.provider_version
        and item.upstream_source == declaration.upstream_source
    )
    identity_reasons: list[str] = []
    if len(matching_sources) != 1:
        identity_reasons.append("declared_source_not_bound_exactly_once")
    elif matching_sources[0].manifest_hash != declaration.provider_manifest_hash:
        identity_reasons.append("provider_manifest_hash_mismatch")
    elif matching_sources[0].source_config_hash != declaration.source_config_hash:
        identity_reasons.append("source_config_hash_mismatch")
    if declaration.rights_reviewed_at > evaluated_at:
        identity_reasons.append("rights_review_after_evaluation")
    if rights_evidence is None:
        identity_reasons.append("rights_evidence_missing")
    else:
        if (
            rights_evidence.source_ref != declaration.rights_basis_url
            or rights_evidence.final_url != declaration.rights_basis_url
        ):
            identity_reasons.append("rights_evidence_identity_mismatch")
        if rights_evidence.retrieved_at > evaluated_at:
            identity_reasons.append("rights_evidence_after_evaluation")

    attempts = tuple(
        item
        for item in accepted_snapshot.attempts
        if item.provider_id == declaration.provider_id
        and item.provider_version == declaration.provider_version
        and item.upstream_source == declaration.upstream_source
    )
    transport_reasons: list[str] = []
    if len(attempts) != 1:
        transport_reasons.append("declared_source_attempt_not_found")
    elif not attempts[0].status.completed or attempts[0].raw_response_hash is None:
        transport_reasons.append("source_transport_not_completed")

    completeness_reasons: list[str] = []
    if not accepted_snapshot.coverage_complete:
        completeness_reasons.append("snapshot_coverage_incomplete")
    if len(attempts) == 1:
        attempt = attempts[0]
        if any(
            count != 0
            for count in (
                attempt.rejected_missing_availability,
                attempt.rejected_after_cutoff,
                attempt.rejected_missing_authority,
                attempt.rejected_authority_after_cutoff,
                attempt.rejected_lane_mismatch,
            )
        ):
            completeness_reasons.append("source_records_rejected_by_snapshot_gate")

    time_reasons: list[str] = []
    if accepted_snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
        time_reasons.append("sample_snapshot_not_prospective")
    matching_observations = tuple(
        item
        for item in accepted_snapshot.observations
        if item.provider_id == declaration.provider_id
        and item.provider_version == declaration.provider_version
        and item.upstream_source == declaration.upstream_source
    )
    if any(
        item.times.availability_basis is not AvailabilityBasis.ACTUAL_RECEIPT
        or item.times.available_at != item.times.retrieved_at
        or item.authority_kind != "actual_receipt"
        or item.authority_at != item.times.retrieved_at
        or (
            item.times.published_at is not None
            and item.times.published_at > item.times.retrieved_at
        )
        for item in matching_observations
    ):
        time_reasons.append("observation_receipt_semantics_invalid")
    versions = tuple((item.lineage_id, item.raw_content_hash) for item in matching_observations)
    if len(versions) != len(set(versions)):
        time_reasons.append("duplicate_observation_version")

    semantics_reasons: list[str] = []
    if accepted_snapshot.query.capability is not declaration.capability:
        semantics_reasons.append("capability_mismatch")
    if not declaration.semantic_scope:
        semantics_reasons.append("semantic_scope_missing")

    determinism_reasons = _snapshot_storage_reasons(
        store=source_store,
        snapshot=accepted_snapshot,
        prefix="source",
    )
    source_raw_response: bytes | None = None
    rights_payload: bytes | None = None
    if len(attempts) == 1 and attempts[0].raw_response_hash is not None:
        with suppress(FileNotFoundError, ValueError):
            source_raw_response = source_store.artifacts.get(
                attempts[0].raw_response_hash,
                media_type="application/octet-stream",
            ).path.read_bytes()
    if rights_evidence is not None:
        try:
            rights_payload = source_store.artifacts.get(
                rights_evidence.raw_content_hash,
                media_type="application/octet-stream",
            ).path.read_bytes()
        except (FileNotFoundError, ValueError):
            determinism_reasons.append("source_rights_storage_invalid")
    deterministic_replay_snapshot_id: str | None = None
    if replay_from_stored_artifacts is None:
        determinism_reasons.append("deterministic_replay_missing")
    elif stored_snapshot is None or source_raw_response is None or rights_payload is None:
        determinism_reasons.append("deterministic_replay_source_artifacts_invalid")
    else:
        try:
            replay = replay_from_stored_artifacts(
                SourceRouteReplayRequest(
                    source_snapshot_id=stored_snapshot.snapshot_id,
                    raw_response_payload=source_raw_response,
                    rights_payload=rights_payload,
                )
            )
            replay_snapshot = replay.store.get(replay.snapshot_id)
        except (FileNotFoundError, KeyError, ValueError):
            determinism_reasons.append("deterministic_replay_storage_invalid")
        except Exception:
            determinism_reasons.append("deterministic_replay_failed")
        else:
            deterministic_replay_snapshot_id = replay_snapshot.snapshot_id
            if replay_snapshot.to_dict() != stored_snapshot.to_dict():
                determinism_reasons.append("deterministic_replay_mismatch")
            determinism_reasons.extend(
                _snapshot_storage_reasons(
                    store=replay.store,
                    snapshot=replay_snapshot,
                    prefix="deterministic_replay",
                )
            )

    isolation_reasons: list[str] = []
    if not accepted_snapshot.coverage_complete:
        isolation_reasons.append("incomplete_snapshot_not_agent_eligible")
    if len(accepted_snapshot.query.sources) != 1 or len(matching_sources) != 1:
        isolation_reasons.append("sample_query_source_scope_not_exact")
    if accepted_snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
        isolation_reasons.append("sample_query_lane_not_prospective")

    gates = (
        _gate(SourceAcceptanceGate.RIGHTS_AND_IDENTITY, identity_reasons),
        _gate(SourceAcceptanceGate.TRANSPORT, transport_reasons),
        _gate(SourceAcceptanceGate.COMPLETENESS, completeness_reasons),
        _gate(SourceAcceptanceGate.TIME_AND_REVISIONS, time_reasons),
        _gate(SourceAcceptanceGate.MARKET_SEMANTICS, semantics_reasons),
        _gate(SourceAcceptanceGate.DETERMINISM_AND_STORAGE, determinism_reasons),
        _gate(SourceAcceptanceGate.AGENT_ISOLATION, isolation_reasons),
    )
    core = {
        "schema_version": SOURCE_ROUTE_ACCEPTANCE_REPORT_SCHEMA,
        "declaration": declaration.to_dict(),
        "rights_evidence": (None if rights_evidence is None else rights_evidence.to_dict()),
        "data_snapshot_id": accepted_snapshot.snapshot_id,
        "deterministic_replay_snapshot_id": deterministic_replay_snapshot_id,
        "evaluated_at": _timestamp(evaluated_at),
        "gates": [item.to_dict() for item in gates],
        "accepted": all(item.status == SourceAcceptanceStatus.PASS for item in gates),
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }
    return SourceRouteAcceptanceReport(
        report_id=f"source-route-acceptance-report-{canonical_hash(core)}",
        declaration=declaration,
        rights_evidence=rights_evidence,
        data_snapshot_id=accepted_snapshot.snapshot_id,
        deterministic_replay_snapshot_id=deterministic_replay_snapshot_id,
        evaluated_at=evaluated_at,
        gates=gates,
        accepted=bool(core["accepted"]),
    )


def _snapshot_storage_reasons(
    *,
    store: LocalDataSnapshotStore,
    snapshot: DataSnapshot,
    prefix: str,
) -> list[str]:
    reasons: list[str] = []
    try:
        if store.get(snapshot.snapshot_id) != snapshot:
            reasons.append(f"{prefix}_snapshot_storage_invalid")
    except (FileNotFoundError, KeyError, ValueError):
        reasons.append(f"{prefix}_snapshot_storage_invalid")
    raw_hashes = [
        item.raw_response_hash for item in snapshot.attempts if item.raw_response_hash is not None
    ]
    raw_hashes.extend(item.raw_content_hash for item in snapshot.observations)
    try:
        for content_hash in raw_hashes:
            store.artifacts.get(content_hash, media_type="application/octet-stream")
    except (FileNotFoundError, ValueError):
        reasons.append(f"{prefix}_raw_storage_invalid")
    return reasons


def write_source_route_acceptance_report(
    report: SourceRouteAcceptanceReport,
    output_root: Path,
) -> Path:
    root = output_root.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    target = root / f"{report.report_id}.json"
    encoded = json.dumps(
        report.to_dict(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode()
    if target.exists():
        if target.read_bytes() != encoded:
            raise ValueError("source acceptance report identity has conflicting content")
        os.chmod(target, 0o600)
        return target
    descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-acceptance-", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        temporary.replace(target)
        os.chmod(target, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    return target


def _source_route_acceptance_declaration_from_dict(
    value: object,
) -> SourceRouteAcceptanceDeclaration:
    payload = _object(value, "source route acceptance declaration")
    _closed(
        payload,
        {
            "schema_version",
            "declaration_id",
            "provider_id",
            "provider_version",
            "provider_manifest_hash",
            "source_config_hash",
            "upstream_source",
            "capability",
            "rights_basis_url",
            "rights_reviewed_at",
            "permitted_use",
            "retention_scope",
            "redistribution_allowed",
            "semantic_scope",
            "revision_strategy",
        },
        "source route acceptance declaration",
    )
    return SourceRouteAcceptanceDeclaration(
        declaration_id=_string(payload, "declaration_id"),
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        provider_manifest_hash=_string(payload, "provider_manifest_hash"),
        source_config_hash=_string(payload, "source_config_hash"),
        upstream_source=_string(payload, "upstream_source"),
        capability=ObservationCapability(_string(payload, "capability")),
        rights_basis_url=_string(payload, "rights_basis_url"),
        rights_reviewed_at=_datetime(payload["rights_reviewed_at"], "rights_reviewed_at"),
        permitted_use=_string(payload, "permitted_use"),
        retention_scope=_string(payload, "retention_scope"),
        redistribution_allowed=_boolean(payload, "redistribution_allowed"),
        semantic_scope=_string(payload, "semantic_scope"),
        revision_strategy=_string(payload, "revision_strategy"),
        schema_version=_string(payload, "schema_version"),
    )


def _source_rights_evidence_from_dict(value: object) -> SourceRightsEvidence:
    payload = _object(value, "source rights evidence")
    _closed(
        payload,
        {
            "schema_version",
            "evidence_id",
            "source_ref",
            "final_url",
            "retrieved_at",
            "raw_content_hash",
        },
        "source rights evidence",
    )
    return SourceRightsEvidence(
        evidence_id=_string(payload, "evidence_id"),
        source_ref=_string(payload, "source_ref"),
        final_url=_string(payload, "final_url"),
        retrieved_at=_datetime(payload["retrieved_at"], "retrieved_at"),
        raw_content_hash=_string(payload, "raw_content_hash"),
        schema_version=_string(payload, "schema_version"),
    )


def _source_acceptance_gate_result_from_dict(value: object) -> SourceAcceptanceGateResult:
    payload = _object(value, "source acceptance gate result")
    _closed(payload, {"gate", "status", "reasons"}, "source acceptance gate result")
    return SourceAcceptanceGateResult(
        gate=_string(payload, "gate"),
        status=_string(payload, "status"),
        reasons=tuple(
            _string(item, "source acceptance gate reason")
            for item in _list(payload["reasons"], "reasons")
        ),
    )


def _gate(
    gate: SourceAcceptanceGate,
    reasons: list[str],
) -> SourceAcceptanceGateResult:
    unique = tuple(dict.fromkeys(reasons))
    return SourceAcceptanceGateResult(
        gate=gate.value,
        status=(
            SourceAcceptanceStatus.PASS.value if not unique else SourceAcceptanceStatus.FAIL.value
        ),
        reasons=unique,
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _closed(value: dict[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{name} fields are invalid")


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _string(value: dict[str, object] | object, name: str) -> str:
    if isinstance(value, dict):
        candidate: object = cast(dict[str, object], value)[name]
    else:
        candidate = value
    if not isinstance(candidate, str):
        raise TypeError(f"{name} must be a string")
    return candidate


def _optional_string(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _string(value, name)


def _boolean(value: dict[str, object], name: str) -> bool:
    candidate = value[name]
    if not isinstance(candidate, bool):
        raise TypeError(f"{name} must be a boolean")
    return candidate


def _datetime(value: object, name: str) -> datetime:
    raw = _string(value, name)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} must use lowercase letters, digits, dot, dash, or underscore")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _https_url(value: str, name: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.hostname is None:
        raise ValueError(f"{name} must be an HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"{name} cannot contain credentials")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
