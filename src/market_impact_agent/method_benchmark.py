from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    PatternPack,
    canonical_hash,
)
from market_impact_agent.agent_runtime import SkillRegistry
from market_impact_agent.domain import require_aware
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.observations import AvailabilityBasis
from market_impact_agent.research import EventArchetype
from market_impact_agent.research_methods import (
    MethodArm,
    ResearchContext,
    ResearchMethodCatalog,
    ResearchMethodRouter,
)

HISTORICAL_EVIDENCE_MANIFEST_SCHEMA = "market-impact.historical-evidence-manifest.v1"
METHOD_QUALITY_BENCHMARK_SCHEMA = "market-impact.method-quality-benchmark-registration.v1"
METHOD_QUALITY_BENCHMARK_SCHEMA_V2 = "market-impact.method-quality-benchmark-registration.v2"
MASKED_AGENT_INPUT_MANIFEST_SCHEMA = "market-impact.masked-agent-input-manifest.v1"
METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA = (
    "market-impact.method-quality-evaluation-specification.v1"
)
METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA_V2 = (
    "market-impact.method-quality-evaluation-specification.v2"
)
SOURCE_VERSION_RECEIPT_SCHEMA = "market-impact.source-version-receipt.v1"
LATENCY_CALIBRATION_SCHEMA = "market-impact.latency-calibration.v1"
RETIRED_METHOD_QUALITY_V1_REGISTRATION_ID = (
    "method-quality-benchmark-fbebb357c40f091ff03214b517bdc8e75011126fc82d28ee49a57c641c0187de"
)


class BenchmarkSplit(StrEnum):
    DEVELOPMENT = "development"
    RETROSPECTIVE_HOLDOUT = "retrospective_holdout"
    PROSPECTIVE_HOLDOUT = "prospective_holdout"


class IdentityMaskingPolicy(StrEnum):
    NONE = "none"
    CONSISTENT_ALIASES = "consistent_aliases"


class ProvenanceTrustStatus(StrEnum):
    SYNTHETIC_CONTRACT_ONLY = "synthetic_contract_only"
    CONTRACT_VALIDATED_UNTRUSTED = "contract_validated_untrusted"


@dataclass(frozen=True, slots=True)
class LatencyCalibration:
    calibration_id: str
    source_class: str
    provider_id: str
    archive_id: str
    calibration_version: str
    sample_hash: str
    sample_count: int
    calibrated_at: datetime
    availability_offset_seconds: int
    trust_status: ProvenanceTrustStatus

    def __post_init__(self) -> None:
        for name in ("source_class", "provider_id", "archive_id", "calibration_version"):
            _nonempty(cast(str, getattr(self, name)), f"latency calibration {name}")
        _sha256(self.sample_hash, "latency calibration sample_hash")
        require_aware(self.calibrated_at, "latency calibration calibrated_at")
        if self.sample_count < 1:
            raise ValueError("latency calibration sample_count must be positive")
        if self.availability_offset_seconds < 0:
            raise ValueError("latency calibration offset must not be negative")
        if self.calibration_id != self.expected_calibration_id:
            raise ValueError("latency calibration identity does not match content")

    @property
    def calibration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_calibration_id(self) -> str:
        return f"latency-calibration-{self.calibration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": LATENCY_CALIBRATION_SCHEMA,
            "source_class": self.source_class,
            "provider_id": self.provider_id,
            "archive_id": self.archive_id,
            "calibration_version": self.calibration_version,
            "sample_hash": self.sample_hash,
            "sample_count": self.sample_count,
            "calibrated_at": _timestamp(self.calibrated_at),
            "availability_offset_seconds": self.availability_offset_seconds,
            "trust_status": self.trust_status.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "calibration_id": self.calibration_id}


@dataclass(frozen=True, slots=True)
class SourceVersionReceipt:
    receipt_id: str
    source_ref: str
    provider_id: str
    archive_id: str
    archive_version: str
    source_version_id: str
    raw_content_hash: str
    extracted_content_hash: str
    published_at: datetime
    source_updated_at: datetime | None
    retrieved_at: datetime
    available_at: datetime
    availability_basis: AvailabilityBasis
    latency_calibration: LatencyCalibration | None
    trust_status: ProvenanceTrustStatus

    def __post_init__(self) -> None:
        for name in (
            "source_ref",
            "provider_id",
            "archive_id",
            "archive_version",
            "source_version_id",
        ):
            _nonempty(cast(str, getattr(self, name)), f"source receipt {name}")
        _sha256(self.raw_content_hash, "source receipt raw_content_hash")
        _sha256(self.extracted_content_hash, "source receipt extracted_content_hash")
        for name in ("published_at", "retrieved_at", "available_at"):
            require_aware(cast(datetime, getattr(self, name)), f"source receipt {name}")
        if self.source_updated_at is not None:
            require_aware(self.source_updated_at, "source receipt source_updated_at")
            if self.source_updated_at > self.retrieved_at:
                raise ValueError("source receipt source_updated_at must not follow retrieval")
        if self.published_at > self.available_at or self.available_at > self.retrieved_at:
            raise ValueError("source receipt timestamps are not chronological")
        if self.availability_basis is AvailabilityBasis.UNKNOWN:
            raise ValueError("source receipts cannot use unknown availability")
        if self.availability_basis is AvailabilityBasis.MODELED_LATENCY:
            calibration = self.latency_calibration
            if calibration is None:
                raise ValueError("modeled source availability requires a latency calibration")
            if calibration.calibrated_at > self.published_at:
                raise ValueError("latency calibration must predate the modeled source version")
            expected = self.published_at + timedelta(
                seconds=calibration.availability_offset_seconds
            )
            if self.available_at != expected:
                raise ValueError("modeled source availability does not match calibration")
        elif self.latency_calibration is not None:
            raise ValueError("latency calibration is only valid for modeled availability")
        if (
            self.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
            and self.available_at != self.retrieved_at
        ):
            raise ValueError("actual receipt availability must equal retrieval")
        if self.receipt_id != self.expected_receipt_id:
            raise ValueError("source receipt identity does not match content")

    @property
    def receipt_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_receipt_id(self) -> str:
        return f"source-version-receipt-{self.receipt_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": SOURCE_VERSION_RECEIPT_SCHEMA,
            "source_ref": self.source_ref,
            "provider_id": self.provider_id,
            "archive_id": self.archive_id,
            "archive_version": self.archive_version,
            "source_version_id": self.source_version_id,
            "raw_content_hash": self.raw_content_hash,
            "extracted_content_hash": self.extracted_content_hash,
            "published_at": _timestamp(self.published_at),
            "source_updated_at": (
                None if self.source_updated_at is None else _timestamp(self.source_updated_at)
            ),
            "retrieved_at": _timestamp(self.retrieved_at),
            "available_at": _timestamp(self.available_at),
            "availability_basis": self.availability_basis.value,
            "latency_calibration": (
                None if self.latency_calibration is None else self.latency_calibration.to_dict()
            ),
            "trust_status": self.trust_status.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "receipt_id": self.receipt_id}


@dataclass(frozen=True, slots=True)
class HistoricalEvidenceVersion:
    evidence_id: str
    claim_id: str
    source_version_id: str
    occurred_at: datetime
    published_at: datetime
    source_updated_at: datetime | None
    available_at: datetime
    retrieved_at: datetime
    availability_basis: AvailabilityBasis
    source_version_receipt: SourceVersionReceipt
    supersedes_id: str | None
    content_hash: str

    def __post_init__(self) -> None:
        for name in ("evidence_id", "claim_id", "source_version_id"):
            _nonempty(cast(str, getattr(self, name)), name)
        for name in ("occurred_at", "published_at", "available_at", "retrieved_at"):
            require_aware(cast(datetime, getattr(self, name)), name)
        if self.source_updated_at is not None:
            require_aware(self.source_updated_at, "source_updated_at")
            if self.source_updated_at > self.retrieved_at:
                raise ValueError("source_updated_at must not be after retrieved_at")
        if self.published_at > self.available_at:
            raise ValueError("published_at must not be after available_at")
        if self.available_at > self.retrieved_at:
            raise ValueError("available_at must not be after retrieved_at")
        if self.availability_basis is AvailabilityBasis.UNKNOWN:
            raise ValueError("strict historical evidence cannot have unknown availability")
        if (
            self.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
            and self.available_at != self.retrieved_at
        ):
            raise ValueError("actual-receipt historical availability must equal retrieval")
        if self.supersedes_id == self.evidence_id:
            raise ValueError("historical evidence cannot supersede itself")
        _sha256(self.content_hash, "historical evidence content_hash")
        receipt = self.source_version_receipt
        if (
            receipt.source_version_id != self.source_version_id
            or receipt.published_at != self.published_at
            or receipt.source_updated_at != self.source_updated_at
            or receipt.available_at != self.available_at
            or receipt.retrieved_at != self.retrieved_at
            or receipt.availability_basis is not self.availability_basis
            or receipt.extracted_content_hash != self.content_hash
        ):
            raise ValueError("historical evidence does not match its source version receipt")

    def to_dict(self) -> dict[str, object]:
        return {
            "evidence_id": self.evidence_id,
            "claim_id": self.claim_id,
            "source_version_id": self.source_version_id,
            "occurred_at": _timestamp(self.occurred_at),
            "published_at": _timestamp(self.published_at),
            "source_updated_at": (
                None if self.source_updated_at is None else _timestamp(self.source_updated_at)
            ),
            "available_at": _timestamp(self.available_at),
            "retrieved_at": _timestamp(self.retrieved_at),
            "availability_basis": self.availability_basis.value,
            "source_version_receipt": self.source_version_receipt.to_dict(),
            "supersedes_id": self.supersedes_id,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class HistoricalEvidenceManifest:
    manifest_id: str
    case_alias: str
    split: BenchmarkSplit
    evidence_pack_id: str
    evidence_pack_hash: str
    as_of: datetime
    identity_masking: IdentityMaskingPolicy
    masked_agent_input_manifest_id: str | None
    masked_agent_input_manifest_hash: str | None
    provenance_trust_status: ProvenanceTrustStatus
    outcomes_opened: bool
    evidence_versions: tuple[HistoricalEvidenceVersion, ...]
    external_tool_access: bool
    outcome_memory_policy: str
    execution_capability: str

    def __post_init__(self) -> None:
        _identifier(self.case_alias, "historical case_alias")
        _nonempty(self.evidence_pack_id, "historical evidence_pack_id")
        _sha256(self.evidence_pack_hash, "historical evidence_pack_hash")
        require_aware(self.as_of, "historical evidence as_of")
        if not self.evidence_versions:
            raise ValueError("historical evidence manifests require evidence versions")
        evidence_ids = tuple(item.evidence_id for item in self.evidence_versions)
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("historical evidence_ids must be unique")
        if any(item.available_at > self.as_of for item in self.evidence_versions):
            raise ValueError("historical manifest contains future-available evidence")
        if any(
            item.source_updated_at is not None and item.source_updated_at > self.as_of
            for item in self.evidence_versions
        ):
            raise ValueError("historical manifest contains a source version updated after as_of")
        _validate_revision_lineage(self.evidence_versions)
        if (
            self.split is BenchmarkSplit.RETROSPECTIVE_HOLDOUT
            and self.identity_masking is not IdentityMaskingPolicy.CONSISTENT_ALIASES
        ):
            raise ValueError("retrospective holdouts require consistent identity aliases")
        receipt_statuses = {
            item.source_version_receipt.trust_status for item in self.evidence_versions
        }
        if self.provenance_trust_status not in receipt_statuses or len(receipt_statuses) != 1:
            raise ValueError("manifest trust status must match every source receipt")
        if self.split is BenchmarkSplit.RETROSPECTIVE_HOLDOUT:
            raise ValueError("retrospective holdout admission is unavailable in v1")
        if self.identity_masking is IdentityMaskingPolicy.CONSISTENT_ALIASES:
            if (
                self.masked_agent_input_manifest_id is None
                or self.masked_agent_input_manifest_hash is None
            ):
                raise ValueError("consistent aliases require a masked Agent Input Manifest")
            _sha256(self.masked_agent_input_manifest_hash, "masked Agent Input Manifest hash")
            if self.masked_agent_input_manifest_id != (
                f"masked-agent-input-{self.masked_agent_input_manifest_hash}"
            ):
                raise ValueError("masked Agent Input Manifest identity is inconsistent")
        elif (
            self.masked_agent_input_manifest_id is not None
            or self.masked_agent_input_manifest_hash is not None
        ):
            raise ValueError("unmasked historical cases cannot bind a masked Agent Input Manifest")
        if self.split is not BenchmarkSplit.DEVELOPMENT and self.outcomes_opened:
            raise ValueError("holdout outcomes must remain sealed")
        if self.external_tool_access:
            raise ValueError("historical benchmark cases cannot grant external tool access")
        if self.outcome_memory_policy != "train_only_frozen_pattern_packs":
            raise ValueError("historical outcome memory must be isolated to frozen train packs")
        if self.execution_capability != "none":
            raise ValueError("historical evidence manifests grant no execution capability")
        if self.manifest_id != self.expected_manifest_id:
            raise ValueError("historical evidence manifest_id does not match content")

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_manifest_id(self) -> str:
        return f"historical-evidence-{self.manifest_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": HISTORICAL_EVIDENCE_MANIFEST_SCHEMA,
            "case_alias": self.case_alias,
            "split": self.split.value,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "as_of": _timestamp(self.as_of),
            "identity_masking": self.identity_masking.value,
            "masked_agent_input_manifest_id": self.masked_agent_input_manifest_id,
            "masked_agent_input_manifest_hash": self.masked_agent_input_manifest_hash,
            "provenance_trust_status": self.provenance_trust_status.value,
            "outcomes_opened": self.outcomes_opened,
            "evidence_versions": [item.to_dict() for item in self.evidence_versions],
            "external_tool_access": self.external_tool_access,
            "outcome_memory_policy": self.outcome_memory_policy,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "manifest_id": self.manifest_id}

    def validate_against(self, evidence_pack: EvidencePack) -> None:
        if self.evidence_pack_id != evidence_pack.pack_id:
            raise ValueError("historical manifest does not match Evidence Pack identity")
        if self.evidence_pack_hash != canonical_hash(evidence_pack.to_dict()):
            raise ValueError("historical manifest does not match Evidence Pack content")
        if self.as_of != evidence_pack.as_of:
            raise ValueError("historical manifest as_of does not match Evidence Pack")
        references = {item.evidence_id: item for item in evidence_pack.evidence}
        versions = {item.evidence_id: item for item in self.evidence_versions}
        if set(references) != set(versions):
            raise ValueError("historical versions must exactly match Evidence Pack evidence")
        for evidence_id, reference in references.items():
            version = versions[evidence_id]
            if (
                reference.claim_id != version.claim_id
                or reference.source_ref != version.source_version_receipt.source_ref
                or reference.available_at != version.available_at
                or reference.content_hash != version.content_hash
            ):
                raise ValueError(f"historical provenance mismatch: {evidence_id}")


@dataclass(frozen=True, slots=True)
class IdentityAlias:
    original: str
    masked: str

    def __post_init__(self) -> None:
        _nonempty(self.original, "identity alias original")
        _nonempty(self.masked, "identity alias masked")
        if self.original == self.masked:
            raise ValueError("identity aliases must change their source token")

    def to_dict(self) -> dict[str, str]:
        return {"original": self.original, "masked": self.masked}


@dataclass(frozen=True, slots=True)
class MaskedAgentInputManifest:
    manifest_id: str
    original_evidence_pack_id: str
    original_evidence_pack_hash: str
    original_documents_hash: str
    original_pattern_packs_hash: str
    masked_evidence_pack_id: str
    masked_evidence_pack_hash: str
    masked_documents_hash: str
    masked_pattern_packs_hash: str
    alias_map: tuple[IdentityAlias, ...]
    alias_map_hash: str
    forbidden_tokens: tuple[str, ...]
    forbidden_tokens_hash: str
    agent_visible_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in (
            "original_evidence_pack_hash",
            "original_documents_hash",
            "original_pattern_packs_hash",
            "masked_evidence_pack_hash",
            "masked_documents_hash",
            "masked_pattern_packs_hash",
            "alias_map_hash",
            "forbidden_tokens_hash",
        ):
            _sha256(cast(str, getattr(self, name)), name)
        if not self.alias_map:
            raise ValueError("masked Agent inputs require a non-empty alias map")
        originals = tuple(item.original for item in self.alias_map)
        masked = tuple(item.masked for item in self.alias_map)
        if len(originals) != len(set(originals)) or len(masked) != len(set(masked)):
            raise ValueError("masked Agent input aliases must be one-to-one")
        if self.alias_map_hash != canonical_hash([item.to_dict() for item in self.alias_map]):
            raise ValueError("masked Agent input alias_map_hash does not match content")
        _unique_nonempty(self.forbidden_tokens, "masked Agent input forbidden_tokens")
        if set(self.forbidden_tokens) != set(originals):
            raise ValueError("forbidden tokens must exactly match original alias tokens")
        if self.forbidden_tokens_hash != canonical_hash(list(self.forbidden_tokens)):
            raise ValueError("forbidden_tokens_hash does not match content")
        required_fields = {
            "evidence_pack.*",
            "read_evidence.*",
            "read_pattern_pack.*",
        }
        if set(self.agent_visible_fields) != required_fields or len(
            self.agent_visible_fields
        ) != len(required_fields):
            raise ValueError("masked Agent input visible-field coverage is incomplete")
        if self.manifest_id != self.expected_manifest_id:
            raise ValueError("masked Agent Input Manifest identity does not match content")

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_manifest_id(self) -> str:
        return f"masked-agent-input-{self.manifest_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": MASKED_AGENT_INPUT_MANIFEST_SCHEMA,
            "original_evidence_pack_id": self.original_evidence_pack_id,
            "original_evidence_pack_hash": self.original_evidence_pack_hash,
            "original_documents_hash": self.original_documents_hash,
            "original_pattern_packs_hash": self.original_pattern_packs_hash,
            "masked_evidence_pack_id": self.masked_evidence_pack_id,
            "masked_evidence_pack_hash": self.masked_evidence_pack_hash,
            "masked_documents_hash": self.masked_documents_hash,
            "masked_pattern_packs_hash": self.masked_pattern_packs_hash,
            "alias_map": [item.to_dict() for item in self.alias_map],
            "alias_map_hash": self.alias_map_hash,
            "forbidden_tokens": list(self.forbidden_tokens),
            "forbidden_tokens_hash": self.forbidden_tokens_hash,
            "agent_visible_fields": list(self.agent_visible_fields),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "manifest_id": self.manifest_id}

    def validate_against(
        self,
        *,
        original_pack: EvidencePack,
        original_documents: dict[str, object],
        original_pattern_packs: tuple[PatternPack, ...],
        masked_pack: EvidencePack,
        masked_documents: dict[str, object],
        masked_pattern_packs: tuple[PatternPack, ...],
    ) -> None:
        if (
            self.original_evidence_pack_id != original_pack.pack_id
            or self.original_evidence_pack_hash != canonical_hash(original_pack.to_dict())
        ):
            raise ValueError("masked Agent input does not match original Evidence Pack")
        if self.original_documents_hash != canonical_hash(original_documents):
            raise ValueError("masked Agent input does not match original evidence documents")
        if self.original_pattern_packs_hash != canonical_hash(
            [item.to_dict() for item in original_pattern_packs]
        ):
            raise ValueError("masked Agent input does not match original Pattern Packs")
        if (
            self.masked_evidence_pack_id != masked_pack.pack_id
            or self.masked_evidence_pack_hash != canonical_hash(masked_pack.to_dict())
        ):
            raise ValueError("masked Agent input does not match masked Evidence Pack")
        if self.masked_documents_hash != canonical_hash(masked_documents):
            raise ValueError("masked Agent input does not match masked evidence documents")
        if self.masked_pattern_packs_hash != canonical_hash(
            [item.to_dict() for item in masked_pattern_packs]
        ):
            raise ValueError("masked Agent input does not match masked Pattern Packs")
        visible_input = {
            "evidence_pack": masked_pack.to_dict(),
            "evidence_documents": masked_documents,
            "pattern_packs": [item.to_dict() for item in masked_pattern_packs],
        }
        leaked = tuple(
            token for token in self.forbidden_tokens if _contains_token(visible_input, token)
        )
        if leaked:
            raise ValueError(f"masked Agent input contains forbidden token: {leaked[0]}")
        aliases = tuple((item.original, item.masked) for item in self.alias_map)
        expected_documents = _apply_aliases(original_documents, aliases)
        if expected_documents != masked_documents:
            raise ValueError("masked evidence documents are not the registered alias transform")
        _validate_document_bindings(original_pack, original_documents)
        _validate_document_bindings(masked_pack, masked_documents)
        _validate_pattern_bindings(original_pack, original_pattern_packs)
        _validate_pattern_bindings(masked_pack, masked_pattern_packs)
        _validate_masked_pattern_packs(original_pattern_packs, masked_pattern_packs, aliases)
        _validate_masked_pack_fields(
            original_pack,
            original_documents,
            original_pattern_packs,
            masked_pack,
            masked_documents,
            masked_pattern_packs,
            aliases,
        )


@dataclass(frozen=True, slots=True)
class MechanismStratum:
    event_archetype: EventArchetype
    target_case_count: int
    minimum_required_abstentions: int

    def __post_init__(self) -> None:
        if self.target_case_count < 1:
            raise ValueError("mechanism stratum target_case_count must be positive")
        if not 0 <= self.minimum_required_abstentions <= self.target_case_count:
            raise ValueError("mechanism stratum abstention count is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "event_archetype": self.event_archetype.value,
            "target_case_count": self.target_case_count,
            "minimum_required_abstentions": self.minimum_required_abstentions,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkSuite:
    suite_id: str
    arms: tuple[MethodArm, ...]
    eligible_archetypes: tuple[EventArchetype, ...]
    minimum_case_count: int
    claim: str

    def __post_init__(self) -> None:
        _identifier(self.suite_id, "benchmark suite_id")
        if not self.arms or len(self.arms) != len(set(self.arms)):
            raise ValueError("benchmark suite arms must be non-empty and unique")
        if not self.eligible_archetypes or len(self.eligible_archetypes) != len(
            set(self.eligible_archetypes)
        ):
            raise ValueError("benchmark suite archetypes must be non-empty and unique")
        if self.minimum_case_count < 1:
            raise ValueError("benchmark suite minimum_case_count must be positive")
        _nonempty(self.claim, "benchmark suite claim")

    def to_dict(self) -> dict[str, object]:
        return {
            "suite_id": self.suite_id,
            "arms": [item.value for item in self.arms],
            "eligible_archetypes": [item.value for item in self.eligible_archetypes],
            "minimum_case_count": self.minimum_case_count,
            "claim": self.claim,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkTreatmentBinding:
    suite_id: str
    arm: MethodArm
    context: ResearchContext
    route_id: str
    route_hash: str
    requested_skills: tuple[str, ...]
    loaded_skills: tuple[str, ...]
    manifest_hashes: tuple[str, ...]
    instruction_hashes: tuple[str, ...]
    allowed_capabilities: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.suite_id, "treatment binding suite_id")
        _sha256(self.route_hash, "treatment binding route_hash")
        if self.route_id != f"skill-route-{self.route_hash}":
            raise ValueError("treatment binding route identity is inconsistent")
        _unique_nonempty(self.requested_skills, "treatment binding requested_skills")
        _unique_nonempty(self.loaded_skills, "treatment binding loaded_skills")
        if not (
            len(self.loaded_skills) == len(self.manifest_hashes) == len(self.instruction_hashes)
        ):
            raise ValueError("treatment binding Skill identities are incomplete")
        for value in self.manifest_hashes:
            _sha256(value, "treatment binding manifest hash")
        for value in self.instruction_hashes:
            _sha256(value, "treatment binding instruction hash")
        _unique_nonempty(self.allowed_capabilities, "treatment binding capabilities")
        _unique_nonempty(self.allowed_tools, "treatment binding tools")
        _unique_nonempty(self.reasons, "treatment binding reasons")

    def to_dict(self) -> dict[str, object]:
        return {
            "suite_id": self.suite_id,
            "arm": self.arm.value,
            "context": {
                "mechanism_family": self.context.mechanism_family,
                "asset_class": self.context.asset_class,
                "has_pattern_pack": self.context.has_pattern_pack,
            },
            "route_id": self.route_id,
            "route_hash": self.route_hash,
            "requested_skills": list(self.requested_skills),
            "loaded_skills": list(self.loaded_skills),
            "manifest_hashes": list(self.manifest_hashes),
            "instruction_hashes": list(self.instruction_hashes),
            "allowed_capabilities": list(self.allowed_capabilities),
            "allowed_tools": list(self.allowed_tools),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class MethodQualityEvaluationSpecification:
    specification_id: str
    canonical_specification_json: str

    def __post_init__(self) -> None:
        core = self.core_dict()
        _validate_evaluation_specification_core(core)
        if self.specification_id != self.expected_specification_id:
            raise ValueError(
                "method quality evaluation specification identity does not match content"
            )

    @property
    def specification_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_specification_id(self) -> str:
        return f"method-quality-evaluation-{self.specification_hash}"

    def core_dict(self) -> dict[str, object]:
        return _object(json.loads(self.canonical_specification_json), "evaluation specification")

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "specification_id": self.specification_id}


@dataclass(frozen=True, slots=True)
class ContaminationControl:
    historical_identity_policy: str
    masked_fields: tuple[str, ...]
    economic_roles_preserved: bool
    source_tiers_preserved: bool
    prompt_and_tool_aliases_identical: bool
    external_network_access: bool
    outcome_access_policy: str
    outcome_memory_policy: str

    def __post_init__(self) -> None:
        if self.historical_identity_policy != "consistent_aliases":
            raise ValueError("historical benchmark identity policy must use consistent aliases")
        required_masked = {"calendar_date", "event_identity", "instrument_identity"}
        if set(self.masked_fields) != required_masked or len(self.masked_fields) != len(
            required_masked
        ):
            raise ValueError("historical benchmark masked fields are incomplete")
        if not self.economic_roles_preserved or not self.source_tiers_preserved:
            raise ValueError("masking must preserve economic roles and source tiers")
        if not self.prompt_and_tool_aliases_identical:
            raise ValueError("prompt and tool aliases must be identical")
        if self.external_network_access:
            raise ValueError("benchmark Agent runs cannot access the external network")
        if self.outcome_access_policy != "sealed_until_all_judgments_are_content_bound":
            raise ValueError("benchmark outcome access policy is invalid")
        if self.outcome_memory_policy != "train_only_frozen_pattern_packs":
            raise ValueError("benchmark outcome memory policy is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "historical_identity_policy": self.historical_identity_policy,
            "masked_fields": list(self.masked_fields),
            "economic_roles_preserved": self.economic_roles_preserved,
            "source_tiers_preserved": self.source_tiers_preserved,
            "prompt_and_tool_aliases_identical": self.prompt_and_tool_aliases_identical,
            "external_network_access": self.external_network_access,
            "outcome_access_policy": self.outcome_access_policy,
            "outcome_memory_policy": self.outcome_memory_policy,
        }


@dataclass(frozen=True, slots=True)
class PromotionGate:
    allowed_time_gate_violations: int
    minimum_required_abstention_recall: Decimal
    require_positive_candidate_directional_score: bool
    paired_confidence_level: Decimal
    require_positive_paired_lower_bound_vs_neutral: bool
    minimum_registered_baselines_beaten: int
    maximum_single_case_absolute_outcome_share: Decimal
    minimum_positive_strata: int
    maximum_drawdown_increase_vs_neutral: Decimal
    maximum_cvar95_increase_vs_neutral: Decimal
    added_cost_requires_positive_increment: bool

    def __post_init__(self) -> None:
        if self.allowed_time_gate_violations != 0:
            raise ValueError("method promotion cannot tolerate time-gate violations")
        for name in (
            "minimum_required_abstention_recall",
            "paired_confidence_level",
            "maximum_single_case_absolute_outcome_share",
            "maximum_drawdown_increase_vs_neutral",
            "maximum_cvar95_increase_vs_neutral",
        ):
            value = cast(Decimal, getattr(self, name))
            if not value.is_finite() or not Decimal("0") <= value <= Decimal("1"):
                raise ValueError(f"{name} must be between zero and one")
        if self.paired_confidence_level != Decimal("0.95"):
            raise ValueError("method promotion requires a frozen 95% paired interval")
        if not self.require_positive_candidate_directional_score:
            raise ValueError("method promotion requires positive candidate directional score")
        if not self.require_positive_paired_lower_bound_vs_neutral:
            raise ValueError("method promotion requires a positive paired lower bound")
        if self.minimum_registered_baselines_beaten < 2:
            raise ValueError("method promotion must beat at least two registered baselines")
        if self.minimum_positive_strata < 2:
            raise ValueError("method promotion must work across multiple strata")
        if not self.added_cost_requires_positive_increment:
            raise ValueError("added method cost must require positive incremental value")

    def to_dict(self) -> dict[str, object]:
        return {
            "allowed_time_gate_violations": self.allowed_time_gate_violations,
            "minimum_required_abstention_recall": str(self.minimum_required_abstention_recall),
            "require_positive_candidate_directional_score": (
                self.require_positive_candidate_directional_score
            ),
            "paired_confidence_level": str(self.paired_confidence_level),
            "require_positive_paired_lower_bound_vs_neutral": (
                self.require_positive_paired_lower_bound_vs_neutral
            ),
            "minimum_registered_baselines_beaten": self.minimum_registered_baselines_beaten,
            "maximum_single_case_absolute_outcome_share": str(
                self.maximum_single_case_absolute_outcome_share
            ),
            "minimum_positive_strata": self.minimum_positive_strata,
            "maximum_drawdown_increase_vs_neutral": str(self.maximum_drawdown_increase_vs_neutral),
            "maximum_cvar95_increase_vs_neutral": str(self.maximum_cvar95_increase_vs_neutral),
            "added_cost_requires_positive_increment": (self.added_cost_requires_positive_increment),
        }


@dataclass(frozen=True, slots=True)
class ClusteredPairedEstimatePolicy:
    registration_id: str
    registration_hash: str
    evaluation_specification_id: str
    evaluation_specification_hash: str
    suite_id: str
    candidate_arm: MethodArm
    comparator_arm: MethodArm
    replicate_count: int
    independent_case_count: int
    critical_value: Decimal
    confidence_level: Decimal
    contrast_role: str
    promotion_eligible: bool

    def __post_init__(self) -> None:
        _sha256(self.registration_hash, "clustered estimate registration hash")
        _sha256(
            self.evaluation_specification_hash,
            "clustered estimate evaluation specification hash",
        )
        if self.registration_id != f"method-quality-benchmark-{self.registration_hash}":
            raise ValueError("clustered estimate registration identity is inconsistent")
        if self.evaluation_specification_id != (
            f"method-quality-evaluation-{self.evaluation_specification_hash}"
        ):
            raise ValueError("clustered estimate evaluation specification identity is inconsistent")
        _identifier(self.suite_id, "clustered estimate suite_id")
        if self.candidate_arm is self.comparator_arm:
            raise ValueError("clustered estimate candidate and comparator arms must differ")
        if self.replicate_count < 1 or self.independent_case_count < 2:
            raise ValueError("clustered estimate registered counts are invalid")
        if not self.critical_value.is_finite() or self.critical_value <= 0:
            raise ValueError("clustered estimate registered critical value is invalid")
        if self.confidence_level != Decimal("0.95"):
            raise ValueError("clustered estimate confidence level must remain 0.95")
        if self.contrast_role not in {"primary_promotion", "secondary_diagnostic"}:
            raise ValueError("clustered estimate contrast role is invalid")
        if self.promotion_eligible != (self.contrast_role == "primary_promotion"):
            raise ValueError("clustered estimate promotion eligibility is inconsistent")


@dataclass(frozen=True, slots=True)
class MethodQualityBenchmarkRegistration:
    schema_version: str
    registration_id: str
    registered_at: datetime
    provider_profile_id: str
    provider_profile_hash: str
    method_catalog_id: str
    method_catalog_hash: str
    immutable_prior_registration_ids: tuple[str, ...]
    development_case_count: int
    retrospective_holdout_case_count: int
    replicate_count: int
    run_order: str
    strata: tuple[MechanismStratum, ...]
    suites: tuple[BenchmarkSuite, ...]
    treatment_bindings: tuple[BenchmarkTreatmentBinding, ...]
    contamination_control: ContaminationControl
    evaluation_specification_id: str
    evaluation_specification_hash: str
    metrics: tuple[str, ...]
    baselines: tuple[str, ...]
    promotion_gate: PromotionGate
    all_event_denominator: bool
    outcomes_opened: bool
    claim_scope: str
    execution_capability: str

    def __post_init__(self) -> None:
        if self.schema_version not in {
            METHOD_QUALITY_BENCHMARK_SCHEMA,
            METHOD_QUALITY_BENCHMARK_SCHEMA_V2,
        }:
            raise ValueError("unsupported Method Quality Benchmark schema_version")
        require_aware(self.registered_at, "method quality registered_at")
        _sha256(self.provider_profile_hash, "method quality provider_profile_hash")
        _sha256(self.method_catalog_hash, "method quality method_catalog_hash")
        _sha256(
            self.evaluation_specification_hash,
            "method quality evaluation specification hash",
        )
        if self.evaluation_specification_id != (
            f"method-quality-evaluation-{self.evaluation_specification_hash}"
        ):
            raise ValueError("method quality evaluation specification identity is inconsistent")
        _unique_nonempty(
            self.immutable_prior_registration_ids,
            "method quality immutable prior registrations",
        )
        if (
            self.schema_version == METHOD_QUALITY_BENCHMARK_SCHEMA_V2
            and RETIRED_METHOD_QUALITY_V1_REGISTRATION_ID
            not in self.immutable_prior_registration_ids
        ):
            raise ValueError("method quality v2 must retain the retired v1 registration")
        if self.development_case_count != 8:
            raise ValueError("method quality development set must contain eight cases")
        if self.retrospective_holdout_case_count != 24:
            raise ValueError("method quality retrospective holdout must contain 24 cases")
        if self.replicate_count != 5:
            raise ValueError("method quality benchmark requires five replicates")
        if self.run_order != "interleaved_by_case_then_replicate_then_arm":
            raise ValueError("method quality benchmark run order is invalid")
        archetypes = tuple(item.event_archetype for item in self.strata)
        if len(archetypes) != len(set(archetypes)):
            raise ValueError("method quality strata archetypes must be unique")
        if sum(item.target_case_count for item in self.strata) != 24:
            raise ValueError("method quality strata must allocate all 24 holdout cases")
        suite_ids = tuple(item.suite_id for item in self.suites)
        if suite_ids != ("general_methods", "family_increment"):
            raise ValueError("method quality benchmark suites are incomplete")
        required_general = (
            MethodArm.NEUTRAL_EVIDENCE,
            MethodArm.GENERAL_METHODS,
            MethodArm.GENERAL_PATTERN,
        )
        if self.suites[0].arms != required_general:
            raise ValueError("general method suite must preserve the three general arms")
        if self.suites[1].arms != tuple(MethodArm):
            raise ValueError("family increment suite must preserve all four arms")
        if self.suites[1].eligible_archetypes != (EventArchetype.PHYSICAL_SUPPLY_LOGISTICS,):
            raise ValueError("first family increment suite is limited to physical supply")
        if self.suites[1].minimum_case_count < 8:
            raise ValueError("family increment suite requires at least eight cases")
        expected_bindings = {(suite.suite_id, arm) for suite in self.suites for arm in suite.arms}
        actual_bindings = {(item.suite_id, item.arm) for item in self.treatment_bindings}
        if actual_bindings != expected_bindings or len(self.treatment_bindings) != len(
            expected_bindings
        ):
            raise ValueError("method quality treatment bindings are incomplete")
        required_metrics = {
            "schema_and_time_gate",
            "evidence_coverage",
            "required_abstention",
            "decision_stability",
            "candidate_directional_score_after_cost_proxy",
            "benchmark_adjusted_directional_score",
            "maximum_directional_score_drawdown",
            "directional_score_cvar95",
            "turnover_proxy",
            "provider_cost",
            "wall_clock_latency",
        }
        if set(self.metrics) != required_metrics or len(self.metrics) != len(required_metrics):
            raise ValueError("method quality metrics are incomplete")
        required_baselines = {
            "neutral_evidence",
            "fixed_exposure",
            "pre_cutoff_momentum",
            "simple_hold",
        }
        if set(self.baselines) != required_baselines or len(self.baselines) != len(
            required_baselines
        ):
            raise ValueError("method quality baselines are incomplete")
        if not self.all_event_denominator:
            raise ValueError("method quality benchmark must use the all-event denominator")
        if self.outcomes_opened:
            raise ValueError("method quality benchmark cannot register opened outcomes")
        if self.claim_scope != "method_quality_only_no_alpha_or_execution":
            raise ValueError("method quality claim scope is invalid")
        if self.execution_capability != "none":
            raise ValueError("method quality benchmark grants no execution capability")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("method quality registration_id does not match content")

    @property
    def registration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_registration_id(self) -> str:
        return f"method-quality-benchmark-{self.registration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registered_at": _timestamp(self.registered_at),
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "method_catalog_id": self.method_catalog_id,
            "method_catalog_hash": self.method_catalog_hash,
            "immutable_prior_registration_ids": list(self.immutable_prior_registration_ids),
            "development_case_count": self.development_case_count,
            "retrospective_holdout_case_count": self.retrospective_holdout_case_count,
            "replicate_count": self.replicate_count,
            "run_order": self.run_order,
            "strata": [item.to_dict() for item in self.strata],
            "suites": [item.to_dict() for item in self.suites],
            "treatment_bindings": [item.to_dict() for item in self.treatment_bindings],
            "contamination_control": self.contamination_control.to_dict(),
            "evaluation_specification_id": self.evaluation_specification_id,
            "evaluation_specification_hash": self.evaluation_specification_hash,
            "metrics": list(self.metrics),
            "baselines": list(self.baselines),
            "promotion_gate": self.promotion_gate.to_dict(),
            "all_event_denominator": self.all_event_denominator,
            "outcomes_opened": self.outcomes_opened,
            "claim_scope": self.claim_scope,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}

    def validate_against(
        self,
        *,
        catalog: ResearchMethodCatalog,
        provider_profile: ModelProviderProfile,
        skills: SkillRegistry,
        evaluation_specification: MethodQualityEvaluationSpecification,
    ) -> None:
        evaluation_schema = _string(
            evaluation_specification.core_dict(),
            "schema_version",
        )
        expected_evaluation_schema = (
            METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA_V2
            if self.schema_version == METHOD_QUALITY_BENCHMARK_SCHEMA_V2
            else METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA
        )
        if evaluation_schema != expected_evaluation_schema:
            raise ValueError("method quality registration and evaluation versions do not match")
        if (
            self.method_catalog_id != catalog.catalog_id
            or self.method_catalog_hash != catalog.catalog_hash
        ):
            raise ValueError("method quality benchmark does not match method catalog")
        if (
            self.provider_profile_id != provider_profile.profile_id
            or self.provider_profile_hash != provider_profile.profile_hash
        ):
            raise ValueError("method quality benchmark does not match provider profile")
        if (
            self.evaluation_specification_id != evaluation_specification.specification_id
            or self.evaluation_specification_hash != evaluation_specification.specification_hash
        ):
            raise ValueError("method quality benchmark does not match evaluation specification")
        router = ResearchMethodRouter(catalog=catalog, skills=skills)
        manifests = {item.name: item for item in skills.discover()}
        for binding in self.treatment_bindings:
            route = router.route(arm=binding.arm, context=binding.context)
            loaded_instruction_hashes = tuple(
                manifests[name].instructions_hash for name in route.loaded_skills
            )
            if (
                binding.route_id != route.route_id
                or binding.route_hash != route.route_hash
                or binding.requested_skills != route.requested_skills
                or binding.loaded_skills != route.loaded_skills
                or binding.manifest_hashes != route.manifest_hashes
                or binding.instruction_hashes != loaded_instruction_hashes
                or binding.allowed_capabilities != route.allowed_capabilities
                or binding.allowed_tools != route.allowed_tools
                or binding.reasons != route.reasons
            ):
                raise ValueError(
                    "method quality treatment does not match catalog and Skill Registry: "
                    f"{binding.suite_id}/{binding.arm.value}"
                )

    def clustered_paired_estimate_policy(
        self,
        *,
        specification: MethodQualityEvaluationSpecification,
        suite_id: str,
        candidate_arm: MethodArm,
        comparator_arm: MethodArm,
    ) -> ClusteredPairedEstimatePolicy:
        if self.schema_version != METHOD_QUALITY_BENCHMARK_SCHEMA_V2:
            raise ValueError("clustered estimates require the active v2 benchmark registration")
        if (
            self.evaluation_specification_id != specification.specification_id
            or self.evaluation_specification_hash != specification.specification_hash
        ):
            raise ValueError("benchmark registration does not match evaluation specification")
        specification_core = specification.core_dict()
        if (
            _string(specification_core, "schema_version")
            != METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA_V2
        ):
            raise ValueError("clustered estimates require the active v2 evaluation specification")

        matching_suites = tuple(item for item in self.suites if item.suite_id == suite_id)
        if len(matching_suites) != 1:
            raise ValueError("clustered estimate suite is not registered")
        suite = matching_suites[0]
        if candidate_arm not in suite.arms or comparator_arm not in suite.arms:
            raise ValueError("clustered estimate arms are not registered for the suite")

        contrast_policy = _object(
            specification_core.get("contrast_policy"),
            "evaluation contrast policy",
        )
        primary = _object(
            contrast_policy.get("primary_promotion_contrast"),
            "primary promotion contrast",
        )
        primary_contrast = (
            _string(primary, "suite_id"),
            MethodArm(_string(primary, "candidate_arm")),
            MethodArm(_string(primary, "comparator_arm")),
        )
        secondary_contrasts = tuple(
            (
                _string(item, "suite_id"),
                MethodArm(_string(item, "candidate_arm")),
                MethodArm(_string(item, "comparator_arm")),
            )
            for item in _object_tuple(
                contrast_policy,
                "secondary_diagnostic_contrasts",
            )
        )
        requested_contrast = (suite_id, candidate_arm, comparator_arm)
        if requested_contrast == primary_contrast:
            contrast_role = "primary_promotion"
        elif requested_contrast in secondary_contrasts:
            contrast_role = "secondary_diagnostic"
        else:
            raise ValueError("clustered estimate is not a registered contrast")

        registered_case_count = sum(
            item.target_case_count
            for item in self.strata
            if item.event_archetype in suite.eligible_archetypes
        )
        if suite.minimum_case_count != registered_case_count:
            raise ValueError("clustered estimate suite case count is not fully registered")

        clustered = _object(
            specification_core.get("clustered_paired_estimator"),
            "evaluation clustered paired estimator",
        )
        critical_values = tuple(
            item
            for item in _object_tuple(clustered, "critical_values_by_suite")
            if _string(item, "suite_id") == suite_id
        )
        if len(critical_values) != 1:
            raise ValueError("clustered estimate critical value is not registered")
        critical_value = critical_values[0]
        if _integer(critical_value, "independent_case_count") != registered_case_count:
            raise ValueError("clustered estimate critical value does not match registered cases")

        return ClusteredPairedEstimatePolicy(
            registration_id=self.registration_id,
            registration_hash=self.registration_hash,
            evaluation_specification_id=specification.specification_id,
            evaluation_specification_hash=specification.specification_hash,
            suite_id=suite_id,
            candidate_arm=candidate_arm,
            comparator_arm=comparator_arm,
            replicate_count=self.replicate_count,
            independent_case_count=registered_case_count,
            critical_value=_decimal(critical_value, "critical_value"),
            confidence_level=_decimal(clustered, "confidence_level"),
            contrast_role=contrast_role,
            promotion_eligible=contrast_role == "primary_promotion",
        )


def load_historical_evidence_manifest(path: Path) -> HistoricalEvidenceManifest:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "historical manifest")
    _closed(
        payload,
        {
            "schema_version",
            "manifest_id",
            "case_alias",
            "split",
            "evidence_pack_id",
            "evidence_pack_hash",
            "as_of",
            "identity_masking",
            "masked_agent_input_manifest_id",
            "masked_agent_input_manifest_hash",
            "provenance_trust_status",
            "outcomes_opened",
            "evidence_versions",
            "external_tool_access",
            "outcome_memory_policy",
            "execution_capability",
        },
        "historical manifest",
    )
    if _string(payload, "schema_version") != HISTORICAL_EVIDENCE_MANIFEST_SCHEMA:
        raise ValueError("unsupported Historical Evidence Manifest schema_version")
    raw_versions = payload.get("evidence_versions")
    if not isinstance(raw_versions, list):
        raise TypeError("historical evidence_versions must be an array")
    result = HistoricalEvidenceManifest(
        manifest_id=_string(payload, "manifest_id"),
        case_alias=_string(payload, "case_alias"),
        split=BenchmarkSplit(_string(payload, "split")),
        evidence_pack_id=_string(payload, "evidence_pack_id"),
        evidence_pack_hash=_string(payload, "evidence_pack_hash"),
        as_of=_datetime(payload, "as_of"),
        identity_masking=IdentityMaskingPolicy(_string(payload, "identity_masking")),
        masked_agent_input_manifest_id=_optional_string(payload, "masked_agent_input_manifest_id"),
        masked_agent_input_manifest_hash=_optional_string(
            payload, "masked_agent_input_manifest_hash"
        ),
        provenance_trust_status=ProvenanceTrustStatus(_string(payload, "provenance_trust_status")),
        outcomes_opened=_boolean(payload, "outcomes_opened"),
        evidence_versions=tuple(
            _historical_version(item) for item in cast(list[object], raw_versions)
        ),
        external_tool_access=_boolean(payload, "external_tool_access"),
        outcome_memory_policy=_string(payload, "outcome_memory_policy"),
        execution_capability=_string(payload, "execution_capability"),
    )
    if result.to_dict() != payload:
        raise ValueError("Historical Evidence Manifest does not match canonical contract")
    return result


def load_source_version_receipt(path: Path) -> SourceVersionReceipt:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "source version receipt")
    result = _source_version_receipt(payload)
    if result.to_dict() != payload:
        raise ValueError("Source Version Receipt does not match canonical contract")
    return result


def load_latency_calibration(path: Path) -> LatencyCalibration:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "latency calibration")
    result = _latency_calibration(payload)
    if result.to_dict() != payload:
        raise ValueError("Latency Calibration does not match canonical contract")
    return result


def load_masked_agent_input_manifest(path: Path) -> MaskedAgentInputManifest:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "masked Agent input")
    _closed(
        payload,
        {
            "schema_version",
            "manifest_id",
            "original_evidence_pack_id",
            "original_evidence_pack_hash",
            "original_documents_hash",
            "original_pattern_packs_hash",
            "masked_evidence_pack_id",
            "masked_evidence_pack_hash",
            "masked_documents_hash",
            "masked_pattern_packs_hash",
            "alias_map",
            "alias_map_hash",
            "forbidden_tokens",
            "forbidden_tokens_hash",
            "agent_visible_fields",
        },
        "masked Agent input",
    )
    if _string(payload, "schema_version") != MASKED_AGENT_INPUT_MANIFEST_SCHEMA:
        raise ValueError("unsupported Masked Agent Input Manifest schema_version")
    raw_aliases = payload.get("alias_map")
    if not isinstance(raw_aliases, list):
        raise TypeError("masked Agent input alias_map must be an array")
    result = MaskedAgentInputManifest(
        manifest_id=_string(payload, "manifest_id"),
        original_evidence_pack_id=_string(payload, "original_evidence_pack_id"),
        original_evidence_pack_hash=_string(payload, "original_evidence_pack_hash"),
        original_documents_hash=_string(payload, "original_documents_hash"),
        original_pattern_packs_hash=_string(payload, "original_pattern_packs_hash"),
        masked_evidence_pack_id=_string(payload, "masked_evidence_pack_id"),
        masked_evidence_pack_hash=_string(payload, "masked_evidence_pack_hash"),
        masked_documents_hash=_string(payload, "masked_documents_hash"),
        masked_pattern_packs_hash=_string(payload, "masked_pattern_packs_hash"),
        alias_map=tuple(_identity_alias(item) for item in cast(list[object], raw_aliases)),
        alias_map_hash=_string(payload, "alias_map_hash"),
        forbidden_tokens=_string_tuple(payload, "forbidden_tokens"),
        forbidden_tokens_hash=_string(payload, "forbidden_tokens_hash"),
        agent_visible_fields=_string_tuple(payload, "agent_visible_fields"),
    )
    if result.to_dict() != payload:
        raise ValueError("Masked Agent Input Manifest does not match canonical contract")
    return result


def load_method_quality_evaluation_specification(
    path: Path,
) -> MethodQualityEvaluationSpecification:
    payload = _object(
        json.loads(path.read_text(encoding="utf-8")),
        "method quality evaluation specification",
    )
    schema_version = _string(payload, "schema_version")
    if schema_version == METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA:
        estimator_fields = {"paired_estimator"}
    elif schema_version == METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA_V2:
        estimator_fields = {"clustered_paired_estimator", "contrast_policy"}
    else:
        raise ValueError("unsupported Method Quality Evaluation Specification schema_version")
    _closed(
        payload,
        {
            "schema_version",
            "specification_id",
            "contract_schemas",
            "horizons_sessions",
            "scoring",
            "case_value_aggregation",
            "attribution",
            "execution_capability",
        }
        | estimator_fields,
        "method quality evaluation specification",
    )
    specification_id = _string(payload, "specification_id")
    core = {key: value for key, value in payload.items() if key != "specification_id"}
    result = MethodQualityEvaluationSpecification(
        specification_id=specification_id,
        canonical_specification_json=json.dumps(
            core,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ),
    )
    if result.to_dict() != payload:
        raise ValueError("Method Quality Evaluation Specification is not canonical")
    return result


def load_method_quality_benchmark(path: Path) -> MethodQualityBenchmarkRegistration:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "method quality benchmark")
    _closed(
        payload,
        {
            "schema_version",
            "registration_id",
            "registered_at",
            "provider_profile_id",
            "provider_profile_hash",
            "method_catalog_id",
            "method_catalog_hash",
            "immutable_prior_registration_ids",
            "development_case_count",
            "retrospective_holdout_case_count",
            "replicate_count",
            "run_order",
            "strata",
            "suites",
            "treatment_bindings",
            "contamination_control",
            "evaluation_specification_id",
            "evaluation_specification_hash",
            "metrics",
            "baselines",
            "promotion_gate",
            "all_event_denominator",
            "outcomes_opened",
            "claim_scope",
            "execution_capability",
        },
        "method quality benchmark",
    )
    schema_version = _string(payload, "schema_version")
    if schema_version not in {
        METHOD_QUALITY_BENCHMARK_SCHEMA,
        METHOD_QUALITY_BENCHMARK_SCHEMA_V2,
    }:
        raise ValueError("unsupported Method Quality Benchmark schema_version")
    raw_strata = payload.get("strata")
    raw_suites = payload.get("suites")
    raw_bindings = payload.get("treatment_bindings")
    if (
        not isinstance(raw_strata, list)
        or not isinstance(raw_suites, list)
        or not isinstance(raw_bindings, list)
    ):
        raise TypeError("method quality strata, suites, and treatment bindings must be arrays")
    result = MethodQualityBenchmarkRegistration(
        schema_version=schema_version,
        registration_id=_string(payload, "registration_id"),
        registered_at=_datetime(payload, "registered_at"),
        provider_profile_id=_string(payload, "provider_profile_id"),
        provider_profile_hash=_string(payload, "provider_profile_hash"),
        method_catalog_id=_string(payload, "method_catalog_id"),
        method_catalog_hash=_string(payload, "method_catalog_hash"),
        immutable_prior_registration_ids=_string_tuple(payload, "immutable_prior_registration_ids"),
        development_case_count=_integer(payload, "development_case_count"),
        retrospective_holdout_case_count=_integer(payload, "retrospective_holdout_case_count"),
        replicate_count=_integer(payload, "replicate_count"),
        run_order=_string(payload, "run_order"),
        strata=tuple(_stratum(item) for item in cast(list[object], raw_strata)),
        suites=tuple(_suite(item) for item in cast(list[object], raw_suites)),
        treatment_bindings=tuple(
            _treatment_binding(item) for item in cast(list[object], raw_bindings)
        ),
        contamination_control=_contamination(payload.get("contamination_control")),
        evaluation_specification_id=_string(payload, "evaluation_specification_id"),
        evaluation_specification_hash=_string(payload, "evaluation_specification_hash"),
        metrics=_string_tuple(payload, "metrics"),
        baselines=_string_tuple(payload, "baselines"),
        promotion_gate=_promotion_gate(payload.get("promotion_gate")),
        all_event_denominator=_boolean(payload, "all_event_denominator"),
        outcomes_opened=_boolean(payload, "outcomes_opened"),
        claim_scope=_string(payload, "claim_scope"),
        execution_capability=_string(payload, "execution_capability"),
    )
    if result.to_dict() != payload:
        raise ValueError("Method Quality Benchmark does not match canonical contract")
    return result


def _historical_version(value: object) -> HistoricalEvidenceVersion:
    payload = _object(value, "historical evidence version")
    _closed(
        payload,
        {
            "evidence_id",
            "claim_id",
            "source_version_id",
            "occurred_at",
            "published_at",
            "source_updated_at",
            "available_at",
            "retrieved_at",
            "availability_basis",
            "source_version_receipt",
            "supersedes_id",
            "content_hash",
        },
        "historical evidence version",
    )
    return HistoricalEvidenceVersion(
        evidence_id=_string(payload, "evidence_id"),
        claim_id=_string(payload, "claim_id"),
        source_version_id=_string(payload, "source_version_id"),
        occurred_at=_datetime(payload, "occurred_at"),
        published_at=_datetime(payload, "published_at"),
        source_updated_at=_optional_datetime(payload, "source_updated_at"),
        available_at=_datetime(payload, "available_at"),
        retrieved_at=_datetime(payload, "retrieved_at"),
        availability_basis=AvailabilityBasis(_string(payload, "availability_basis")),
        source_version_receipt=_source_version_receipt(payload.get("source_version_receipt")),
        supersedes_id=_optional_string(payload, "supersedes_id"),
        content_hash=_string(payload, "content_hash"),
    )


def _source_version_receipt(value: object) -> SourceVersionReceipt:
    payload = _object(value, "source version receipt")
    _closed(
        payload,
        {
            "schema_version",
            "receipt_id",
            "source_ref",
            "provider_id",
            "archive_id",
            "archive_version",
            "source_version_id",
            "raw_content_hash",
            "extracted_content_hash",
            "published_at",
            "source_updated_at",
            "retrieved_at",
            "available_at",
            "availability_basis",
            "latency_calibration",
            "trust_status",
        },
        "source version receipt",
    )
    if _string(payload, "schema_version") != SOURCE_VERSION_RECEIPT_SCHEMA:
        raise ValueError("unsupported Source Version Receipt schema_version")
    raw_calibration = payload.get("latency_calibration")
    return SourceVersionReceipt(
        receipt_id=_string(payload, "receipt_id"),
        source_ref=_string(payload, "source_ref"),
        provider_id=_string(payload, "provider_id"),
        archive_id=_string(payload, "archive_id"),
        archive_version=_string(payload, "archive_version"),
        source_version_id=_string(payload, "source_version_id"),
        raw_content_hash=_string(payload, "raw_content_hash"),
        extracted_content_hash=_string(payload, "extracted_content_hash"),
        published_at=_datetime(payload, "published_at"),
        source_updated_at=_optional_datetime(payload, "source_updated_at"),
        retrieved_at=_datetime(payload, "retrieved_at"),
        available_at=_datetime(payload, "available_at"),
        availability_basis=AvailabilityBasis(_string(payload, "availability_basis")),
        latency_calibration=(
            None if raw_calibration is None else _latency_calibration(raw_calibration)
        ),
        trust_status=ProvenanceTrustStatus(_string(payload, "trust_status")),
    )


def _latency_calibration(value: object) -> LatencyCalibration:
    payload = _object(value, "latency calibration")
    _closed(
        payload,
        {
            "schema_version",
            "calibration_id",
            "source_class",
            "provider_id",
            "archive_id",
            "calibration_version",
            "sample_hash",
            "sample_count",
            "calibrated_at",
            "availability_offset_seconds",
            "trust_status",
        },
        "latency calibration",
    )
    if _string(payload, "schema_version") != LATENCY_CALIBRATION_SCHEMA:
        raise ValueError("unsupported Latency Calibration schema_version")
    return LatencyCalibration(
        calibration_id=_string(payload, "calibration_id"),
        source_class=_string(payload, "source_class"),
        provider_id=_string(payload, "provider_id"),
        archive_id=_string(payload, "archive_id"),
        calibration_version=_string(payload, "calibration_version"),
        sample_hash=_string(payload, "sample_hash"),
        sample_count=_integer(payload, "sample_count"),
        calibrated_at=_datetime(payload, "calibrated_at"),
        availability_offset_seconds=_integer(payload, "availability_offset_seconds"),
        trust_status=ProvenanceTrustStatus(_string(payload, "trust_status")),
    )


def _stratum(value: object) -> MechanismStratum:
    payload = _object(value, "mechanism stratum")
    _closed(
        payload,
        {"event_archetype", "target_case_count", "minimum_required_abstentions"},
        "mechanism stratum",
    )
    return MechanismStratum(
        event_archetype=EventArchetype(_string(payload, "event_archetype")),
        target_case_count=_integer(payload, "target_case_count"),
        minimum_required_abstentions=_integer(payload, "minimum_required_abstentions"),
    )


def _suite(value: object) -> BenchmarkSuite:
    payload = _object(value, "benchmark suite")
    _closed(
        payload,
        {"suite_id", "arms", "eligible_archetypes", "minimum_case_count", "claim"},
        "benchmark suite",
    )
    return BenchmarkSuite(
        suite_id=_string(payload, "suite_id"),
        arms=tuple(MethodArm(item) for item in _string_tuple(payload, "arms")),
        eligible_archetypes=tuple(
            EventArchetype(item) for item in _string_tuple(payload, "eligible_archetypes")
        ),
        minimum_case_count=_integer(payload, "minimum_case_count"),
        claim=_string(payload, "claim"),
    )


def _identity_alias(value: object) -> IdentityAlias:
    payload = _object(value, "identity alias")
    _closed(payload, {"original", "masked"}, "identity alias")
    return IdentityAlias(
        original=_string(payload, "original"),
        masked=_string(payload, "masked"),
    )


def _treatment_binding(value: object) -> BenchmarkTreatmentBinding:
    payload = _object(value, "treatment binding")
    _closed(
        payload,
        {
            "suite_id",
            "arm",
            "context",
            "route_id",
            "route_hash",
            "requested_skills",
            "loaded_skills",
            "manifest_hashes",
            "instruction_hashes",
            "allowed_capabilities",
            "allowed_tools",
            "reasons",
        },
        "treatment binding",
    )
    context = _object(payload.get("context"), "treatment binding context")
    _closed(
        context,
        {"mechanism_family", "asset_class", "has_pattern_pack"},
        "treatment binding context",
    )
    return BenchmarkTreatmentBinding(
        suite_id=_string(payload, "suite_id"),
        arm=MethodArm(_string(payload, "arm")),
        context=ResearchContext(
            mechanism_family=_string(context, "mechanism_family"),
            asset_class=_string(context, "asset_class"),
            has_pattern_pack=_boolean(context, "has_pattern_pack"),
        ),
        route_id=_string(payload, "route_id"),
        route_hash=_string(payload, "route_hash"),
        requested_skills=_string_tuple(payload, "requested_skills"),
        loaded_skills=_string_tuple(payload, "loaded_skills"),
        manifest_hashes=_string_tuple(payload, "manifest_hashes"),
        instruction_hashes=_string_tuple(payload, "instruction_hashes"),
        allowed_capabilities=_string_tuple(payload, "allowed_capabilities"),
        allowed_tools=_string_tuple(payload, "allowed_tools"),
        reasons=_string_tuple(payload, "reasons"),
    )


def _contamination(value: object) -> ContaminationControl:
    payload = _object(value, "contamination control")
    _closed(
        payload,
        {
            "historical_identity_policy",
            "masked_fields",
            "economic_roles_preserved",
            "source_tiers_preserved",
            "prompt_and_tool_aliases_identical",
            "external_network_access",
            "outcome_access_policy",
            "outcome_memory_policy",
        },
        "contamination control",
    )
    return ContaminationControl(
        historical_identity_policy=_string(payload, "historical_identity_policy"),
        masked_fields=_string_tuple(payload, "masked_fields"),
        economic_roles_preserved=_boolean(payload, "economic_roles_preserved"),
        source_tiers_preserved=_boolean(payload, "source_tiers_preserved"),
        prompt_and_tool_aliases_identical=_boolean(payload, "prompt_and_tool_aliases_identical"),
        external_network_access=_boolean(payload, "external_network_access"),
        outcome_access_policy=_string(payload, "outcome_access_policy"),
        outcome_memory_policy=_string(payload, "outcome_memory_policy"),
    )


def _promotion_gate(value: object) -> PromotionGate:
    payload = _object(value, "promotion gate")
    _closed(
        payload,
        {
            "allowed_time_gate_violations",
            "minimum_required_abstention_recall",
            "require_positive_candidate_directional_score",
            "paired_confidence_level",
            "require_positive_paired_lower_bound_vs_neutral",
            "minimum_registered_baselines_beaten",
            "maximum_single_case_absolute_outcome_share",
            "minimum_positive_strata",
            "maximum_drawdown_increase_vs_neutral",
            "maximum_cvar95_increase_vs_neutral",
            "added_cost_requires_positive_increment",
        },
        "promotion gate",
    )
    return PromotionGate(
        allowed_time_gate_violations=_integer(payload, "allowed_time_gate_violations"),
        minimum_required_abstention_recall=_decimal(payload, "minimum_required_abstention_recall"),
        require_positive_candidate_directional_score=_boolean(
            payload, "require_positive_candidate_directional_score"
        ),
        paired_confidence_level=_decimal(payload, "paired_confidence_level"),
        require_positive_paired_lower_bound_vs_neutral=_boolean(
            payload, "require_positive_paired_lower_bound_vs_neutral"
        ),
        minimum_registered_baselines_beaten=_integer(
            payload, "minimum_registered_baselines_beaten"
        ),
        maximum_single_case_absolute_outcome_share=_decimal(
            payload, "maximum_single_case_absolute_outcome_share"
        ),
        minimum_positive_strata=_integer(payload, "minimum_positive_strata"),
        maximum_drawdown_increase_vs_neutral=_decimal(
            payload, "maximum_drawdown_increase_vs_neutral"
        ),
        maximum_cvar95_increase_vs_neutral=_decimal(payload, "maximum_cvar95_increase_vs_neutral"),
        added_cost_requires_positive_increment=_boolean(
            payload, "added_cost_requires_positive_increment"
        ),
    )


def _validate_revision_lineage(versions: tuple[HistoricalEvidenceVersion, ...]) -> None:
    by_id = {item.evidence_id: item for item in versions}
    successors: dict[str, int] = {}
    for item in versions:
        if item.supersedes_id is None:
            continue
        previous = by_id.get(item.supersedes_id)
        if previous is None:
            raise ValueError(
                f"historical revision supersedes unknown evidence: {item.supersedes_id}"
            )
        if item.claim_id != previous.claim_id:
            raise ValueError("historical revisions must retain claim_id")
        if item.available_at <= previous.available_at:
            raise ValueError("historical revisions must advance availability")
        successors[previous.evidence_id] = successors.get(previous.evidence_id, 0) + 1
        if successors[previous.evidence_id] > 1:
            raise ValueError("historical evidence revisions cannot fork")


def _validate_evaluation_specification_core(core: dict[str, object]) -> None:
    schema_version = _string(core, "schema_version")
    if schema_version == METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA:
        estimator_field = "paired_estimator"
        extra_fields: set[str] = set()
    elif schema_version == METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA_V2:
        estimator_field = "clustered_paired_estimator"
        extra_fields = {"contrast_policy"}
    else:
        raise ValueError("unsupported Method Quality Evaluation Specification schema_version")
    _closed(
        core,
        {
            "schema_version",
            "contract_schemas",
            "horizons_sessions",
            "scoring",
            "case_value_aggregation",
            "attribution",
            estimator_field,
            "execution_capability",
        }
        | extra_fields,
        "evaluation specification core",
    )
    if _string(core, "execution_capability") != "none":
        raise ValueError("method quality evaluation grants no execution capability")
    if _integer_tuple(core, "horizons_sessions") != (1, 3, 10):
        raise ValueError("method quality evaluation horizons must remain 1, 3, and 10 sessions")
    contracts = _object(core.get("contract_schemas"), "evaluation contract schemas")
    _closed(
        contracts,
        {"market_snapshot", "outcome_seal", "outcome_opening"},
        "evaluation contract schemas",
    )
    expected_contracts = {
        "market_snapshot": "market-impact.method-quality-market-snapshot.v1",
        "outcome_seal": "market-impact.method-quality-outcome-seal.v1",
        "outcome_opening": "market-impact.method-quality-outcome-opening.v1",
    }
    if any(_string(contracts, name) != value for name, value in expected_contracts.items()):
        raise ValueError("evaluation contract schema versions are invalid")

    scoring = _object(core.get("scoring"), "evaluation scoring")
    _closed(
        scoring,
        {
            "procedure",
            "data_granularity",
            "notional_currency",
            "research_notional_budget",
            "random_seed",
            "entry_price",
            "entry_search_limit_sessions",
            "entry_failure_value",
            "quantity",
            "exit_price",
            "missing_exit_value",
            "direction_multiplier",
            "price_move_ratio",
            "directional_score",
            "cost_proxy",
            "benchmark_move_ratio",
            "benchmark_adjusted_directional_score",
            "cost_proxy_rounding",
            "venue_rule_selection",
        },
        "evaluation scoring",
    )
    expected_scoring = {
        "procedure": "deterministic_directional_research_score",
        "data_granularity": "1_day_adjusted",
        "notional_currency": "CNY",
        "research_notional_budget": "1000000",
    }
    if any(_string(scoring, name) != value for name, value in expected_scoring.items()):
        raise ValueError("evaluation scoring constants are invalid")
    if _integer(scoring, "random_seed") != 0:
        raise ValueError("evaluation scoring random seed must remain zero")
    if _integer(scoring, "entry_search_limit_sessions") != 3:
        raise ValueError("evaluation entry search limit must remain three sessions")
    exact_scoring = {
        "entry_price": "first_eligible_session_open",
        "entry_failure_value": "zero_retained_in_all_event_denominator",
        "quantity": "largest_whole_effective_board_lot_affordable_after_entry_fees",
        "exit_price": "adjusted_close_exactly_horizon_sessions_after_entry",
        "missing_exit_value": "zero_retained_in_all_event_denominator",
        "direction_multiplier": "up=1;down=-1;mixed=0;unknown=0",
        "price_move_ratio": "(exit_reference_value-entry_reference_value)/entry_reference_value",
        "directional_score": "direction_multiplier*price_move_ratio",
        "cost_proxy": "sum(cost_component_amount)/entry_reference_value",
        "benchmark_move_ratio": (
            "(benchmark_exit_close-benchmark_entry_open)/benchmark_entry_open"
        ),
        "benchmark_adjusted_directional_score": (
            "directional_score-cost_proxy-direction_multiplier*benchmark_move_ratio"
        ),
        "cost_proxy_rounding": (
            "max(rate*side_reference_value,minimum_amount)_quantized_half_up_per_component"
        ),
        "venue_rule_selection": "exactly_one_effective_row_or_fail_closed",
    }
    if any(_string(scoring, name) != value for name, value in exact_scoring.items()):
        raise ValueError("evaluation scoring equations are invalid")

    aggregation = _object(core.get("case_value_aggregation"), "case value aggregation")
    expected_aggregation = {
        "candidate_rows": "exact_artifact_candidate_target_and_declared_horizon_set",
        "multiple_candidate_rule": (
            "arithmetic_mean_of_all_candidate_benchmark_adjusted_directional_scores_no_selection_or_reweighting"
        ),
        "abstain_value": "0",
        "mixed_value": "0",
        "unknown_value": "0",
        "no_fill_value": "0",
        "missing_market_data_value": "0",
        "denominator": "every_registered_case_replicate_arm_exactly_once",
    }
    _closed(aggregation, set(expected_aggregation), "case value aggregation")
    if any(_string(aggregation, name) != value for name, value in expected_aggregation.items()):
        raise ValueError("case value aggregation rules are invalid")

    attribution = _object(core.get("attribution"), "evaluation attribution")
    _closed(
        attribution,
        {"status", "promotion_dependency", "promotion_metric", "reason"},
        "evaluation attribution",
    )
    deferred_reason = (
        "style_universe_lag_breakpoints_weights_rebalance_missing_and_regression_inputs_not_frozen"
    )
    if (
        _string(attribution, "status") != "deferred_diagnostic_only"
        or _boolean(attribution, "promotion_dependency")
        or attribution.get("promotion_metric") is not None
        or _string(attribution, "reason") != deferred_reason
    ):
        raise ValueError("style attribution must remain deferred from promotion")

    if schema_version == METHOD_QUALITY_EVALUATION_SPECIFICATION_SCHEMA_V2:
        _validate_clustered_estimator(core)
        return

    paired = _object(core.get("paired_estimator"), "evaluation paired estimator")
    _closed(
        paired,
        {
            "pairing_unit",
            "candidate_input",
            "comparator_input",
            "difference",
            "point_estimator",
            "sample_variance",
            "standard_error",
            "interval_lower",
            "confidence_level",
            "critical_value_source",
            "critical_values_by_suite",
            "missing_pair_action",
            "paired_gate_rule",
        },
        "evaluation paired estimator",
    )
    expected_paired = {
        "pairing_unit": "case_replicate",
        "candidate_input": "case_replicate_arm_value",
        "comparator_input": "same_case_replicate_neutral_evidence_value",
        "difference": "candidate_input-comparator_input",
        "point_estimator": "sum(difference)/n",
        "sample_variance": "sum((difference-point_estimator)^2)/(n-1)",
        "standard_error": "sqrt(sample_variance/n)",
        "interval_lower": "point_estimator-critical_value*standard_error",
        "confidence_level": "0.95",
        "critical_value_source": ("NIST_SEMATECH_e_Handbook_1_3_6_7_2_two_sided_0.05_table"),
        "missing_pair_action": "inconclusive_no_promotion_no_pair_deletion",
        "paired_gate_rule": (
            "mean_candidate_directional_score>0_and_interval_lower>0_else_gate_not_passed"
        ),
    }
    if any(_string(paired, name) != value for name, value in expected_paired.items()):
        raise ValueError("evaluation paired estimator is invalid")
    critical_values = _object_tuple(paired, "critical_values_by_suite")
    expected_critical_values = (
        ("general_methods", 120, 119, Decimal("1.980")),
        ("family_increment", 40, 39, Decimal("2.023")),
    )
    actual_critical_values = tuple(
        (
            _string(item, "suite_id"),
            _integer(item, "pair_count"),
            _integer(item, "degrees_of_freedom"),
            _decimal(item, "critical_value"),
        )
        for item in critical_values
    )
    if actual_critical_values != expected_critical_values:
        raise ValueError("evaluation paired critical values are invalid")


def _validate_clustered_estimator(core: dict[str, object]) -> None:
    clustered = _object(
        core.get("clustered_paired_estimator"),
        "evaluation clustered paired estimator",
    )
    expected_clustered = {
        "independent_unit": "event_case",
        "replicate_role": "within_case_stochastic_measurement_not_independent_observation",
        "arm_case_value": "arithmetic_mean_of_all_five_case_replicate_arm_values",
        "pairing_unit": "event_case",
        "candidate_input": "candidate_arm_case_value",
        "comparator_input": "comparator_arm_case_value",
        "difference": "candidate_input-comparator_input",
        "point_estimator": "sum(case_difference)/independent_case_count",
        "sample_variance": ("sum((case_difference-point_estimator)^2)/(independent_case_count-1)"),
        "standard_error": "sqrt(sample_variance/independent_case_count)",
        "interval_lower": "point_estimator-critical_value*standard_error",
        "confidence_level": "0.95",
        "critical_value_source": ("NIST_SEMATECH_e_Handbook_1_3_6_7_2_two_sided_0.05_table"),
        "missing_pair_action": "inconclusive_no_promotion_no_pair_deletion",
        "cluster_gate_rule": (
            "mean_candidate_directional_score>0_and_interval_lower>0_else_gate_not_passed"
        ),
    }
    _closed(
        clustered,
        set(expected_clustered) | {"critical_values_by_suite"},
        "evaluation clustered paired estimator",
    )
    if any(_string(clustered, name) != value for name, value in expected_clustered.items()):
        raise ValueError("evaluation clustered paired estimator is invalid")
    critical_values = _object_tuple(clustered, "critical_values_by_suite")
    expected_critical_values = (
        ("general_methods", 24, 23, Decimal("2.069")),
        ("family_increment", 8, 7, Decimal("2.365")),
    )
    actual_critical_values = tuple(
        (
            _string(item, "suite_id"),
            _integer(item, "independent_case_count"),
            _integer(item, "degrees_of_freedom"),
            _decimal(item, "critical_value"),
        )
        for item in critical_values
    )
    if actual_critical_values != expected_critical_values:
        raise ValueError("evaluation clustered critical values are invalid")

    contrast = _object(core.get("contrast_policy"), "evaluation contrast policy")
    _closed(
        contrast,
        {
            "primary_promotion_contrast",
            "secondary_diagnostic_contrasts",
            "selection_policy",
            "secondary_claim_action",
        },
        "evaluation contrast policy",
    )
    primary = _object(
        contrast.get("primary_promotion_contrast"),
        "primary promotion contrast",
    )
    _closed(primary, {"suite_id", "candidate_arm", "comparator_arm"}, "primary contrast")
    if (
        _string(primary, "suite_id") != "general_methods"
        or _string(primary, "candidate_arm") != "general_methods"
        or _string(primary, "comparator_arm") != "neutral_evidence"
    ):
        raise ValueError("evaluation primary promotion contrast is invalid")
    secondary = _object_tuple(contrast, "secondary_diagnostic_contrasts")
    actual_secondary = tuple(
        (
            _string(item, "suite_id"),
            _string(item, "candidate_arm"),
            _string(item, "comparator_arm"),
        )
        for item in secondary
    )
    expected_secondary = (
        ("general_methods", "general_pattern", "general_methods"),
        ("family_increment", "family_guided", "general_pattern"),
    )
    if actual_secondary != expected_secondary:
        raise ValueError("evaluation secondary diagnostic contrasts are invalid")
    if (
        _string(contrast, "selection_policy") != "no_best_observed_arm_selection"
        or _string(contrast, "secondary_claim_action")
        != "no_promotion_without_new_prospective_preregistration"
    ):
        raise ValueError("evaluation contrast claim policy is invalid")


def _validate_document_bindings(
    pack: EvidencePack,
    document_payload: dict[str, object],
) -> None:
    documents = document_payload.get("documents")
    if not isinstance(documents, dict):
        raise TypeError("evidence document payload requires a documents object")
    raw_documents = cast(dict[object, object], documents)
    if any(not isinstance(key, str) for key in raw_documents):
        raise TypeError("evidence document ids must be strings")
    by_id = cast(dict[str, object], raw_documents)
    if set(by_id) != {item.evidence_id for item in pack.evidence}:
        raise ValueError("evidence documents must exactly match the Evidence Pack")
    for reference in pack.evidence:
        if canonical_hash(by_id[reference.evidence_id]) != reference.content_hash:
            raise ValueError(f"evidence document content hash mismatch: {reference.evidence_id}")


def _validate_pattern_bindings(
    pack: EvidencePack,
    pattern_packs: tuple[PatternPack, ...],
) -> None:
    by_id = {item.pack_id: item for item in pattern_packs}
    if len(by_id) != len(pattern_packs) or set(by_id) != {
        item.pack_id for item in pack.pattern_packs
    }:
        raise ValueError("Pattern Pack documents must exactly match the Evidence Pack")
    for reference in pack.pattern_packs:
        pattern = by_id[reference.pack_id]
        if pattern.version != reference.version:
            raise ValueError(f"Pattern Pack version mismatch: {reference.pack_id}")
        if canonical_hash(pattern.to_dict()) != reference.content_hash:
            raise ValueError(f"Pattern Pack content hash mismatch: {reference.pack_id}")


def _validate_masked_pattern_packs(
    original: tuple[PatternPack, ...],
    masked: tuple[PatternPack, ...],
    aliases: tuple[tuple[str, str], ...],
) -> None:
    if len(original) != len(masked):
        raise ValueError("masked Pattern Pack documents do not match original documents")
    for source, actual in zip(original, masked, strict=True):
        expected_core = _apply_aliases(source.core_dict(), aliases)
        if actual.core_dict() != expected_core:
            raise ValueError("masked Pattern Pack is not the registered alias transform")
        expected_id = f"pattern-{canonical_hash(expected_core)}"
        if actual.pack_id != expected_id:
            raise ValueError("masked Pattern Pack identity does not match transformed content")


def _validate_masked_pack_fields(
    original: EvidencePack,
    original_documents: dict[str, object],
    original_pattern_packs: tuple[PatternPack, ...],
    masked: EvidencePack,
    masked_documents: dict[str, object],
    masked_pattern_packs: tuple[PatternPack, ...],
    aliases: tuple[tuple[str, str], ...],
) -> None:
    expected_core = cast(dict[str, object], _apply_aliases(original.core_dict(), aliases))
    expected_evidence = cast(list[object], expected_core["evidence"])
    masked_document_payload = cast(dict[str, object], masked_documents["documents"])
    for item in expected_evidence:
        reference = cast(dict[str, object], item)
        evidence_id = cast(str, reference["evidence_id"])
        reference["content_hash"] = canonical_hash(masked_document_payload[evidence_id])
    expected_patterns = cast(list[object], expected_core["pattern_packs"])
    for reference_value, pattern in zip(expected_patterns, masked_pattern_packs, strict=True):
        reference = cast(dict[str, object], reference_value)
        reference["pack_id"] = pattern.pack_id
        reference["content_hash"] = canonical_hash(pattern.to_dict())
    expected_pack = {**expected_core, "pack_id": f"evidence-pack-{canonical_hash(expected_core)}"}
    if masked.to_dict() != expected_pack:
        raise ValueError("masked Evidence Pack is not the registered full alias transform")

    expected_documents = _apply_aliases(original_documents, aliases)
    if expected_documents != masked_documents:
        raise ValueError("masked evidence documents are not the registered alias transform")
    if len(original_pattern_packs) != len(masked_pattern_packs):
        raise ValueError("masked Pattern Pack documents do not match original documents")


def _apply_aliases(value: object, aliases: tuple[tuple[str, str], ...]) -> object:
    if isinstance(value, str):
        result = value
        for original, masked in sorted(aliases, key=lambda item: len(item[0]), reverse=True):
            result = result.replace(original, masked)
        return result
    if isinstance(value, list):
        return [_apply_aliases(item, aliases) for item in cast(list[object], value)]
    if isinstance(value, dict):
        payload = cast(dict[str, object], value)
        transformed: dict[str, object] = {}
        for key, item in payload.items():
            masked_key = cast(str, _apply_aliases(key, aliases))
            if masked_key in transformed:
                raise ValueError("alias transform creates a duplicate object key")
            transformed[masked_key] = _apply_aliases(item, aliases)
        return transformed
    return value


def _contains_token(value: object, token: str) -> bool:
    normalized = token.casefold()
    if isinstance(value, str):
        return normalized in value.casefold()
    if isinstance(value, list):
        return any(_contains_token(item, token) for item in cast(list[object], value))
    if isinstance(value, dict):
        payload = cast(dict[object, object], value)
        return any(
            _contains_token(key, token) or _contains_token(item, token)
            for key, item in payload.items()
        )
    return False


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object")
    mapping = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in mapping):
        raise TypeError(f"{label} must have string keys")
    return cast(dict[str, object], mapping)


def _closed(payload: dict[str, object], expected: set[str], label: str) -> None:
    if set(payload) != expected:
        raise ValueError(f"{label} fields are invalid")


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _optional_string(payload: dict[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be null or a non-empty trimmed string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _string_tuple(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], items))


def _integer_tuple(payload: dict[str, object], name: str) -> tuple[int, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of integers")
    items = cast(list[object], value)
    if any(isinstance(item, bool) or not isinstance(item, int) for item in items):
        raise TypeError(f"{name} must be an array of integers")
    return tuple(cast(list[int], items))


def _object_tuple(payload: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of objects")
    return tuple(_object(item, name) for item in cast(list[object], value))


def _datetime(payload: dict[str, object], name: str) -> datetime:
    return datetime.fromisoformat(_string(payload, name).replace("Z", "+00:00"))


def _optional_datetime(payload: dict[str, object], name: str) -> datetime | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be null or a timestamp string")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _decimal(payload: dict[str, object], name: str) -> Decimal:
    return Decimal(_string(payload, name))


def _identifier(value: str, label: str) -> None:
    _nonempty(value, label)
    if any(character not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for character in value):
        raise ValueError(f"{label} must be a lowercase identifier")


def _unique_nonempty(values: tuple[str, ...], label: str) -> None:
    for value in values:
        _nonempty(value, label)
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")


def _nonempty(value: str, label: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{label} must be a non-empty trimmed string")


def _sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
