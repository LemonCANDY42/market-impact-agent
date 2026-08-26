from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from market_impact_agent.accrual import AccrualDecision, AccrualDisposition, AccrualLedger
from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    PatternPack,
    PatternPackReference,
    canonical_hash,
    canonical_json_bytes,
    pattern_pack_from_dict,
)
from market_impact_agent.agent_study import ExposureRegistry
from market_impact_agent.domain import require_aware
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.research import EvidenceTier

PROSPECTIVE_FREEZE_MANIFEST_SCHEMA = "market-impact.prospective-freeze-manifest.v1"


@dataclass(frozen=True, slots=True)
class FrozenEvidenceBundle:
    accrued_event_id: str
    evidence_pack: EvidencePack
    root: Path
    manifest_path: Path
    already_existed: bool


@dataclass(frozen=True, slots=True)
class EvidenceFreezeBatch:
    frozen: tuple[FrozenEvidenceBundle, ...]
    pending_event_ids: tuple[str, ...]


def freeze_due_evidence_packs(
    *,
    ledger: AccrualLedger,
    registry: ExposureRegistry,
    pattern_pack_paths: tuple[Path, ...],
    output_root: Path,
    now: datetime,
) -> EvidenceFreezeBatch:
    require_aware(now, "freeze scheduler now")
    resolved_now = now.astimezone(UTC)
    patterns = _load_patterns(pattern_pack_paths)
    root = output_root.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    decisions = ledger.decisions()
    admitted = tuple(item for item in decisions if item.disposition is AccrualDisposition.ACCRUED)
    frozen: list[FrozenEvidenceBundle] = []
    pending: list[str] = []
    for decision in admitted:
        if decision.accrued_event_id is None or decision.evidence_cutoff_at is None:
            raise ValueError("accrued decision is missing freeze identity")
        if resolved_now < decision.evidence_cutoff_at:
            pending.append(decision.accrued_event_id)
            continue
        frozen.append(
            _freeze_one(
                ledger=ledger,
                registry=registry,
                decision=decision,
                all_decisions=decisions,
                patterns=patterns,
                root=root / decision.accrued_event_id,
                frozen_at=resolved_now,
            )
        )
    return EvidenceFreezeBatch(
        frozen=tuple(frozen),
        pending_event_ids=tuple(pending),
    )


def _freeze_one(
    *,
    ledger: AccrualLedger,
    registry: ExposureRegistry,
    decision: AccrualDecision,
    all_decisions: tuple[AccrualDecision, ...],
    patterns: tuple[PatternPack, ...],
    root: Path,
    frozen_at: datetime,
) -> FrozenEvidenceBundle:
    assert decision.accrued_event_id is not None
    assert decision.evidence_cutoff_at is not None
    if root.exists():
        return _load_existing_bundle(root, decision.accrued_event_id)
    if registry.as_of > decision.evidence_cutoff_at:
        raise ValueError("Exposure Registry was not available by evidence cutoff")
    for pattern in patterns:
        if pattern.available_at > decision.evidence_cutoff_at:
            raise ValueError(f"Pattern Pack is future-visible: {pattern.pack_id}")
    event_decisions = tuple(
        item
        for item in all_decisions
        if item.observation.event_id == decision.observation.event_id
        and item.observation.source.available_at <= decision.evidence_cutoff_at
    )
    documents: dict[str, object] = {}
    references: list[EvidenceReference] = []
    raw_hashes: dict[str, str] = {}
    for item in event_decisions:
        observation = item.observation
        evidence_id = f"candidate-evidence-{observation.observation_hash}"
        document = {
            "schema_version": "market-impact.candidate-event-evidence.v1",
            "accrued_event_id": decision.accrued_event_id,
            "event_id": observation.event_id,
            "observation": observation.to_dict(),
            "coverage_receipt": item.coverage_receipt.to_dict(),
        }
        documents[evidence_id] = document
        raw_hashes[evidence_id] = observation.source.raw_content_hash
        references.append(
            EvidenceReference(
                evidence_id=evidence_id,
                claim_id=observation.event_id,
                source_ref=observation.source.source_ref,
                source_tier=observation.source.source_tier,
                available_at=observation.source.available_at,
                content_hash=canonical_hash(document),
                summary=observation.source.claim_summary,
                untrusted_text=True,
            )
        )
    registry_evidence_id = f"exposure-registry-evidence-{registry.registry_hash}"
    registry_document = {
        "schema_version": "market-impact.exposure-registry-evidence.v1",
        "registry": registry.to_dict(),
    }
    documents[registry_evidence_id] = registry_document
    references.append(
        EvidenceReference(
            evidence_id=registry_evidence_id,
            claim_id="registered-a-share-energy-exposures",
            source_ref=f"exposure-registry://{registry.registry_id}",
            source_tier=EvidenceTier.PRIMARY,
            available_at=registry.as_of,
            content_hash=canonical_hash(registry_document),
            summary=(
                "Pre-outcome registry constrains eligible A-share upstream targets, "
                "applicability conditions, and offsets."
            ),
            untrusted_text=True,
        )
    )
    pattern_references = tuple(
        PatternPackReference(
            pack_id=item.pack_id,
            version=item.version,
            available_at=item.available_at,
            content_hash=canonical_hash(item.to_dict()),
        )
        for item in patterns
    )
    allowed_targets = tuple(
        item.instrument_id
        for item in registry.entries
        if item.selection_eligible and item.eligible_from <= decision.evidence_cutoff_at.date()
    )
    if not allowed_targets:
        raise ValueError("no Exposure Registry target is eligible at evidence cutoff")
    data_gaps = tuple(
        sorted(
            {
                *ledger.coverage_registration.known_blind_spots,
                (
                    "Benchmark, macro, positioning, and offset-capacity evidence adapters "
                    "are not registered in this first source-monitoring slice."
                ),
            }
        )
    )
    evidence_pack = EvidencePack.build(
        event_id=decision.accrued_event_id,
        as_of=decision.evidence_cutoff_at,
        research_question=(
            "Given only evidence actually available by the registered cutoff, which eligible "
            "A-share upstream exposure has a defensible long direction and 1, 3, or 10-session "
            "horizon, or should the study abstain?"
        ),
        evidence=tuple(sorted(references, key=lambda item: (item.available_at, item.evidence_id))),
        pattern_packs=pattern_references,
        allowed_targets=allowed_targets,
        data_gaps=data_gaps,
    )
    documents_payload = {
        "schema_version": "market-impact.frozen-evidence-documents.v1",
        "documents": documents,
    }
    manifest_core = {
        "schema_version": PROSPECTIVE_FREEZE_MANIFEST_SCHEMA,
        "accrued_event_id": decision.accrued_event_id,
        "prospective_registration_id": ledger.registration.registration_id,
        "prospective_registration_hash": ledger.registration.registration_hash,
        "source_coverage_registration_id": (ledger.coverage_registration.coverage_registration_id),
        "source_coverage_registration_hash": (
            ledger.coverage_registration.coverage_registration_hash
        ),
        "exposure_registry_id": registry.registry_id,
        "exposure_registry_hash": registry.registry_hash,
        "accrual_decision_hash": decision.decision_hash,
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(evidence_pack.to_dict()),
        "evidence_documents_hash": canonical_hash(documents_payload),
        "pattern_pack_hashes": {item.pack_id: canonical_hash(item.to_dict()) for item in patterns},
        "raw_source_hashes": raw_hashes,
        "evidence_cutoff_at": _timestamp(decision.evidence_cutoff_at),
    }
    manifest = {
        **manifest_core,
        "freeze_id": f"prospective-freeze-{canonical_hash(manifest_core)}",
        "frozen_at": _timestamp(frozen_at),
        "execution_capability": "none",
    }
    temporary = Path(tempfile.mkdtemp(prefix=".tmp-freeze-", dir=root.parent))
    try:
        os.chmod(temporary, 0o700)
        pattern_root = temporary / "pattern-packs"
        pattern_root.mkdir(mode=0o700)
        _write_private(temporary / "evidence-pack.json", evidence_pack.to_dict())
        _write_private(temporary / "evidence-documents.json", documents_payload)
        for pattern in patterns:
            _write_private(pattern_root / f"{pattern.pack_id}.json", pattern.to_dict())
        _write_private(temporary / "freeze-manifest.json", manifest)
        _validate_bundle(temporary)
        temporary.replace(root)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return FrozenEvidenceBundle(
        accrued_event_id=decision.accrued_event_id,
        evidence_pack=evidence_pack,
        root=root,
        manifest_path=root / "freeze-manifest.json",
        already_existed=False,
    )


def _load_existing_bundle(root: Path, accrued_event_id: str) -> FrozenEvidenceBundle:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("existing prospective freeze root must be a regular directory")
    repository = _validate_bundle(root)
    if repository.evidence_pack.event_id != accrued_event_id:
        raise ValueError("existing Evidence Pack has a different Accrued Event")
    return FrozenEvidenceBundle(
        accrued_event_id=accrued_event_id,
        evidence_pack=repository.evidence_pack,
        root=root,
        manifest_path=root / "freeze-manifest.json",
        already_existed=True,
    )


def _validate_bundle(root: Path) -> FrozenResearchRepository:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("prospective freeze root must be a regular directory")
    pack_path = root / "evidence-pack.json"
    documents_path = root / "evidence-documents.json"
    manifest_path = root / "freeze-manifest.json"
    pattern_paths = tuple(sorted((root / "pattern-packs").glob("*.json")))
    for path in (pack_path, documents_path, manifest_path, *pattern_paths):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"prospective freeze artifact is not a regular file: {path.name}")
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=pack_path,
        evidence_documents_path=documents_path,
        pattern_pack_paths=pattern_paths,
    )
    manifest = _read_object(manifest_path)
    manifest_keys = {
        "schema_version",
        "freeze_id",
        "accrued_event_id",
        "prospective_registration_id",
        "prospective_registration_hash",
        "source_coverage_registration_id",
        "source_coverage_registration_hash",
        "exposure_registry_id",
        "exposure_registry_hash",
        "accrual_decision_hash",
        "evidence_pack_id",
        "evidence_pack_hash",
        "evidence_documents_hash",
        "pattern_pack_hashes",
        "raw_source_hashes",
        "evidence_cutoff_at",
        "frozen_at",
        "execution_capability",
    }
    if set(manifest) != manifest_keys:
        raise ValueError("freeze manifest fields are invalid")
    if manifest.get("schema_version") != PROSPECTIVE_FREEZE_MANIFEST_SCHEMA:
        raise ValueError("freeze manifest schema is invalid")
    if manifest.get("execution_capability") != "none":
        raise ValueError("freeze manifest execution capability is invalid")
    manifest_core = {
        key: value
        for key, value in manifest.items()
        if key not in {"freeze_id", "frozen_at", "execution_capability"}
    }
    if manifest.get("freeze_id") != f"prospective-freeze-{canonical_hash(manifest_core)}":
        raise ValueError("freeze manifest identity is invalid")
    if manifest.get("evidence_pack_id") != repository.evidence_pack.pack_id:
        raise ValueError("freeze manifest Evidence Pack identity is invalid")
    if manifest.get("evidence_pack_hash") != canonical_hash(repository.evidence_pack.to_dict()):
        raise ValueError("freeze manifest Evidence Pack hash is invalid")
    documents_payload = _read_object(documents_path)
    if manifest.get("evidence_documents_hash") != canonical_hash(documents_payload):
        raise ValueError("freeze manifest evidence documents hash is invalid")
    if manifest.get("accrued_event_id") != repository.evidence_pack.event_id:
        raise ValueError("freeze manifest Accrued Event identity is invalid")
    if manifest.get("evidence_cutoff_at") != _timestamp(repository.evidence_pack.as_of):
        raise ValueError("freeze manifest evidence cutoff is invalid")
    frozen_at = _parse_timestamp(manifest.get("frozen_at"), "frozen_at")
    if frozen_at < repository.evidence_pack.as_of:
        raise ValueError("freeze manifest predates the evidence cutoff")
    expected_patterns = {
        item.pack_id: item.content_hash for item in repository.evidence_pack.pattern_packs
    }
    if _string_map(manifest.get("pattern_pack_hashes"), "pattern_pack_hashes") != (
        expected_patterns
    ):
        raise ValueError("freeze manifest Pattern Pack hashes are invalid")
    documents = documents_payload.get("documents")
    if not isinstance(documents, dict):
        raise TypeError("frozen evidence document file requires a documents object")
    expected_raw_hashes: dict[str, str] = {}
    for evidence_id, document in cast(dict[object, object], documents).items():
        if not isinstance(evidence_id, str) or not isinstance(document, dict):
            continue
        observation = cast(dict[object, object], document).get("observation")
        if not isinstance(observation, dict):
            continue
        source = cast(dict[object, object], observation).get("source")
        if not isinstance(source, dict):
            continue
        raw_hash = cast(dict[object, object], source).get("raw_content_hash")
        if not isinstance(raw_hash, str):
            raise TypeError("frozen Candidate Event evidence raw hash is invalid")
        expected_raw_hashes[evidence_id] = raw_hash
    if _string_map(manifest.get("raw_source_hashes"), "raw_source_hashes") != (expected_raw_hashes):
        raise ValueError("freeze manifest raw source hashes are invalid")
    return repository


def _load_patterns(paths: tuple[Path, ...]) -> tuple[PatternPack, ...]:
    if not paths:
        raise ValueError("at least one Pattern Pack is required")
    patterns: list[PatternPack] = []
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Pattern Pack path must be a regular file: {path}")
        patterns.append(pattern_pack_from_dict(_read_object(path)))
    ids = tuple(item.pack_id for item in patterns)
    if len(ids) != len(set(ids)):
        raise ValueError("Pattern Pack inputs contain duplicate identities")
    return tuple(sorted(patterns, key=lambda item: item.pack_id))


def _read_object(path: Path) -> dict[str, object]:
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"JSON artifact must be an object: {path}")
    raw = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"JSON artifact keys must be strings: {path}")
    return cast(dict[str, object], payload)


def _write_private(path: Path, payload: object) -> None:
    path.write_bytes(canonical_json_bytes(payload))
    os.chmod(path, 0o600)


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"freeze manifest {name} must be a timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, f"freeze manifest {name}")
    return parsed.astimezone(UTC)


def _string_map(value: object, name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise TypeError(f"freeze manifest {name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) or not isinstance(item, str) for key, item in raw.items()):
        raise TypeError(f"freeze manifest {name} values must be strings")
    return cast(dict[str, str], value)
