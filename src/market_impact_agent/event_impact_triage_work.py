from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.event_impact_triage import EventImpactTriageCandidateSet

if TYPE_CHECKING:
    from market_impact_agent.event_impact_triage_runtime import TriageCandidateContent

EVENT_IMPACT_TRIAGE_WORK_MANIFEST_SCHEMA = "market-impact.event-impact-triage-work-manifest.v1"
EVENT_IMPACT_TRIAGE_CANDIDATE_DIGEST_SCHEMA = (
    "market-impact.event-impact-triage-candidate-digest.v1"
)
EVENT_IMPACT_TRIAGE_CLUSTER_PARTITION_SCHEMA = (
    "market-impact.event-impact-triage-cluster-partition.v1"
)
TRIAGE_WORK_MANIFEST_CONTENT_VIEW = "normalized-observation-payload-v1"
TRIAGE_WORK_MANIFEST_TOKEN_ESTIMATOR = "canonical-json-utf8-upper-bound-v1"
TRIAGE_WORK_MANIFEST_SERIALIZED_PROMPT_FORMAT = "triage-work-unit-content.v1"
MAX_TRIAGE_WORK_ATOMS = 128
MAX_TRIAGE_WORK_CANDIDATE_VERSIONS = 128
MAX_TRIAGE_DIGEST_TEXT_ITEMS = 8
MAX_TRIAGE_DIGEST_TEXT_CHARS = 600
MAX_TRIAGE_CLUSTER_SEED_ATOMS = MAX_TRIAGE_WORK_ATOMS
MAX_TRIAGE_CLUSTER_MERGE_EVIDENCE = 8
MAX_TRIAGE_CLUSTER_UNCERTAINTY_NOTES = 8
_RESERVED_TRIAGE_CONTROL_TOKENS = (
    "gold_label",
    "label_set_id",
    "checkpoint_eligibility",
    "expected_route",
    "recommended_route",
    "must_catch",
    "material_transmission_expected",
    "batch_gate_passed",
    "promotion_eligible",
    "eligible",
    "ineligible",
    "needs_review",
    "checkpoint_candidate",
    "event_assessment",
    "attention_watch",
    "signal_intent",
    "order_intent",
    "trading_mandate",
    "approval_decision",
    "historical_pit_claim",
    "judgment_model_calls_authorized",
    "execution_capability",
)


@dataclass(frozen=True, slots=True)
class TriageWorkManifestPolicy:
    """Frozen ceilings using a conservative one-UTF-8-byte-per-token upper bound.

    This is deliberately not a provider tokenizer count. It bounds the canonical serialized
    work-unit content before a later runtime adds its own provider-specific request surface.
    """

    max_atoms_per_work_unit: int
    max_candidate_versions_per_work_unit: int
    max_estimated_serialized_prompt_utf8_tokens: int
    token_estimator_id: str = TRIAGE_WORK_MANIFEST_TOKEN_ESTIMATOR
    serialized_prompt_format: str = TRIAGE_WORK_MANIFEST_SERIALIZED_PROMPT_FORMAT

    def __post_init__(self) -> None:
        if not 1 <= self.max_atoms_per_work_unit <= MAX_TRIAGE_WORK_ATOMS:
            raise ValueError("triage work max_atoms_per_work_unit must be within the atom cap")
        if self.max_candidate_versions_per_work_unit < 1:
            raise ValueError("triage work max_candidate_versions_per_work_unit must be positive")
        if self.max_candidate_versions_per_work_unit > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage work candidate-version ceiling exceeds the batch cap")
        if self.max_estimated_serialized_prompt_utf8_tokens < 1:
            raise ValueError(
                "triage work max_estimated_serialized_prompt_utf8_tokens must be positive"
            )
        if self.token_estimator_id != TRIAGE_WORK_MANIFEST_TOKEN_ESTIMATOR:
            raise ValueError("unsupported triage work token estimator")
        if self.serialized_prompt_format != TRIAGE_WORK_MANIFEST_SERIALIZED_PROMPT_FORMAT:
            raise ValueError("unsupported triage work serialized prompt format")

    def to_dict(self) -> dict[str, object]:
        return {
            "max_atoms_per_work_unit": self.max_atoms_per_work_unit,
            "max_candidate_versions_per_work_unit": self.max_candidate_versions_per_work_unit,
            "max_estimated_serialized_prompt_utf8_tokens": (
                self.max_estimated_serialized_prompt_utf8_tokens
            ),
            "token_estimator_id": self.token_estimator_id,
            "serialized_prompt_format": self.serialized_prompt_format,
        }


@dataclass(frozen=True, slots=True)
class TriageWorkAtom:
    atom_id: str
    normalized_payload_hash: str
    serialized_payload_bytes: int
    candidate_version_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _prefixed_hash(self.atom_id, "event-impact-triage-work-atom-", "triage work atom_id")
        _sha256(self.normalized_payload_hash, "triage work atom normalized_payload_hash")
        if self.serialized_payload_bytes < 1:
            raise ValueError("triage work atom serialized_payload_bytes must be positive")
        _version_ids(self.candidate_version_ids, "triage work atom candidate versions")
        if len(self.candidate_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage work atom exceeds the candidate-version cap")
        if self.atom_id != self.expected_atom_id:
            raise ValueError("triage work atom_id does not match content")

    @property
    def expected_atom_id(self) -> str:
        return f"event-impact-triage-work-atom-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "normalized_payload_hash": self.normalized_payload_hash,
            "serialized_payload_bytes": self.serialized_payload_bytes,
            "candidate_version_ids": list(self.candidate_version_ids),
        }

    def to_dict(self) -> dict[str, object]:
        return {"atom_id": self.atom_id, **self.core_dict()}


@dataclass(frozen=True, slots=True)
class TriageWorkUnit:
    work_unit_id: str
    ordinal: int
    atom_ids: tuple[str, ...]
    candidate_version_ids: tuple[str, ...]
    atom_count: int
    candidate_version_count: int
    estimated_serialized_prompt_utf8_tokens: int

    def __post_init__(self) -> None:
        _prefixed_hash(
            self.work_unit_id,
            "event-impact-triage-work-unit-",
            "triage work unit_id",
        )
        if self.ordinal < 1:
            raise ValueError("triage work unit ordinal must be positive")
        _prefixed_hashes(
            self.atom_ids,
            "event-impact-triage-work-atom-",
            "triage work unit atom IDs",
        )
        _version_ids(self.candidate_version_ids, "triage work unit candidate versions")
        if len(self.candidate_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage work unit exceeds the candidate-version cap")
        if self.atom_count != len(self.atom_ids):
            raise ValueError("triage work unit atom_count does not match atom IDs")
        if self.candidate_version_count != len(self.candidate_version_ids):
            raise ValueError("triage work unit candidate_version_count does not match versions")
        if self.estimated_serialized_prompt_utf8_tokens < 1:
            raise ValueError(
                "triage work unit estimated_serialized_prompt_utf8_tokens must be positive"
            )
        if self.work_unit_id != self.expected_work_unit_id:
            raise ValueError("triage work unit_id does not match content")

    @property
    def expected_work_unit_id(self) -> str:
        return f"event-impact-triage-work-unit-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "atom_ids": list(self.atom_ids),
            "candidate_version_ids": list(self.candidate_version_ids),
            "atom_count": self.atom_count,
            "candidate_version_count": self.candidate_version_count,
            "estimated_serialized_prompt_utf8_tokens": (
                self.estimated_serialized_prompt_utf8_tokens
            ),
        }

    def to_dict(self) -> dict[str, object]:
        return {"work_unit_id": self.work_unit_id, **self.core_dict()}


@dataclass(frozen=True, slots=True)
class EventImpactTriageWorkManifest:
    """Harness-owned, arm-neutral partitioning of one frozen Candidate Set."""

    manifest_id: str
    candidate_set_id: str
    candidate_set_hash: str
    candidate_content_view: str
    policy: TriageWorkManifestPolicy
    ordered_candidate_version_ids: tuple[str, ...]
    atoms: tuple[TriageWorkAtom, ...]
    work_units: tuple[TriageWorkUnit, ...]
    historical_pit_claim: bool = False
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = EVENT_IMPACT_TRIAGE_WORK_MANIFEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_IMPACT_TRIAGE_WORK_MANIFEST_SCHEMA:
            raise ValueError("unsupported Event Impact Triage Work Manifest schema")
        _prefixed_hash(
            self.manifest_id,
            "event-impact-triage-work-manifest-",
            "triage work manifest_id",
        )
        _prefixed_hash(
            self.candidate_set_id,
            "event-impact-triage-candidate-set-",
            "triage work Candidate Set",
        )
        _sha256(self.candidate_set_hash, "triage work Candidate Set hash")
        if self.candidate_content_view != TRIAGE_WORK_MANIFEST_CONTENT_VIEW:
            raise ValueError("unsupported triage work candidate content view")
        _version_ids(self.ordered_candidate_version_ids, "triage work ordered candidate versions")
        if len(self.ordered_candidate_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage work manifest exceeds the 128 candidate-version cap")
        if not self.atoms:
            raise ValueError("triage work manifest requires at least one atom")
        if len(self.atoms) > MAX_TRIAGE_WORK_ATOMS:
            raise ValueError("triage work manifest exceeds the 128 atom cap")
        if len({item.atom_id for item in self.atoms}) != len(self.atoms):
            raise ValueError("triage work manifest atom IDs must be unique")
        if len({item.normalized_payload_hash for item in self.atoms}) != len(self.atoms):
            raise ValueError("triage work manifest must collapse duplicate normalized payloads")
        self._validate_atom_coverage()
        self._validate_work_units()
        if (
            self.historical_pit_claim
            or self.judgment_model_calls_authorized
            or self.execution_capability
        ):
            raise ValueError(
                "triage work manifest cannot grant PIT, Judgment, or execution authority"
            )
        if self.manifest_id != self.expected_manifest_id:
            raise ValueError("triage work manifest_id does not match content")

    @property
    def expected_manifest_id(self) -> str:
        return f"event-impact-triage-work-manifest-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "candidate_set_hash": self.candidate_set_hash,
            "candidate_content_view": self.candidate_content_view,
            "policy": self.policy.to_dict(),
            "ordered_candidate_version_ids": list(self.ordered_candidate_version_ids),
            "atoms": [item.to_dict() for item in self.atoms],
            "work_units": [item.to_dict() for item in self.work_units],
            "historical_pit_claim": self.historical_pit_claim,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {"manifest_id": self.manifest_id, **self.core_dict()}

    def validate_against(self, candidate_set: EventImpactTriageCandidateSet) -> None:
        """Fail closed unless this arm-neutral manifest still binds the exact Candidate Set."""

        if (
            self.candidate_set_id != candidate_set.candidate_set_id
            or self.candidate_set_hash != canonical_hash(candidate_set.to_dict())
        ):
            raise ValueError("triage work manifest belongs to another Candidate Set")
        if self.ordered_candidate_version_ids != candidate_set.version_ids:
            raise ValueError("triage work manifest candidate order differs from the Candidate Set")
        atom_by_version = {
            version_id: atom for atom in self.atoms for version_id in atom.candidate_version_ids
        }
        for observation in candidate_set.observations:
            atom = atom_by_version.get(observation.version_id)
            if atom is None or atom.normalized_payload_hash != observation.normalized_payload_hash:
                raise ValueError("triage work manifest content differs from the Candidate Set")

    def _validate_atom_coverage(self) -> None:
        positions = {value: index for index, value in enumerate(self.ordered_candidate_version_ids)}
        covered = tuple(
            version_id for atom in self.atoms for version_id in atom.candidate_version_ids
        )
        if len(set(covered)) != len(covered) or set(covered) != set(positions):
            raise ValueError("triage work atoms must cover every candidate version exactly once")
        if tuple(
            min(positions[value] for value in atom.candidate_version_ids) for atom in self.atoms
        ) != tuple(
            sorted(
                min(positions[value] for value in atom.candidate_version_ids) for atom in self.atoms
            )
        ):
            raise ValueError("triage work atoms must use stable first-receipt order")
        for atom in self.atoms:
            if atom.candidate_version_ids != tuple(
                sorted(atom.candidate_version_ids, key=positions.__getitem__)
            ):
                raise ValueError("triage work atom versions must use Candidate Set receipt order")

    def _validate_work_units(self) -> None:
        if not self.work_units:
            raise ValueError("triage work manifest requires at least one work unit")
        if tuple(item.ordinal for item in self.work_units) != tuple(
            range(1, len(self.work_units) + 1)
        ):
            raise ValueError("triage work units must use consecutive receipt-order ordinals")
        if len({item.work_unit_id for item in self.work_units}) != len(self.work_units):
            raise ValueError("triage work manifest work unit IDs must be unique")
        atom_by_id = {item.atom_id: item for item in self.atoms}
        ordered_atom_ids = tuple(item.atom_id for item in self.atoms)
        covered_atom_ids = tuple(atom_id for unit in self.work_units for atom_id in unit.atom_ids)
        if covered_atom_ids != ordered_atom_ids:
            raise ValueError("triage work units must partition atoms in receipt order")
        positions = {value: index for index, value in enumerate(self.ordered_candidate_version_ids)}
        for unit in self.work_units:
            if unit.atom_count > self.policy.max_atoms_per_work_unit:
                raise ValueError("triage work unit exceeds the frozen atom ceiling")
            if unit.candidate_version_count > self.policy.max_candidate_versions_per_work_unit:
                raise ValueError("triage work unit exceeds the frozen candidate-version ceiling")
            if (
                unit.estimated_serialized_prompt_utf8_tokens
                > self.policy.max_estimated_serialized_prompt_utf8_tokens
            ):
                raise ValueError(
                    "triage work unit exceeds the frozen serialized-prompt UTF-8 upper bound"
                )
            unit_versions = {
                version_id
                for atom_id in unit.atom_ids
                for version_id in atom_by_id[atom_id].candidate_version_ids
            }
            expected_versions = tuple(
                value for value in self.ordered_candidate_version_ids if value in unit_versions
            )
            if unit.candidate_version_ids != expected_versions:
                raise ValueError("triage work unit candidate coverage is not canonical")
            if unit.candidate_version_ids != tuple(
                sorted(unit.candidate_version_ids, key=positions.__getitem__)
            ):
                raise ValueError("triage work unit versions must use Candidate Set receipt order")


@dataclass(frozen=True, slots=True)
class _ResolvedAtom:
    atom: TriageWorkAtom
    normalized_payload: dict[str, object]


def build_event_impact_triage_work_manifest(
    *,
    candidate_set: EventImpactTriageCandidateSet,
    contents: tuple[TriageCandidateContent, ...],
    policy: TriageWorkManifestPolicy,
) -> EventImpactTriageWorkManifest:
    """Build an arm-neutral, greedy work partition without retaining source payloads."""

    _validate_resolved_contents(candidate_set, contents)
    resolved_atoms = _collapse_atoms(candidate_set, contents)
    if len(resolved_atoms) > MAX_TRIAGE_WORK_ATOMS:
        raise ValueError("triage work manifest exceeds the 128 atom cap before partitioning")
    _validate_singletons(resolved_atoms, policy)
    units = _partition_work_units(
        resolved_atoms=resolved_atoms,
        ordered_candidate_version_ids=candidate_set.version_ids,
        policy=policy,
    )
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_WORK_MANIFEST_SCHEMA,
        "candidate_set_id": candidate_set.candidate_set_id,
        "candidate_set_hash": canonical_hash(candidate_set.to_dict()),
        "candidate_content_view": TRIAGE_WORK_MANIFEST_CONTENT_VIEW,
        "policy": policy.to_dict(),
        "ordered_candidate_version_ids": list(candidate_set.version_ids),
        "atoms": [item.atom.to_dict() for item in resolved_atoms],
        "work_units": [item.to_dict() for item in units],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    manifest = EventImpactTriageWorkManifest(
        manifest_id=f"event-impact-triage-work-manifest-{canonical_hash(core)}",
        candidate_set_id=candidate_set.candidate_set_id,
        candidate_set_hash=canonical_hash(candidate_set.to_dict()),
        candidate_content_view=TRIAGE_WORK_MANIFEST_CONTENT_VIEW,
        policy=policy,
        ordered_candidate_version_ids=candidate_set.version_ids,
        atoms=tuple(item.atom for item in resolved_atoms),
        work_units=units,
    )
    manifest.validate_against(candidate_set)
    return manifest


def event_impact_triage_work_manifest_from_dict(value: object) -> EventImpactTriageWorkManifest:
    payload = _object(value, "Event Impact Triage Work Manifest")
    _exact_keys(
        payload,
        {
            "schema_version",
            "manifest_id",
            "candidate_set_id",
            "candidate_set_hash",
            "candidate_content_view",
            "policy",
            "ordered_candidate_version_ids",
            "atoms",
            "work_units",
            "historical_pit_claim",
            "judgment_model_calls_authorized",
            "execution_capability",
        },
        "Event Impact Triage Work Manifest",
    )
    policy = _policy_from_dict(payload.get("policy"))
    atoms = tuple(
        _atom_from_dict(item) for item in _array(payload.get("atoms"), "triage work atoms")
    )
    work_units = tuple(
        _work_unit_from_dict(item)
        for item in _array(payload.get("work_units"), "triage work units")
    )
    result = EventImpactTriageWorkManifest(
        manifest_id=_string(payload, "manifest_id"),
        candidate_set_id=_string(payload, "candidate_set_id"),
        candidate_set_hash=_string(payload, "candidate_set_hash"),
        candidate_content_view=_string(payload, "candidate_content_view"),
        policy=policy,
        ordered_candidate_version_ids=_version_id_tuple(
            payload.get("ordered_candidate_version_ids"),
            "triage work ordered candidate versions",
        ),
        atoms=atoms,
        work_units=work_units,
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        judgment_model_calls_authorized=_boolean(payload, "judgment_model_calls_authorized"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("Event Impact Triage Work Manifest is not canonical")
    return result


def _validate_resolved_contents(
    candidate_set: EventImpactTriageCandidateSet,
    contents: tuple[TriageCandidateContent, ...],
) -> None:
    if tuple(item.version_id for item in contents) != candidate_set.version_ids:
        raise ValueError("triage work content must preserve exact Candidate Set receipt order")
    for observation, content in zip(candidate_set.observations, contents, strict=True):
        if content.payload_hash != observation.normalized_payload_hash:
            raise ValueError("triage work content differs from the frozen Candidate Set")


def _collapse_atoms(
    candidate_set: EventImpactTriageCandidateSet,
    contents: tuple[TriageCandidateContent, ...],
) -> tuple[_ResolvedAtom, ...]:
    grouped: dict[bytes, tuple[dict[str, object], list[str]]] = {}
    order: list[bytes] = []
    for observation, content in zip(candidate_set.observations, contents, strict=True):
        serialized_payload = canonical_json_bytes(content.normalized_payload)
        existing = grouped.get(serialized_payload)
        if existing is None:
            grouped[serialized_payload] = (content.normalized_payload, [observation.version_id])
            order.append(serialized_payload)
        else:
            existing[1].append(observation.version_id)
    resolved: list[_ResolvedAtom] = []
    for serialized_payload in order:
        payload, version_ids = grouped[serialized_payload]
        atom_core = {
            "normalized_payload_hash": canonical_hash(payload),
            "serialized_payload_bytes": len(serialized_payload),
            "candidate_version_ids": version_ids,
        }
        atom = TriageWorkAtom(
            atom_id=f"event-impact-triage-work-atom-{canonical_hash(atom_core)}",
            normalized_payload_hash=canonical_hash(payload),
            serialized_payload_bytes=len(serialized_payload),
            candidate_version_ids=tuple(version_ids),
        )
        resolved.append(_ResolvedAtom(atom=atom, normalized_payload=payload))
    return tuple(resolved)


def _validate_singletons(
    resolved_atoms: tuple[_ResolvedAtom, ...], policy: TriageWorkManifestPolicy
) -> None:
    for resolved in resolved_atoms:
        atom = resolved.atom
        if len(atom.candidate_version_ids) > policy.max_candidate_versions_per_work_unit:
            raise ValueError("triage work atom exceeds the frozen candidate-version ceiling")
        estimated = _estimate_serialized_prompt_utf8_tokens((resolved,))
        if estimated > policy.max_estimated_serialized_prompt_utf8_tokens:
            raise ValueError(
                "triage work singleton exceeds the frozen serialized-prompt UTF-8 upper bound"
            )


def _partition_work_units(
    *,
    resolved_atoms: tuple[_ResolvedAtom, ...],
    ordered_candidate_version_ids: tuple[str, ...],
    policy: TriageWorkManifestPolicy,
) -> tuple[TriageWorkUnit, ...]:
    partitions: list[tuple[_ResolvedAtom, ...]] = []
    current: list[_ResolvedAtom] = []
    for resolved in resolved_atoms:
        proposed = tuple((*current, resolved))
        candidate_count = sum(len(item.atom.candidate_version_ids) for item in proposed)
        if current and (
            len(proposed) > policy.max_atoms_per_work_unit
            or candidate_count > policy.max_candidate_versions_per_work_unit
            or (
                _estimate_serialized_prompt_utf8_tokens(proposed)
                > policy.max_estimated_serialized_prompt_utf8_tokens
            )
        ):
            partitions.append(tuple(current))
            current = [resolved]
        else:
            current = list(proposed)
    if current:
        partitions.append(tuple(current))
    units: list[TriageWorkUnit] = []
    for ordinal, partition in enumerate(partitions, start=1):
        atom_ids = tuple(item.atom.atom_id for item in partition)
        covered_versions = {
            version_id for item in partition for version_id in item.atom.candidate_version_ids
        }
        candidate_version_ids = tuple(
            value for value in ordered_candidate_version_ids if value in covered_versions
        )
        estimated_serialized_prompt_utf8_tokens = _estimate_serialized_prompt_utf8_tokens(partition)
        core = {
            "ordinal": ordinal,
            "atom_ids": list(atom_ids),
            "candidate_version_ids": list(candidate_version_ids),
            "atom_count": len(atom_ids),
            "candidate_version_count": len(candidate_version_ids),
            "estimated_serialized_prompt_utf8_tokens": (estimated_serialized_prompt_utf8_tokens),
        }
        units.append(
            TriageWorkUnit(
                work_unit_id=f"event-impact-triage-work-unit-{canonical_hash(core)}",
                ordinal=ordinal,
                atom_ids=atom_ids,
                candidate_version_ids=candidate_version_ids,
                atom_count=len(atom_ids),
                candidate_version_count=len(candidate_version_ids),
                estimated_serialized_prompt_utf8_tokens=(estimated_serialized_prompt_utf8_tokens),
            )
        )
    return tuple(units)


def _estimate_serialized_prompt_utf8_tokens(resolved_atoms: tuple[_ResolvedAtom, ...]) -> int:
    """Return a conservative 1 UTF-8 byte/token bound, never a provider tokenizer count."""
    prompt_content = {
        "candidate_content_view": TRIAGE_WORK_MANIFEST_CONTENT_VIEW,
        "serialized_prompt_format": TRIAGE_WORK_MANIFEST_SERIALIZED_PROMPT_FORMAT,
        "atoms": [
            {
                "atom_id": item.atom.atom_id,
                "candidate_version_ids": list(item.atom.candidate_version_ids),
                "normalized_payload": item.normalized_payload,
            }
            for item in resolved_atoms
        ],
    }
    return max(1, len(canonical_json_bytes(prompt_content)))


def _policy_from_dict(value: object) -> TriageWorkManifestPolicy:
    payload = _object(value, "triage work policy")
    _exact_keys(
        payload,
        {
            "max_atoms_per_work_unit",
            "max_candidate_versions_per_work_unit",
            "max_estimated_serialized_prompt_utf8_tokens",
            "token_estimator_id",
            "serialized_prompt_format",
        },
        "triage work policy",
    )
    return TriageWorkManifestPolicy(
        max_atoms_per_work_unit=_positive_int(payload, "max_atoms_per_work_unit"),
        max_candidate_versions_per_work_unit=_positive_int(
            payload, "max_candidate_versions_per_work_unit"
        ),
        max_estimated_serialized_prompt_utf8_tokens=_positive_int(
            payload, "max_estimated_serialized_prompt_utf8_tokens"
        ),
        token_estimator_id=_string(payload, "token_estimator_id"),
        serialized_prompt_format=_string(payload, "serialized_prompt_format"),
    )


def _atom_from_dict(value: object) -> TriageWorkAtom:
    payload = _object(value, "triage work atom")
    _exact_keys(
        payload,
        {
            "atom_id",
            "normalized_payload_hash",
            "serialized_payload_bytes",
            "candidate_version_ids",
        },
        "triage work atom",
    )
    return TriageWorkAtom(
        atom_id=_string(payload, "atom_id"),
        normalized_payload_hash=_string(payload, "normalized_payload_hash"),
        serialized_payload_bytes=_positive_int(payload, "serialized_payload_bytes"),
        candidate_version_ids=_version_id_tuple(
            payload.get("candidate_version_ids"), "triage work atom candidate versions"
        ),
    )


def _work_unit_from_dict(value: object) -> TriageWorkUnit:
    payload = _object(value, "triage work unit")
    _exact_keys(
        payload,
        {
            "work_unit_id",
            "ordinal",
            "atom_ids",
            "candidate_version_ids",
            "atom_count",
            "candidate_version_count",
            "estimated_serialized_prompt_utf8_tokens",
        },
        "triage work unit",
    )
    return TriageWorkUnit(
        work_unit_id=_string(payload, "work_unit_id"),
        ordinal=_positive_int(payload, "ordinal"),
        atom_ids=_prefixed_hash_tuple(
            payload.get("atom_ids"),
            "event-impact-triage-work-atom-",
            "triage work unit atom IDs",
        ),
        candidate_version_ids=_version_id_tuple(
            payload.get("candidate_version_ids"), "triage work unit candidate versions"
        ),
        atom_count=_positive_int(payload, "atom_count"),
        candidate_version_count=_positive_int(payload, "candidate_version_count"),
        estimated_serialized_prompt_utf8_tokens=_positive_int(
            payload, "estimated_serialized_prompt_utf8_tokens"
        ),
    )


class TriageClusterMergeState(StrEnum):
    MERGED = "merged"
    NEEDS_REVIEW = "needs_review"


@dataclass(frozen=True, slots=True)
class TriageCandidateDigest:
    """One bounded, no-authority summary of one Manifest Atom."""

    digest_id: str
    manifest_id: str
    manifest_hash: str
    work_unit_id: str
    atom_id: str
    candidate_version_ids: tuple[str, ...]
    changed_facts: tuple[str, ...]
    source_conflicts: tuple[str, ...]
    transmission_paths: tuple[str, ...]
    countercases: tuple[str, ...]
    uncertainty_notes: tuple[str, ...]
    checkpoint_rule_evidence: tuple[str, ...]
    historical_pit_claim: bool = False
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = EVENT_IMPACT_TRIAGE_CANDIDATE_DIGEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_IMPACT_TRIAGE_CANDIDATE_DIGEST_SCHEMA:
            raise ValueError("unsupported Triage Candidate Digest schema")
        _prefixed_hash(
            self.digest_id,
            "event-impact-triage-candidate-digest-",
            "triage candidate digest_id",
        )
        _prefixed_hash(
            self.manifest_id,
            "event-impact-triage-work-manifest-",
            "triage digest manifest_id",
        )
        _sha256(self.manifest_hash, "triage digest manifest_hash")
        _prefixed_hash(
            self.work_unit_id,
            "event-impact-triage-work-unit-",
            "triage digest work_unit_id",
        )
        _prefixed_hash(
            self.atom_id,
            "event-impact-triage-work-atom-",
            "triage digest atom_id",
        )
        _version_ids(self.candidate_version_ids, "triage digest candidate versions")
        if len(self.candidate_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage candidate digest exceeds the candidate-version cap")
        _canonical_digest_texts(self.changed_facts, "changed_facts")
        _canonical_digest_texts(self.source_conflicts, "source_conflicts")
        _canonical_digest_texts(self.transmission_paths, "transmission_paths")
        _canonical_digest_texts(self.countercases, "countercases")
        _canonical_digest_texts(self.uncertainty_notes, "uncertainty_notes")
        _canonical_digest_texts(
            self.checkpoint_rule_evidence,
            "checkpoint_rule_evidence",
        )
        if (
            self.historical_pit_claim
            or self.judgment_model_calls_authorized
            or self.execution_capability
        ):
            raise ValueError(
                "triage candidate digest cannot grant PIT, Judgment, or execution authority"
            )
        if self.digest_id != self.expected_digest_id:
            raise ValueError("triage candidate digest_id does not match content")

    @property
    def expected_digest_id(self) -> str:
        return f"event-impact-triage-candidate-digest-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "manifest_hash": self.manifest_hash,
            "work_unit_id": self.work_unit_id,
            "atom_id": self.atom_id,
            "candidate_version_ids": list(self.candidate_version_ids),
            "changed_facts": list(self.changed_facts),
            "source_conflicts": list(self.source_conflicts),
            "transmission_paths": list(self.transmission_paths),
            "countercases": list(self.countercases),
            "uncertainty_notes": list(self.uncertainty_notes),
            "checkpoint_rule_evidence": list(self.checkpoint_rule_evidence),
            "historical_pit_claim": self.historical_pit_claim,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {"digest_id": self.digest_id, **self.core_dict()}

    def validate_against(self, manifest: EventImpactTriageWorkManifest) -> None:
        if self.manifest_id != manifest.manifest_id or self.manifest_hash != canonical_hash(
            manifest.to_dict()
        ):
            raise ValueError("triage candidate digest belongs to another manifest")
        atom = next((item for item in manifest.atoms if item.atom_id == self.atom_id), None)
        if atom is None:
            raise ValueError("triage candidate digest references an unknown manifest atom")
        unit = next((item for item in manifest.work_units if self.atom_id in item.atom_ids), None)
        if unit is None:
            raise ValueError("triage candidate digest atom is outside every manifest work unit")
        if self.work_unit_id != unit.work_unit_id:
            raise ValueError("triage candidate digest work unit differs from the manifest")
        if self.candidate_version_ids != atom.candidate_version_ids:
            raise ValueError("triage candidate digest versions differ from the manifest atom")

    @classmethod
    def build(
        cls,
        *,
        manifest: EventImpactTriageWorkManifest,
        atom_id: str,
        changed_facts: tuple[str, ...],
        uncertainty_notes: tuple[str, ...],
        checkpoint_rule_evidence: tuple[str, ...],
        source_conflicts: tuple[str, ...] = (),
        transmission_paths: tuple[str, ...] = (),
        countercases: tuple[str, ...] = (),
    ) -> TriageCandidateDigest:
        atom = next((item for item in manifest.atoms if item.atom_id == atom_id), None)
        if atom is None:
            raise ValueError("triage candidate digest references an unknown manifest atom")
        unit = next((item for item in manifest.work_units if atom_id in item.atom_ids), None)
        if unit is None:
            raise ValueError("triage candidate digest atom is outside every manifest work unit")
        canonical_changed_facts = _build_digest_texts(changed_facts, "changed_facts")
        canonical_source_conflicts = _build_digest_texts(source_conflicts, "source_conflicts")
        canonical_transmission_paths = _build_digest_texts(
            transmission_paths,
            "transmission_paths",
        )
        canonical_countercases = _build_digest_texts(countercases, "countercases")
        canonical_uncertainty_notes = _build_digest_texts(
            uncertainty_notes,
            "uncertainty_notes",
        )
        canonical_checkpoint_rule_evidence = _build_digest_texts(
            checkpoint_rule_evidence,
            "checkpoint_rule_evidence",
        )
        core = {
            "schema_version": EVENT_IMPACT_TRIAGE_CANDIDATE_DIGEST_SCHEMA,
            "manifest_id": manifest.manifest_id,
            "manifest_hash": canonical_hash(manifest.to_dict()),
            "work_unit_id": unit.work_unit_id,
            "atom_id": atom.atom_id,
            "candidate_version_ids": list(atom.candidate_version_ids),
            "changed_facts": list(canonical_changed_facts),
            "source_conflicts": list(canonical_source_conflicts),
            "transmission_paths": list(canonical_transmission_paths),
            "countercases": list(canonical_countercases),
            "uncertainty_notes": list(canonical_uncertainty_notes),
            "checkpoint_rule_evidence": list(canonical_checkpoint_rule_evidence),
            "historical_pit_claim": False,
            "judgment_model_calls_authorized": False,
            "execution_capability": False,
        }
        result = cls(
            digest_id=f"event-impact-triage-candidate-digest-{canonical_hash(core)}",
            manifest_id=manifest.manifest_id,
            manifest_hash=canonical_hash(manifest.to_dict()),
            work_unit_id=unit.work_unit_id,
            atom_id=atom.atom_id,
            candidate_version_ids=atom.candidate_version_ids,
            changed_facts=canonical_changed_facts,
            source_conflicts=canonical_source_conflicts,
            transmission_paths=canonical_transmission_paths,
            countercases=canonical_countercases,
            uncertainty_notes=canonical_uncertainty_notes,
            checkpoint_rule_evidence=canonical_checkpoint_rule_evidence,
        )
        result.validate_against(manifest)
        return result


@dataclass(frozen=True, slots=True)
class TriageClusterSeed:
    """A bounded, no-decision grouping of one or more Atom Digests."""

    cluster_seed_id: str
    digest_ids: tuple[str, ...]
    atom_ids: tuple[str, ...]
    candidate_version_ids: tuple[str, ...]
    merge_state: TriageClusterMergeState
    merge_evidence: tuple[str, ...]
    uncertainty_notes: tuple[str, ...]

    def __post_init__(self) -> None:
        _prefixed_hash(
            self.cluster_seed_id,
            "event-impact-triage-cluster-seed-",
            "triage cluster seed_id",
        )
        _prefixed_hashes(
            self.digest_ids,
            "event-impact-triage-candidate-digest-",
            "triage cluster digest IDs",
        )
        _prefixed_hashes(
            self.atom_ids,
            "event-impact-triage-work-atom-",
            "triage cluster atom IDs",
        )
        if len(self.digest_ids) > MAX_TRIAGE_CLUSTER_SEED_ATOMS:
            raise ValueError("triage cluster seed exceeds the digest ceiling")
        if len(self.atom_ids) > MAX_TRIAGE_CLUSTER_SEED_ATOMS:
            raise ValueError("triage cluster seed exceeds the atom ceiling")
        if len(self.digest_ids) != len(self.atom_ids):
            raise ValueError("triage cluster seed requires one digest per atom")
        _version_ids(self.candidate_version_ids, "triage cluster candidate versions")
        if len(self.candidate_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage cluster seed exceeds the candidate-version cap")
        _canonical_texts(
            self.merge_evidence,
            "triage cluster merge_evidence",
            maximum=MAX_TRIAGE_CLUSTER_MERGE_EVIDENCE,
            minimum=(
                1
                if len(self.atom_ids) > 1 and self.merge_state is TriageClusterMergeState.MERGED
                else 0
            ),
        )
        _canonical_texts(
            self.uncertainty_notes,
            "triage cluster uncertainty_notes",
            maximum=MAX_TRIAGE_CLUSTER_UNCERTAINTY_NOTES,
            minimum=1 if self.merge_state is TriageClusterMergeState.NEEDS_REVIEW else 0,
        )
        if self.cluster_seed_id != self.expected_cluster_seed_id:
            raise ValueError("triage cluster seed_id does not match content")

    @property
    def expected_cluster_seed_id(self) -> str:
        return f"event-impact-triage-cluster-seed-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "digest_ids": list(self.digest_ids),
            "atom_ids": list(self.atom_ids),
            "candidate_version_ids": list(self.candidate_version_ids),
            "merge_state": self.merge_state.value,
            "merge_evidence": list(self.merge_evidence),
            "uncertainty_notes": list(self.uncertainty_notes),
        }

    def to_dict(self) -> dict[str, object]:
        return {"cluster_seed_id": self.cluster_seed_id, **self.core_dict()}

    @classmethod
    def build(
        cls,
        *,
        manifest: EventImpactTriageWorkManifest,
        digests: tuple[TriageCandidateDigest, ...],
        atom_ids: tuple[str, ...],
        merge_state: TriageClusterMergeState,
        merge_evidence: tuple[str, ...],
        uncertainty_notes: tuple[str, ...] = (),
    ) -> TriageClusterSeed:
        atom_by_id = {item.atom_id: item for item in manifest.atoms}
        _prefixed_hashes(atom_ids, "event-impact-triage-work-atom-", "triage cluster atom IDs")
        digest_by_atom: dict[str, TriageCandidateDigest] = {}
        for digest in digests:
            digest.validate_against(manifest)
            if digest.atom_id in digest_by_atom:
                raise ValueError("triage cluster digest bindings must be unique")
            digest_by_atom[digest.atom_id] = digest
        unknown = set(atom_ids) - set(atom_by_id)
        if unknown:
            raise ValueError("triage cluster seed references an unknown manifest atom")
        if len(atom_ids) > MAX_TRIAGE_CLUSTER_SEED_ATOMS:
            raise ValueError("triage cluster seed exceeds the atom ceiling")
        positions = {item.atom_id: index for index, item in enumerate(manifest.atoms)}
        canonical_atom_ids = tuple(sorted(atom_ids, key=positions.__getitem__))
        if atom_ids != canonical_atom_ids:
            raise ValueError("triage cluster atoms must use manifest receipt order")
        if set(digest_by_atom) != set(atom_ids):
            raise ValueError("triage cluster atom IDs do not match digest bindings")
        ordered_digests = tuple(digest_by_atom[atom_id] for atom_id in atom_ids)
        candidate_versions = {
            version_id
            for atom_id in atom_ids
            for version_id in atom_by_id[atom_id].candidate_version_ids
        }
        canonical_versions = tuple(
            value for value in manifest.ordered_candidate_version_ids if value in candidate_versions
        )
        canonical_merge_evidence = _build_texts(
            merge_evidence,
            "triage cluster merge_evidence",
            maximum=MAX_TRIAGE_CLUSTER_MERGE_EVIDENCE,
            minimum=(
                1 if len(atom_ids) > 1 and merge_state is TriageClusterMergeState.MERGED else 0
            ),
        )
        canonical_uncertainty_notes = _build_texts(
            uncertainty_notes,
            "triage cluster uncertainty_notes",
            maximum=MAX_TRIAGE_CLUSTER_UNCERTAINTY_NOTES,
            minimum=1 if merge_state is TriageClusterMergeState.NEEDS_REVIEW else 0,
        )
        core = {
            "digest_ids": [item.digest_id for item in ordered_digests],
            "atom_ids": list(atom_ids),
            "candidate_version_ids": list(canonical_versions),
            "merge_state": merge_state.value,
            "merge_evidence": list(canonical_merge_evidence),
            "uncertainty_notes": list(canonical_uncertainty_notes),
        }
        return cls(
            cluster_seed_id=f"event-impact-triage-cluster-seed-{canonical_hash(core)}",
            digest_ids=tuple(item.digest_id for item in ordered_digests),
            atom_ids=atom_ids,
            candidate_version_ids=canonical_versions,
            merge_state=merge_state,
            merge_evidence=canonical_merge_evidence,
            uncertainty_notes=canonical_uncertainty_notes,
        )


@dataclass(frozen=True, slots=True)
class TriageClusterPartition:
    """Arm-neutral, exhaustive clustering of every Digest from one Work Manifest."""

    partition_id: str
    manifest_id: str
    manifest_hash: str
    ordered_digest_ids: tuple[str, ...]
    ordered_candidate_version_ids: tuple[str, ...]
    clusters: tuple[TriageClusterSeed, ...]
    historical_pit_claim: bool = False
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = EVENT_IMPACT_TRIAGE_CLUSTER_PARTITION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_IMPACT_TRIAGE_CLUSTER_PARTITION_SCHEMA:
            raise ValueError("unsupported Triage Cluster Partition schema")
        _prefixed_hash(
            self.partition_id,
            "event-impact-triage-cluster-partition-",
            "triage cluster partition_id",
        )
        _prefixed_hash(
            self.manifest_id,
            "event-impact-triage-work-manifest-",
            "triage partition manifest_id",
        )
        _sha256(self.manifest_hash, "triage partition manifest_hash")
        _prefixed_hashes(
            self.ordered_digest_ids,
            "event-impact-triage-candidate-digest-",
            "triage partition ordered digest IDs",
        )
        if len(self.ordered_digest_ids) > MAX_TRIAGE_WORK_ATOMS:
            raise ValueError("triage cluster partition exceeds the digest ceiling")
        _version_ids(
            self.ordered_candidate_version_ids,
            "triage cluster partition ordered candidate versions",
        )
        if len(self.ordered_candidate_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage cluster partition exceeds the candidate-version cap")
        if not self.clusters:
            raise ValueError("triage cluster partition requires at least one cluster seed")
        if len(self.clusters) > MAX_TRIAGE_WORK_ATOMS:
            raise ValueError("triage cluster partition exceeds the cluster seed ceiling")
        if len({item.cluster_seed_id for item in self.clusters}) != len(self.clusters):
            raise ValueError("triage cluster partition seed IDs must be unique")
        if tuple(item.cluster_seed_id for item in self.clusters) != tuple(
            sorted(item.cluster_seed_id for item in self.clusters)
        ):
            raise ValueError("triage cluster partition seeds must use canonical ID order")
        cluster_digest_ids = tuple(
            digest_id for cluster in self.clusters for digest_id in cluster.digest_ids
        )
        if len(set(cluster_digest_ids)) != len(cluster_digest_ids) or set(
            cluster_digest_ids
        ) != set(self.ordered_digest_ids):
            raise ValueError("triage cluster partition must consume every digest exactly once")
        cluster_atom_ids = tuple(
            atom_id for cluster in self.clusters for atom_id in cluster.atom_ids
        )
        if len(cluster_atom_ids) != len(self.ordered_digest_ids) or len(
            set(cluster_atom_ids)
        ) != len(cluster_atom_ids):
            raise ValueError("triage cluster partition must consume every atom exactly once")
        cluster_version_ids = tuple(
            version_id for cluster in self.clusters for version_id in cluster.candidate_version_ids
        )
        if len(cluster_version_ids) > MAX_TRIAGE_WORK_CANDIDATE_VERSIONS:
            raise ValueError("triage cluster partition exceeds the candidate-version cap")
        if len(set(cluster_version_ids)) != len(cluster_version_ids) or set(
            cluster_version_ids
        ) != set(self.ordered_candidate_version_ids):
            raise ValueError(
                "triage cluster partition must consume every candidate version exactly once"
            )
        if (
            self.historical_pit_claim
            or self.judgment_model_calls_authorized
            or self.execution_capability
        ):
            raise ValueError(
                "triage cluster partition cannot grant PIT, Judgment, or execution authority"
            )
        if self.partition_id != self.expected_partition_id:
            raise ValueError("triage cluster partition_id does not match content")

    @property
    def expected_partition_id(self) -> str:
        return f"event-impact-triage-cluster-partition-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "manifest_hash": self.manifest_hash,
            "ordered_digest_ids": list(self.ordered_digest_ids),
            "ordered_candidate_version_ids": list(self.ordered_candidate_version_ids),
            "clusters": [item.to_dict() for item in self.clusters],
            "historical_pit_claim": self.historical_pit_claim,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {"partition_id": self.partition_id, **self.core_dict()}

    def validate_against(
        self,
        manifest: EventImpactTriageWorkManifest,
        digests: tuple[TriageCandidateDigest, ...],
    ) -> None:
        if self.manifest_id != manifest.manifest_id or self.manifest_hash != canonical_hash(
            manifest.to_dict()
        ):
            raise ValueError("triage cluster partition belongs to another manifest")
        if len(digests) != len(manifest.atoms):
            raise ValueError("triage cluster partition requires one digest for every Work Atom")
        digest_by_id: dict[str, TriageCandidateDigest] = {}
        digest_by_atom: dict[str, TriageCandidateDigest] = {}
        for digest in digests:
            digest.validate_against(manifest)
            if digest.digest_id in digest_by_id or digest.atom_id in digest_by_atom:
                raise ValueError("triage cluster partition digest bindings must be unique")
            digest_by_id[digest.digest_id] = digest
            digest_by_atom[digest.atom_id] = digest
        expected_atom_ids = tuple(item.atom_id for item in manifest.atoms)
        if set(digest_by_atom) != set(expected_atom_ids):
            raise ValueError("triage cluster partition digests do not cover every Work Atom")
        expected_digest_ids = tuple(
            digest_by_atom[atom_id].digest_id for atom_id in expected_atom_ids
        )
        if self.ordered_digest_ids != expected_digest_ids:
            raise ValueError("triage cluster partition digest order differs from the manifest")
        if self.ordered_candidate_version_ids != manifest.ordered_candidate_version_ids:
            raise ValueError("triage cluster partition candidate order differs from the manifest")
        cluster_digest_ids = tuple(
            digest_id for cluster in self.clusters for digest_id in cluster.digest_ids
        )
        cluster_atom_ids = tuple(
            atom_id for cluster in self.clusters for atom_id in cluster.atom_ids
        )
        if len(set(cluster_digest_ids)) != len(cluster_digest_ids) or set(
            cluster_digest_ids
        ) != set(expected_digest_ids):
            raise ValueError("triage cluster partition must consume every digest exactly once")
        if len(set(cluster_atom_ids)) != len(cluster_atom_ids) or set(cluster_atom_ids) != set(
            expected_atom_ids
        ):
            raise ValueError("triage cluster partition must consume every atom exactly once")
        atom_by_id = {item.atom_id: item for item in manifest.atoms}
        positions = {item.atom_id: index for index, item in enumerate(manifest.atoms)}
        for cluster in self.clusters:
            if cluster.atom_ids != tuple(sorted(cluster.atom_ids, key=positions.__getitem__)):
                raise ValueError("triage cluster atoms must use manifest receipt order")
            if (
                tuple(digest_by_id[digest_id].atom_id for digest_id in cluster.digest_ids)
                != cluster.atom_ids
            ):
                raise ValueError("triage cluster digest-to-atom bindings are invalid")
            expected_versions = {
                version_id
                for atom_id in cluster.atom_ids
                for version_id in atom_by_id[atom_id].candidate_version_ids
            }
            canonical_versions = tuple(
                value
                for value in manifest.ordered_candidate_version_ids
                if value in expected_versions
            )
            if cluster.candidate_version_ids != canonical_versions:
                raise ValueError("triage cluster candidate coverage is not canonical")

    @classmethod
    def build(
        cls,
        *,
        manifest: EventImpactTriageWorkManifest,
        digests: tuple[TriageCandidateDigest, ...],
        clusters: tuple[TriageClusterSeed, ...],
    ) -> TriageClusterPartition:
        if len({item.digest_id for item in digests}) != len(digests) or len(
            {item.atom_id for item in digests}
        ) != len(digests):
            raise ValueError("triage cluster partition digest bindings must be unique")
        if len(digests) != len(manifest.atoms):
            raise ValueError("triage cluster partition requires one digest for every Work Atom")
        digest_by_atom: dict[str, TriageCandidateDigest] = {}
        for digest in digests:
            digest.validate_against(manifest)
            if digest.atom_id in digest_by_atom:
                raise ValueError("triage cluster partition digest bindings must be unique")
            digest_by_atom[digest.atom_id] = digest
        expected_atom_ids = tuple(item.atom_id for item in manifest.atoms)
        if set(digest_by_atom) != set(expected_atom_ids):
            raise ValueError("triage cluster partition digests do not cover every Work Atom")
        ordered_digest_ids = tuple(
            digest_by_atom[atom_id].digest_id for atom_id in expected_atom_ids
        )
        ordered_clusters = tuple(sorted(clusters, key=lambda item: item.cluster_seed_id))
        core = {
            "schema_version": EVENT_IMPACT_TRIAGE_CLUSTER_PARTITION_SCHEMA,
            "manifest_id": manifest.manifest_id,
            "manifest_hash": canonical_hash(manifest.to_dict()),
            "ordered_digest_ids": list(ordered_digest_ids),
            "ordered_candidate_version_ids": list(manifest.ordered_candidate_version_ids),
            "clusters": [item.to_dict() for item in ordered_clusters],
            "historical_pit_claim": False,
            "judgment_model_calls_authorized": False,
            "execution_capability": False,
        }
        result = cls(
            partition_id=f"event-impact-triage-cluster-partition-{canonical_hash(core)}",
            manifest_id=manifest.manifest_id,
            manifest_hash=canonical_hash(manifest.to_dict()),
            ordered_digest_ids=ordered_digest_ids,
            ordered_candidate_version_ids=manifest.ordered_candidate_version_ids,
            clusters=ordered_clusters,
        )
        result.validate_against(manifest, digests)
        return result


def triage_candidate_digest_from_dict(value: object) -> TriageCandidateDigest:
    payload = _object(value, "Triage Candidate Digest")
    _exact_keys(
        payload,
        {
            "schema_version",
            "digest_id",
            "manifest_id",
            "manifest_hash",
            "work_unit_id",
            "atom_id",
            "candidate_version_ids",
            "changed_facts",
            "source_conflicts",
            "transmission_paths",
            "countercases",
            "uncertainty_notes",
            "checkpoint_rule_evidence",
            "historical_pit_claim",
            "judgment_model_calls_authorized",
            "execution_capability",
        },
        "Triage Candidate Digest",
    )
    result = TriageCandidateDigest(
        digest_id=_string(payload, "digest_id"),
        manifest_id=_string(payload, "manifest_id"),
        manifest_hash=_string(payload, "manifest_hash"),
        work_unit_id=_string(payload, "work_unit_id"),
        atom_id=_string(payload, "atom_id"),
        candidate_version_ids=_version_id_tuple(
            payload.get("candidate_version_ids"), "triage digest candidate versions"
        ),
        changed_facts=_digest_text_tuple(payload.get("changed_facts"), "changed_facts"),
        source_conflicts=_digest_text_tuple(payload.get("source_conflicts"), "source_conflicts"),
        transmission_paths=_digest_text_tuple(
            payload.get("transmission_paths"), "transmission_paths"
        ),
        countercases=_digest_text_tuple(payload.get("countercases"), "countercases"),
        uncertainty_notes=_digest_text_tuple(payload.get("uncertainty_notes"), "uncertainty_notes"),
        checkpoint_rule_evidence=_digest_text_tuple(
            payload.get("checkpoint_rule_evidence"),
            "checkpoint_rule_evidence",
        ),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        judgment_model_calls_authorized=_boolean(payload, "judgment_model_calls_authorized"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("Triage Candidate Digest is not canonical")
    return result


def triage_cluster_partition_from_dict(value: object) -> TriageClusterPartition:
    payload = _object(value, "Triage Cluster Partition")
    _exact_keys(
        payload,
        {
            "schema_version",
            "partition_id",
            "manifest_id",
            "manifest_hash",
            "ordered_digest_ids",
            "ordered_candidate_version_ids",
            "clusters",
            "historical_pit_claim",
            "judgment_model_calls_authorized",
            "execution_capability",
        },
        "Triage Cluster Partition",
    )
    result = TriageClusterPartition(
        partition_id=_string(payload, "partition_id"),
        manifest_id=_string(payload, "manifest_id"),
        manifest_hash=_string(payload, "manifest_hash"),
        ordered_digest_ids=_prefixed_hash_tuple(
            payload.get("ordered_digest_ids"),
            "event-impact-triage-candidate-digest-",
            "triage partition ordered digest IDs",
        ),
        ordered_candidate_version_ids=_version_id_tuple(
            payload.get("ordered_candidate_version_ids"),
            "triage cluster partition ordered candidate versions",
        ),
        clusters=tuple(
            _cluster_seed_from_dict(item)
            for item in _array(payload.get("clusters"), "triage partition clusters")
        ),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        judgment_model_calls_authorized=_boolean(payload, "judgment_model_calls_authorized"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("Triage Cluster Partition is not canonical")
    return result


def _cluster_seed_from_dict(value: object) -> TriageClusterSeed:
    payload = _object(value, "triage cluster seed")
    _exact_keys(
        payload,
        {
            "cluster_seed_id",
            "digest_ids",
            "atom_ids",
            "candidate_version_ids",
            "merge_state",
            "merge_evidence",
            "uncertainty_notes",
        },
        "triage cluster seed",
    )
    merge_state = TriageClusterMergeState(_string(payload, "merge_state"))
    return TriageClusterSeed(
        cluster_seed_id=_string(payload, "cluster_seed_id"),
        digest_ids=_prefixed_hash_tuple(
            payload.get("digest_ids"),
            "event-impact-triage-candidate-digest-",
            "triage cluster digest IDs",
        ),
        atom_ids=_prefixed_hash_tuple(
            payload.get("atom_ids"),
            "event-impact-triage-work-atom-",
            "triage cluster atom IDs",
        ),
        candidate_version_ids=_version_id_tuple(
            payload.get("candidate_version_ids"), "triage cluster candidate versions"
        ),
        merge_state=merge_state,
        merge_evidence=_text_tuple(
            payload.get("merge_evidence"),
            "triage cluster merge_evidence",
            maximum=MAX_TRIAGE_CLUSTER_MERGE_EVIDENCE,
            minimum=(
                1
                if len(_array(payload.get("atom_ids"), "triage cluster atom IDs")) > 1
                and merge_state is TriageClusterMergeState.MERGED
                else 0
            ),
        ),
        uncertainty_notes=_text_tuple(
            payload.get("uncertainty_notes"),
            "triage cluster uncertainty_notes",
            maximum=MAX_TRIAGE_CLUSTER_UNCERTAINTY_NOTES,
            minimum=1 if merge_state is TriageClusterMergeState.NEEDS_REVIEW else 0,
        ),
    )


def _canonical_digest_texts(values: tuple[str, ...], name: str, *, minimum: int = 0) -> None:
    _canonical_texts(
        values,
        f"triage digest {name}",
        maximum=MAX_TRIAGE_DIGEST_TEXT_ITEMS,
        minimum=minimum,
    )


def _build_digest_texts(values: tuple[str, ...], name: str, *, minimum: int = 0) -> tuple[str, ...]:
    return _build_texts(
        values,
        f"triage digest {name}",
        maximum=MAX_TRIAGE_DIGEST_TEXT_ITEMS,
        minimum=minimum,
    )


def _canonical_texts(values: tuple[str, ...], name: str, *, maximum: int, minimum: int = 0) -> None:
    if not minimum <= len(values) <= maximum:
        raise ValueError(f"{name} must contain between {minimum} and {maximum} items")
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")
    for value in values:
        _trimmed(value, name)
        if len(value) > MAX_TRIAGE_DIGEST_TEXT_CHARS:
            raise ValueError(f"{name} text exceeds the conservative character ceiling")
        normalized = re.sub(r"[\s-]+", "_", value.casefold())
        if any(
            re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", normalized)
            for token in _RESERVED_TRIAGE_CONTROL_TOKENS
        ):
            raise ValueError(f"{name} contains a reserved eligibility, route, or authority token")


def _build_texts(
    values: tuple[object, ...], name: str, *, maximum: int, minimum: int = 0
) -> tuple[str, ...]:
    if not all(isinstance(value, str) for value in values):
        raise TypeError(f"{name} must contain text")
    result = tuple(sorted(set(cast(tuple[str, ...], values))))
    _canonical_texts(result, name, maximum=maximum, minimum=minimum)
    return result


def _digest_text_tuple(value: object, name: str, *, minimum: int = 0) -> tuple[str, ...]:
    return _text_tuple(
        value,
        f"triage digest {name}",
        maximum=MAX_TRIAGE_DIGEST_TEXT_ITEMS,
        minimum=minimum,
    )


def _text_tuple(
    value: object,
    name: str,
    *,
    maximum: int,
    minimum: int = 0,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    values = tuple(cast(list[object], value))
    if not all(isinstance(item, str) for item in values):
        raise TypeError(f"{name} must contain text")
    result = cast(tuple[str, ...], values)
    _canonical_texts(result, name, maximum=maximum, minimum=minimum)
    return result


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    payload = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in payload):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], payload)


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _exact_keys(value: dict[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{name} fields are invalid")


def _string(value: dict[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise TypeError(f"{name} must be text")
    _trimmed(item, name)
    return item


def _boolean(value: dict[str, object], name: str) -> bool:
    item = value.get(name)
    if not isinstance(item, bool):
        raise TypeError(f"{name} must be boolean")
    return item


def _positive_int(value: dict[str, object], name: str) -> int:
    item = value.get(name)
    if not isinstance(item, int) or isinstance(item, bool) or item < 1:
        raise TypeError(f"{name} must be a positive integer")
    return item


def _version_id_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    result = tuple(cast(list[object], value))
    if not all(isinstance(item, str) for item in result):
        raise TypeError(f"{name} must contain text")
    typed = cast(tuple[str, ...], result)
    _version_ids(typed, name)
    return typed


def _prefixed_hash_tuple(value: object, prefix: str, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    result = tuple(cast(list[object], value))
    if not all(isinstance(item, str) for item in result):
        raise TypeError(f"{name} must contain text")
    typed = cast(tuple[str, ...], result)
    _prefixed_hashes(typed, prefix, name)
    return typed


def _version_ids(values: tuple[str, ...], name: str) -> None:
    _prefixed_hashes(values, "prospective-observation-version-", name)


def _prefixed_hashes(values: Iterable[str], prefix: str, name: str) -> None:
    ordered = tuple(values)
    if not ordered or len(set(ordered)) != len(ordered):
        raise ValueError(f"{name} must be non-empty and unique")
    for value in ordered:
        _prefixed_hash(value, prefix, name)


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")
