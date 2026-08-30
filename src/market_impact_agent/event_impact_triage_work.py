from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.event_impact_triage import EventImpactTriageCandidateSet

if TYPE_CHECKING:
    from market_impact_agent.event_impact_triage_runtime import TriageCandidateContent

EVENT_IMPACT_TRIAGE_WORK_MANIFEST_SCHEMA = "market-impact.event-impact-triage-work-manifest.v1"
TRIAGE_WORK_MANIFEST_CONTENT_VIEW = "normalized-observation-payload-v1"
TRIAGE_WORK_MANIFEST_TOKEN_ESTIMATOR = "canonical-json-utf8-upper-bound-v1"
TRIAGE_WORK_MANIFEST_SERIALIZED_PROMPT_FORMAT = "triage-work-unit-content.v1"
MAX_TRIAGE_WORK_ATOMS = 128


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
