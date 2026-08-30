from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    TriageObservationRef,
)
from market_impact_agent.event_impact_triage_runtime import TriageCandidateContent
from market_impact_agent.event_impact_triage_work import (
    TriageWorkManifestPolicy,
    build_event_impact_triage_work_manifest,
    event_impact_triage_work_manifest_from_dict,
)

NOW = datetime(2026, 8, 30, 6, tzinfo=UTC)


def _hex(index: int) -> str:
    return f"{index:064x}"


def _candidate_set(
    payloads: tuple[dict[str, object], ...],
) -> tuple[EventImpactTriageCandidateSet, tuple[TriageCandidateContent, ...]]:
    registration_id = f"prospective-diagnostic-registration-{_hex(3001)}"
    checkpoint_key = "next-a-share-policy-event"
    route_plan_id = f"prospective-checkpoint-route-plan-{_hex(3002)}"
    route_admission_id = f"prospective-checkpoint-route-admission-{_hex(3003)}"
    readiness_report_id = f"prospective-checkpoint-readiness-report-{_hex(3004)}"
    data_snapshot_id = f"data-snapshot-{_hex(3005)}"
    observations: list[TriageObservationRef] = []
    contents: list[TriageCandidateContent] = []
    for index, payload in enumerate(payloads, start=1):
        received_at = NOW + timedelta(seconds=index)
        version_id = f"prospective-observation-version-{_hex(index)}"
        observations.append(
            TriageObservationRef(
                version_id=version_id,
                observation_id=f"source-observation-{_hex(1000 + index)}",
                first_available_at=received_at,
                authority_at=received_at,
                provider_id="fixture-provider",
                provider_version="fixture-v1",
                upstream_source="fixture-source",
                source_ref=f"fixture://news/{index}",
                raw_content_hash=_hex(2000 + index),
                normalized_payload_hash=canonical_hash(payload),
            )
        )
        contents.append(
            TriageCandidateContent(
                version_id=version_id,
                normalized_payload=payload,
                license_scope="private_research_no_redistribution",
            )
        )
    ordered = tuple(observations)
    core: dict[str, object] = {
        "schema_version": "market-impact.event-impact-triage-candidate-set.v1",
        "registration_id": registration_id,
        "checkpoint_key": checkpoint_key,
        "route_plan_id": route_plan_id,
        "route_admission_id": route_admission_id,
        "readiness_report_id": readiness_report_id,
        "data_snapshot_id": data_snapshot_id,
        "admitted_at": "2026-08-30T06:00:00Z",
        "frozen_at": "2026-08-30T07:00:00Z",
        "observations": [item.to_dict() for item in ordered],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return (
        EventImpactTriageCandidateSet(
            candidate_set_id=(f"event-impact-triage-candidate-set-{canonical_hash(core)}"),
            registration_id=registration_id,
            checkpoint_key=checkpoint_key,
            route_plan_id=route_plan_id,
            route_admission_id=route_admission_id,
            readiness_report_id=readiness_report_id,
            data_snapshot_id=data_snapshot_id,
            admitted_at=NOW,
            frozen_at=NOW + timedelta(hours=1),
            observations=ordered,
        ),
        tuple(contents),
    )


def _policy(
    *,
    max_atoms_per_work_unit: int = 10,
    max_candidate_versions_per_work_unit: int = 10,
    max_estimated_serialized_prompt_utf8_tokens: int = 2_000,
) -> TriageWorkManifestPolicy:
    return TriageWorkManifestPolicy(
        max_atoms_per_work_unit=max_atoms_per_work_unit,
        max_candidate_versions_per_work_unit=max_candidate_versions_per_work_unit,
        max_estimated_serialized_prompt_utf8_tokens=(max_estimated_serialized_prompt_utf8_tokens),
    )


def test_work_manifest_is_stable_and_has_exact_121_candidate_coverage() -> None:
    candidate_set, contents = _candidate_set(
        tuple(
            {"record": {"headline": f"fixture event {index}", "body": "x" * 80}}
            for index in range(121)
        )
    )
    policy = _policy()

    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=policy,
    )
    repeated = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=policy,
    )

    assert manifest.manifest_id == repeated.manifest_id
    assert manifest.to_dict() == repeated.to_dict()
    assert tuple(unit.ordinal for unit in manifest.work_units) == tuple(
        range(1, len(manifest.work_units) + 1)
    )
    assert (
        tuple(
            version_id for unit in manifest.work_units for version_id in unit.candidate_version_ids
        )
        == candidate_set.version_ids
    )
    assert all(unit.atom_count <= policy.max_atoms_per_work_unit for unit in manifest.work_units)
    assert all(
        unit.candidate_version_count <= policy.max_candidate_versions_per_work_unit
        for unit in manifest.work_units
    )
    assert all(
        unit.estimated_serialized_prompt_utf8_tokens
        <= policy.max_estimated_serialized_prompt_utf8_tokens
        for unit in manifest.work_units
    )
    assert manifest.ordered_candidate_version_ids == candidate_set.version_ids
    assert manifest.historical_pit_claim is False
    assert manifest.judgment_model_calls_authorized is False
    assert manifest.execution_capability is False
    assert "labels" not in manifest.to_dict()


def test_work_manifest_collapses_exact_nonadjacent_normalized_payload_duplicates() -> None:
    duplicate: dict[str, object] = {"record": {"headline": "same source payload"}}
    distinct: dict[str, object] = {"record": {"headline": "another payload"}}
    candidate_set, contents = _candidate_set((duplicate, distinct, duplicate))

    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=_policy(max_atoms_per_work_unit=1, max_candidate_versions_per_work_unit=3),
    )

    assert len(manifest.atoms) == 2
    assert manifest.atoms[0].candidate_version_ids == (
        candidate_set.version_ids[0],
        candidate_set.version_ids[2],
    )
    assert manifest.ordered_candidate_version_ids == candidate_set.version_ids
    assert manifest.work_units[0].candidate_version_ids == (
        candidate_set.version_ids[0],
        candidate_set.version_ids[2],
    )
    assert manifest.work_units[1].candidate_version_ids == (candidate_set.version_ids[1],)


def test_work_manifest_identity_changes_for_frozen_policy_or_candidate_content() -> None:
    source_payloads: tuple[dict[str, object], ...] = (
        {"record": {"headline": "first"}},
        {"record": {"headline": "second"}},
    )
    candidate_set, contents = _candidate_set(source_payloads)
    policy = _policy()
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=policy,
    )
    changed_policy = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=replace(policy, max_atoms_per_work_unit=2),
    )
    changed_first: dict[str, object] = {"record": {"headline": "first changed"}}
    changed_candidate_set, changed_contents = _candidate_set((changed_first, source_payloads[1]))
    changed_content = build_event_impact_triage_work_manifest(
        candidate_set=changed_candidate_set,
        contents=changed_contents,
        policy=policy,
    )

    assert manifest.manifest_id != changed_policy.manifest_id
    assert manifest.manifest_id != changed_content.manifest_id
    with pytest.raises(ValueError, match="Candidate Set"):
        manifest.validate_against(changed_candidate_set)


def test_work_manifest_rejects_oversize_singleton_and_more_than_128_atoms() -> None:
    candidate_set, contents = _candidate_set(({"record": {"body": "x" * 1_000}},))

    with pytest.raises(ValueError, match="singleton"):
        build_event_impact_triage_work_manifest(
            candidate_set=candidate_set,
            contents=contents,
            policy=_policy(max_estimated_serialized_prompt_utf8_tokens=1),
        )

    many_candidate_set, many_contents = _candidate_set(
        tuple({"record": {"headline": f"unique {index}"}} for index in range(129))
    )
    with pytest.raises(ValueError, match="128"):
        build_event_impact_triage_work_manifest(
            candidate_set=many_candidate_set,
            contents=many_contents,
            policy=_policy(
                max_atoms_per_work_unit=128,
                max_candidate_versions_per_work_unit=128,
            ),
        )


def test_work_manifest_is_strict_round_trippable_and_validates_its_schema() -> None:
    candidate_set, contents = _candidate_set(({"record": {"headline": "fixture"}},))
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=_policy(),
    )
    payload = manifest.to_dict()

    assert event_impact_triage_work_manifest_from_dict(payload) == manifest
    assert (
        validate_agent_contract(
            payload,
            "event-impact-triage-work-manifest.schema.json",
        )
        == ()
    )
    baseline_manifest_id = manifest.manifest_id
    treatment_manifest_id = manifest.manifest_id
    assert baseline_manifest_id == treatment_manifest_id
    assert "arm" not in payload

    with_extra_field = deepcopy(payload)
    with_extra_field["labels"] = []
    with pytest.raises(ValueError, match="fields"):
        event_impact_triage_work_manifest_from_dict(with_extra_field)

    with_authority = deepcopy(payload)
    with_authority["execution_capability"] = True
    with pytest.raises(ValueError, match="authority"):
        event_impact_triage_work_manifest_from_dict(with_authority)
