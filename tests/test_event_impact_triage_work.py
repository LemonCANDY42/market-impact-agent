from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    TriageObservationRef,
)
from market_impact_agent.event_impact_triage_runtime import TriageCandidateContent
from market_impact_agent.event_impact_triage_work import (
    EventImpactTriageWorkManifest,
    TriageCandidateDigest,
    TriageClusterMergeState,
    TriageClusterPartition,
    TriageClusterSeed,
    TriageWorkManifestPolicy,
    build_event_impact_triage_work_manifest,
    event_impact_triage_work_manifest_from_dict,
    triage_candidate_digest_from_dict,
    triage_cluster_partition_from_dict,
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

    duplicate_candidate_set, duplicate_contents = _candidate_set(
        tuple({"record": {"headline": "same"}} for _ in range(129))
    )
    with pytest.raises(ValueError, match="candidate-version cap"):
        build_event_impact_triage_work_manifest(
            candidate_set=duplicate_candidate_set,
            contents=duplicate_contents,
            policy=_policy(
                max_atoms_per_work_unit=128,
                max_candidate_versions_per_work_unit=128,
            ),
        )
    with pytest.raises(ValueError, match="batch cap"):
        _policy(max_candidate_versions_per_work_unit=129)


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


def _digest(manifest: EventImpactTriageWorkManifest, atom_id: str) -> TriageCandidateDigest:
    return TriageCandidateDigest.build(
        manifest=manifest,
        atom_id=atom_id,
        changed_facts=(f"Observed changed fact for {atom_id[-12:]}",),
        source_conflicts=(),
        transmission_paths=("Potential transmission requires later bounded classification.",),
        countercases=("The observed content may be incomplete or superseded.",),
        uncertainty_notes=("No eligibility or route conclusion is made at digest stage.",),
        checkpoint_rule_evidence=("The frozen checkpoint rule is retained as evidence only.",),
    )


def _seed(
    manifest: EventImpactTriageWorkManifest,
    digests: tuple[TriageCandidateDigest, ...],
    *,
    merge_state: TriageClusterMergeState = TriageClusterMergeState.MERGED,
) -> TriageClusterSeed:
    return TriageClusterSeed.build(
        manifest=manifest,
        digests=digests,
        atom_ids=tuple(item.atom_id for item in digests),
        merge_state=merge_state,
        merge_evidence=("The bounded digest evidence shares one provisional event seed.",),
        uncertainty_notes=(
            ("Cross-unit similarity remains uncertain.",)
            if merge_state is TriageClusterMergeState.NEEDS_REVIEW
            else ()
        ),
    )


def test_digest_and_partition_cover_a_121_candidate_cross_work_unit_batch() -> None:
    candidate_set, contents = _candidate_set(
        tuple({"record": {"headline": f"fixture event {index}"}} for index in range(121))
    )
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=_policy(max_atoms_per_work_unit=10, max_candidate_versions_per_work_unit=10),
    )
    digests = tuple(_digest(manifest, atom.atom_id) for atom in manifest.atoms)
    cross_unit_seed = _seed(
        manifest,
        (digests[0], digests[-1]),
        merge_state=TriageClusterMergeState.NEEDS_REVIEW,
    )
    clusters = (
        cross_unit_seed,
        *(_seed(manifest, (digest,)) for digest in digests[1:-1]),
    )

    partition = TriageClusterPartition.build(
        manifest=manifest,
        digests=digests,
        clusters=clusters,
    )
    repeated = TriageClusterPartition.build(
        manifest=manifest,
        digests=digests,
        clusters=clusters,
    )

    assert len(manifest.work_units) > 1
    assert cross_unit_seed.atom_ids == (manifest.atoms[0].atom_id, manifest.atoms[-1].atom_id)
    assert partition.partition_id == repeated.partition_id
    assert partition.ordered_digest_ids == tuple(item.digest_id for item in digests)
    assert {atom_id for cluster in partition.clusters for atom_id in cluster.atom_ids} == {
        atom.atom_id for atom in manifest.atoms
    }
    assert {digest_id for cluster in partition.clusters for digest_id in cluster.digest_ids} == {
        digest.digest_id for digest in digests
    }
    assert partition.historical_pit_claim is False
    assert partition.judgment_model_calls_authorized is False
    assert partition.execution_capability is False
    assert "labels" not in partition.to_dict()
    assert "checkpoint_eligibility" not in partition.to_dict()
    assert "recommended_route" not in partition.to_dict()


def test_partition_rejects_duplicate_missing_unknown_and_mismatched_digest_bindings() -> None:
    candidate_set, contents = _candidate_set(
        (
            {"record": {"headline": "one"}},
            {"record": {"headline": "two"}},
        )
    )
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=_policy(max_atoms_per_work_unit=1, max_candidate_versions_per_work_unit=1),
    )
    digests = tuple(_digest(manifest, atom.atom_id) for atom in manifest.atoms)
    clusters = tuple(_seed(manifest, (digest,)) for digest in digests)

    partition = TriageClusterPartition.build(
        manifest=manifest,
        digests=digests,
        clusters=clusters,
    )
    reordered = TriageClusterPartition.build(
        manifest=manifest,
        digests=tuple(reversed(digests)),
        clusters=tuple(reversed(clusters)),
    )
    assert reordered.partition_id == partition.partition_id

    with pytest.raises(ValueError, match="unique"):
        TriageClusterPartition.build(
            manifest=manifest,
            digests=(digests[0], digests[0], digests[1]),
            clusters=clusters,
        )
    with pytest.raises(ValueError, match="every Work Atom"):
        TriageClusterPartition.build(
            manifest=manifest,
            digests=(digests[0],),
            clusters=(_seed(manifest, (digests[0],)),),
        )

    unknown_atom_id = f"event-impact-triage-work-atom-{_hex(9999)}"
    with pytest.raises(ValueError, match="unknown"):
        TriageClusterSeed.build(
            manifest=manifest,
            digests=(digests[0],),
            atom_ids=(unknown_atom_id,),
            merge_state=TriageClusterMergeState.MERGED,
            merge_evidence=("Fixture merge evidence.",),
        )

    other_set, other_contents = _candidate_set(({"record": {"headline": "other"}},))
    other_manifest = build_event_impact_triage_work_manifest(
        candidate_set=other_set,
        contents=other_contents,
        policy=_policy(),
    )
    other_digest = _digest(other_manifest, other_manifest.atoms[0].atom_id)
    with pytest.raises(ValueError, match="another manifest"):
        TriageClusterSeed.build(
            manifest=manifest,
            digests=(other_digest,),
            atom_ids=(other_digest.atom_id,),
            merge_state=TriageClusterMergeState.MERGED,
            merge_evidence=("Fixture merge evidence.",),
        )

    with pytest.raises(ValueError, match="between 1 and 8 items"):
        TriageClusterSeed.build(
            manifest=manifest,
            digests=digests,
            atom_ids=tuple(item.atom_id for item in manifest.atoms),
            merge_state=TriageClusterMergeState.MERGED,
            merge_evidence=(),
        )

    partial_payload = deepcopy(partition.to_dict())
    partial_clusters = partial_payload["clusters"]
    assert isinstance(partial_clusters, list)
    partial_payload["clusters"] = partial_clusters[:1]
    partial_core = {key: value for key, value in partial_payload.items() if key != "partition_id"}
    partial_payload["partition_id"] = (
        f"event-impact-triage-cluster-partition-{canonical_hash(partial_core)}"
    )
    with pytest.raises(ValueError, match="consume every digest exactly once"):
        triage_cluster_partition_from_dict(partial_payload)


def test_digest_and_partition_are_closed_round_trippable_and_schema_valid() -> None:
    candidate_set, contents = _candidate_set(({"record": {"headline": "fixture"}},))
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=_policy(),
    )
    digest = _digest(manifest, manifest.atoms[0].atom_id)
    seed = _seed(manifest, (digest,))
    partition = TriageClusterPartition.build(
        manifest=manifest,
        digests=(digest,),
        clusters=(seed,),
    )

    digest_payload = digest.to_dict()
    partition_payload = partition.to_dict()
    assert triage_candidate_digest_from_dict(digest_payload) == digest
    assert triage_cluster_partition_from_dict(partition_payload) == partition
    assert (
        validate_agent_contract(
            digest_payload,
            "event-impact-triage-candidate-digest.schema.json",
        )
        == ()
    )
    assert (
        validate_agent_contract(
            partition_payload,
            "event-impact-triage-cluster-partition.schema.json",
        )
        == ()
    )

    with_label = deepcopy(digest_payload)
    with_label["gold_label"] = "eligible"
    with pytest.raises(ValueError, match="fields"):
        triage_candidate_digest_from_dict(with_label)

    with_authority = deepcopy(partition_payload)
    with_authority["execution_capability"] = True
    with pytest.raises(ValueError, match="authority"):
        triage_cluster_partition_from_dict(with_authority)

    for reserved_text in (
        "gold_label=eligible; recommended_route=immediate",
        "execution_capability=true",
        "label-set-id was disclosed",
        "needs review",
    ):
        with pytest.raises(ValueError, match="reserved eligibility, route, or authority token"):
            TriageCandidateDigest.build(
                manifest=manifest,
                atom_id=manifest.atoms[0].atom_id,
                changed_facts=(reserved_text,),
                uncertainty_notes=(),
                checkpoint_rule_evidence=(),
            )
        reserved_payload = deepcopy(digest_payload)
        reserved_payload["changed_facts"] = [reserved_text]
        reserved_core = {
            key: value for key, value in reserved_payload.items() if key != "digest_id"
        }
        reserved_payload["digest_id"] = (
            f"event-impact-triage-candidate-digest-{canonical_hash(reserved_core)}"
        )
        assert validate_agent_contract(
            reserved_payload,
            "event-impact-triage-candidate-digest.schema.json",
        )
        with pytest.raises(ValueError, match="reserved eligibility, route, or authority token"):
            triage_candidate_digest_from_dict(reserved_payload)

    oversize_partition = deepcopy(partition_payload)
    oversize_versions = [
        f"prospective-observation-version-{_hex(5000 + index)}" for index in range(129)
    ]
    oversize_partition["ordered_candidate_version_ids"] = oversize_versions
    oversize_clusters = cast(list[object], oversize_partition["clusters"])
    typed_source_cluster = cast(dict[str, object], oversize_clusters[0])
    first_cluster = deepcopy(typed_source_cluster)
    second_cluster = deepcopy(typed_source_cluster)
    first_cluster["cluster_seed_id"] = f"event-impact-triage-cluster-seed-{_hex(7001)}"
    first_cluster["digest_ids"] = [f"event-impact-triage-candidate-digest-{_hex(7101)}"]
    first_cluster["atom_ids"] = [f"event-impact-triage-work-atom-{_hex(7201)}"]
    first_cluster["candidate_version_ids"] = oversize_versions[:65]
    second_cluster["cluster_seed_id"] = f"event-impact-triage-cluster-seed-{_hex(7002)}"
    second_cluster["digest_ids"] = [f"event-impact-triage-candidate-digest-{_hex(7102)}"]
    second_cluster["atom_ids"] = [f"event-impact-triage-work-atom-{_hex(7202)}"]
    second_cluster["candidate_version_ids"] = oversize_versions[65:]
    first_digest_ids = cast(list[object], first_cluster["digest_ids"])
    second_digest_ids = cast(list[object], second_cluster["digest_ids"])
    assert isinstance(first_digest_ids, list) and isinstance(second_digest_ids, list)
    oversize_partition["ordered_digest_ids"] = [*first_digest_ids, *second_digest_ids]
    oversize_partition["clusters"] = [first_cluster, second_cluster]
    assert validate_agent_contract(
        oversize_partition,
        "event-impact-triage-cluster-partition.schema.json",
    )


def test_digest_may_preserve_unsupported_extraction_as_empty_without_inventing_facts() -> None:
    candidate_set, contents = _candidate_set(({"record": {"headline": "opaque fixture"}},))
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=_policy(),
    )

    digest = TriageCandidateDigest.build(
        manifest=manifest,
        atom_id=manifest.atoms[0].atom_id,
        changed_facts=(),
        source_conflicts=(),
        transmission_paths=(),
        countercases=(),
        uncertainty_notes=(),
        checkpoint_rule_evidence=(),
    )

    assert triage_candidate_digest_from_dict(digest.to_dict()) == digest
    assert (
        validate_agent_contract(
            digest.to_dict(),
            "event-impact-triage-candidate-digest.schema.json",
        )
        == ()
    )
