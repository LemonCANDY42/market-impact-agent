from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.source_acceptance import (
    SourceRightsEvidence,
    SourceRouteAcceptanceDeclaration,
    SourceRouteReplayRequest,
    SourceRouteReplayResult,
    load_source_route_acceptance_report,
    qualify_source_route,
    source_route_acceptance_report_from_dict,
    write_source_route_acceptance_report,
)

RETRIEVED = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
PUBLISHED = datetime(2026, 8, 28, 7, 55, tzinfo=UTC)
MANIFEST_HASH = canonical_hash({"provider": "csrc-official-news", "version": "1"})
CONFIG_HASH = canonical_hash({"source": "csrc-official-news"})
RECORD_BYTES = b'{"title":"regulatory policy"}'
RESPONSE_BYTES = b'{"data":{"results":[{"title":"regulatory policy"}]}}'
RIGHTS_BYTES = b"CSRC website legal statement"


def _snapshot() -> DataSnapshot:
    source = DataSourceBinding(
        provider_id="csrc-official-news",
        provider_version="1",
        upstream_source="csrc-official-news",
        manifest_hash=MANIFEST_HASH,
        source_config_hash=CONFIG_HASH,
        required=True,
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=RETRIEVED,
        window_start=PUBLISHED,
        source_policy_id="csrc-official-news-prospective-v1",
        parameters={"keywords": [], "max_items": 20},
        sources=(source,),
        minimum_data_sources=1,
    )
    observation = SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        upstream_record_id="c7652148",
        source_ref="https://www.csrc.gov.cn/csrc/c100028/c7652148/content.shtml",
        lineage_id="csrc-official-news:c7652148",
        times=ObservationTimes(
            occurred_at=RETRIEVED,
            published_at=PUBLISHED,
            available_at=RETRIEVED,
            source_updated_at=None,
            aggregator_fetched_at=None,
            retrieved_at=RETRIEVED,
            occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        ),
        authority_at=RETRIEVED,
        authority_kind="actual_receipt",
        raw_content_hash=sha256(RECORD_BYTES).hexdigest(),
        normalized_payload={"headline": "监管政策发布", "publisher": "中国证监会"},
        license_scope="official_public_private_research_no_redistribution",
    )
    attempt = DataProviderAttempt(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        required=True,
        status=DataFetchStatus.DATA,
        retrieved_at=RETRIEVED,
        raw_response_hash=sha256(RESPONSE_BYTES).hexdigest(),
        received_count=1,
        accepted_count=1,
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None,
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [observation.to_dict()],
        "coverage_complete": True,
        "completed_at": "2026-08-28T08:00:00Z",
    }
    return DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=(attempt,),
        observations=(observation,),
        coverage_complete=True,
        completed_at=RETRIEVED,
    )


def _declaration() -> SourceRouteAcceptanceDeclaration:
    return SourceRouteAcceptanceDeclaration.build(
        provider_id="csrc-official-news",
        provider_version="1",
        provider_manifest_hash=MANIFEST_HASH,
        source_config_hash=CONFIG_HASH,
        upstream_source="csrc-official-news",
        capability=ObservationCapability.EVENT_REVELATION,
        rights_basis_url=("https://www.csrc.gov.cn/csrc/c100227/c1362477/content.shtml"),
        rights_reviewed_at=datetime(2026, 8, 28, tzinfo=UTC),
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=False,
        semantic_scope="official_capital_market_policy_publication",
        revision_strategy="append_only_content_versions",
    )


def _rights_evidence() -> SourceRightsEvidence:
    return SourceRightsEvidence.build(
        source_ref="https://www.csrc.gov.cn/csrc/c100227/c1362477/content.shtml",
        final_url="https://www.csrc.gov.cn/csrc/c100227/c1362477/content.shtml",
        retrieved_at=RETRIEVED,
        raw_content_hash=sha256(RIGHTS_BYTES).hexdigest(),
    )


def _stored_snapshot(
    tmp_path: Path,
    snapshot: DataSnapshot,
    *,
    include_rights: bool = True,
) -> LocalDataSnapshotStore:
    store = LocalDataSnapshotStore(tmp_path)
    assert store.put_raw(RESPONSE_BYTES) == snapshot.attempts[0].raw_response_hash
    assert store.put_raw(RECORD_BYTES) == snapshot.observations[0].raw_content_hash
    if include_rights:
        assert store.put_raw(RIGHTS_BYTES) == _rights_evidence().raw_content_hash
    store.put(snapshot)
    return store


def _reimport_from_stored_artifacts(
    replay_store: LocalDataSnapshotStore,
    snapshot: DataSnapshot,
) -> Callable[[SourceRouteReplayRequest], SourceRouteReplayResult]:
    def reimport(request: SourceRouteReplayRequest) -> SourceRouteReplayResult:
        assert request.source_snapshot_id == snapshot.snapshot_id
        assert request.raw_response_payload == RESPONSE_BYTES
        assert request.rights_payload == RIGHTS_BYTES
        assert (
            replay_store.put_raw(request.raw_response_payload)
            == snapshot.attempts[0].raw_response_hash
        )
        assert replay_store.put_raw(RECORD_BYTES) == snapshot.observations[0].raw_content_hash
        replay_store.put(snapshot)
        return SourceRouteReplayResult(
            snapshot_id=snapshot.snapshot_id,
            store=replay_store,
        )

    return reimport


def test_source_route_acceptance_requires_all_seven_gates_and_is_schema_valid(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)
    replay_store = _stored_snapshot(tmp_path / "replay", snapshot, include_rights=False)

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=_reimport_from_stored_artifacts(replay_store, snapshot),
        evaluated_at=RETRIEVED,
    )

    assert report.accepted is True
    assert [item.gate for item in report.gates] == [
        "rights_and_identity",
        "transport",
        "completeness",
        "time_and_revisions",
        "market_semantics",
        "determinism_and_storage",
        "agent_isolation",
    ]
    assert all(item.status == "pass" for item in report.gates)
    assert report.historical_pit_claim is False
    assert report.evidence_promoted is False
    assert report.execution_capability is False
    assert (
        validate_agent_contract(
            report.to_dict(),
            "source-route-acceptance-report.schema.json",
        )
        == ()
    )


def test_source_route_acceptance_reimports_persisted_raw_and_rights_artifacts(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)
    replay_store = LocalDataSnapshotStore(tmp_path / "replay")

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=_reimport_from_stored_artifacts(replay_store, snapshot),
        evaluated_at=RETRIEVED,
    )

    assert report.accepted is True
    assert report.deterministic_replay_snapshot_id == snapshot.snapshot_id


def test_source_route_acceptance_rejects_an_unpersisted_replay_result(tmp_path: Path) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)
    replay_store = LocalDataSnapshotStore(tmp_path / "replay")

    def unpersisted_replay(request: SourceRouteReplayRequest) -> SourceRouteReplayResult:
        assert request.raw_response_payload == RESPONSE_BYTES
        assert request.rights_payload == RIGHTS_BYTES
        return SourceRouteReplayResult(
            snapshot_id=snapshot.snapshot_id,
            store=replay_store,
        )

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=unpersisted_replay,
        evaluated_at=RETRIEVED,
    )

    determinism = next(item for item in report.gates if item.gate == "determinism_and_storage")
    assert report.accepted is False
    assert determinism.reasons == ("deterministic_replay_storage_invalid",)


def test_source_route_acceptance_does_not_replay_with_a_tampered_raw_artifact(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)
    raw_response_hash = snapshot.attempts[0].raw_response_hash
    assert raw_response_hash is not None
    source_store.artifacts.get(
        raw_response_hash,
        media_type="application/octet-stream",
    ).path.write_bytes(b"fabricated raw response")
    replay_calls: list[SourceRouteReplayRequest] = []

    def replay(request: SourceRouteReplayRequest) -> SourceRouteReplayResult:
        replay_calls.append(request)
        raise AssertionError("tampered source artifacts must not be replayed")

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=replay,
        evaluated_at=RETRIEVED,
    )

    determinism = next(item for item in report.gates if item.gate == "determinism_and_storage")
    assert report.accepted is False
    assert replay_calls == []
    assert "source_raw_storage_invalid" in determinism.reasons
    assert "deterministic_replay_source_artifacts_invalid" in determinism.reasons


def test_source_route_acceptance_fails_closed_without_deterministic_replay(tmp_path: Path) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=None,
        evaluated_at=RETRIEVED,
    )

    assert report.accepted is False
    determinism = next(item for item in report.gates if item.gate == "determinism_and_storage")
    assert determinism.status == "fail"
    assert determinism.reasons == ("deterministic_replay_missing",)


def test_source_route_acceptance_fails_closed_without_captured_rights_basis(tmp_path: Path) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot, include_rights=False)
    replay_store = _stored_snapshot(tmp_path / "replay", snapshot, include_rights=False)

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=None,
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=_reimport_from_stored_artifacts(replay_store, snapshot),
        evaluated_at=RETRIEVED,
    )

    assert report.accepted is False
    rights = next(item for item in report.gates if item.gate == "rights_and_identity")
    assert rights.reasons == ("rights_evidence_missing",)


def test_source_route_acceptance_report_is_written_to_private_content_identified_path(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)
    replay_store = _stored_snapshot(tmp_path / "replay", snapshot, include_rights=False)
    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=_reimport_from_stored_artifacts(replay_store, snapshot),
        evaluated_at=RETRIEVED,
    )

    first = write_source_route_acceptance_report(report, tmp_path / "acceptance")
    second = write_source_route_acceptance_report(report, tmp_path / "acceptance")

    assert first == second
    assert first.name == f"{report.report_id}.json"
    assert first.stat().st_mode & 0o777 == 0o600


def test_source_route_acceptance_report_parser_is_canonical_and_closed(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    source_store = _stored_snapshot(tmp_path / "source", snapshot)
    replay_store = LocalDataSnapshotStore(tmp_path / "replay")
    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=_reimport_from_stored_artifacts(replay_store, snapshot),
        evaluated_at=RETRIEVED,
    )
    payload = report.to_dict()
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    assert source_route_acceptance_report_from_dict(payload) == report
    assert load_source_route_acceptance_report(report_path) == report

    extra_field = {**payload, "unexpected": True}
    with pytest.raises(ValueError, match="fields are invalid"):
        source_route_acceptance_report_from_dict(extra_field)

    noncanonical = {**payload, "evaluated_at": "2026-08-28T08:00:00+00:00"}
    with pytest.raises(ValueError, match="canonical contract"):
        source_route_acceptance_report_from_dict(noncanonical)


def test_source_route_acceptance_fails_when_declared_raw_storage_is_missing(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    source_store = LocalDataSnapshotStore(tmp_path / "source")
    source_store.put(snapshot)
    replay_store = _stored_snapshot(tmp_path / "replay", snapshot, include_rights=False)

    report = qualify_source_route(
        declaration=_declaration(),
        rights_evidence=_rights_evidence(),
        snapshot=snapshot,
        source_store=source_store,
        replay_from_stored_artifacts=_reimport_from_stored_artifacts(replay_store, snapshot),
        evaluated_at=RETRIEVED,
    )

    assert report.accepted is False
    determinism = next(item for item in report.gates if item.gate == "determinism_and_storage")
    assert "source_raw_storage_invalid" in determinism.reasons
