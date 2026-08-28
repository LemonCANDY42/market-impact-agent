from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.csrc_news import (
    CsrcNewsHTTPResponse,
    CsrcNewsProvider,
    load_csrc_news_source,
)
from market_impact_agent.data_inputs import DataSourceBinding, LocalDataSnapshotStore
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_collection_runtime import (
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionJob,
)
from market_impact_agent.prospective_collectors import collect_prospective_source_snapshot
from market_impact_agent.prospective_data import ProspectiveCollectionPolicy
from market_impact_agent.source_acceptance import (
    SourceAcceptanceGate,
    SourceAcceptanceGateResult,
    SourceAcceptanceStatus,
    SourceRouteAcceptanceDeclaration,
    SourceRouteAcceptanceReport,
)
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)

RETRIEVED = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
WINDOW_START = RETRIEVED - timedelta(hours=1)
CSRC_CONFIG_PATH = Path("examples/providers/csrc-official-news-v1.json")
TUSHARE_CONFIG_PATH = Path("examples/providers/tushare-observation-index-daily-v1.json")


class FakeCsrcHTTPClient:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.calls = 0

    def get(self, url: str, *, max_response_bytes: int) -> CsrcNewsHTTPResponse:
        assert parse_qs(urlsplit(url).query)["page"] == ["1"]
        assert len(self.body) < max_response_bytes
        self.calls += 1
        return CsrcNewsHTTPResponse(
            body=self.body,
            final_url=url,
            content_type="application/json",
        )


class FakeTushareTransport:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.calls = 0

    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
        assert endpoint == "https://api.tushare.pro"
        assert b"private-test-token" in body
        assert timeout_seconds == 5.0
        self.calls += 1
        return self.response


def _accepted_report(
    *,
    source: DataSourceBinding,
    capability: ObservationCapability,
) -> SourceRouteAcceptanceReport:
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        provider_manifest_hash=source.manifest_hash,
        source_config_hash=source.source_config_hash or "",
        upstream_source=source.upstream_source,
        capability=capability,
        rights_basis_url="https://official.example/terms",
        rights_reviewed_at=WINDOW_START,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=False,
        semantic_scope="prospective fixture route",
        revision_strategy="append_only_content_versions",
    )
    gates = tuple(
        SourceAcceptanceGateResult(
            gate=gate.value,
            status=SourceAcceptanceStatus.PASS.value,
            reasons=(),
        )
        for gate in SourceAcceptanceGate
    )
    core = {
        "schema_version": "market-impact.source-route-acceptance-report.v1",
        "declaration": declaration.to_dict(),
        "rights_evidence": None,
        "data_snapshot_id": "data-snapshot-accepted-fixture",
        "deterministic_replay_snapshot_id": "data-snapshot-accepted-fixture",
        "evaluated_at": RETRIEVED.isoformat().replace("+00:00", "Z"),
        "gates": [item.to_dict() for item in gates],
        "accepted": True,
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }
    return SourceRouteAcceptanceReport(
        report_id=f"source-route-acceptance-report-{canonical_hash(core)}",
        declaration=declaration,
        rights_evidence=None,
        data_snapshot_id="data-snapshot-accepted-fixture",
        deterministic_replay_snapshot_id="data-snapshot-accepted-fixture",
        evaluated_at=RETRIEVED,
        gates=gates,
        accepted=True,
    )


def _job(
    *,
    adapter_kind: ProspectiveCollectionAdapterKind,
    policy: ProspectiveCollectionPolicy,
    source_config: dict[str, object],
) -> ProspectiveCollectionJob:
    return ProspectiveCollectionJob.build(
        adapter_kind=adapter_kind,
        collection_policy=policy,
        source_acceptance_report=_accepted_report(
            source=policy.sources[0],
            capability=policy.capability,
        ),
        source_config=source_config,
        starts_at=RETRIEVED,
        misfire_grace_seconds=30,
        maximum_jitter_seconds=0,
        provider_timeout_seconds=5.0,
    )


def test_csrc_collector_materializes_a_policy_bound_snapshot(tmp_path: Path) -> None:
    config = load_csrc_news_source(CSRC_CONFIG_PATH)
    provider = CsrcNewsProvider((config,))
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(source,),
        window_start=WINDOW_START,
        parameters={"keywords": [], "max_items": 20},
        poll_interval_seconds=300,
        maximum_gap_seconds=900,
    )
    record = {
        "title": "Policy update",
        "url": "//www.csrc.gov.cn/csrc/c100028/c123/content.shtml",
        "publishedTimeStr": "2026-08-28 21:30:00",
        "memo": "Policy update summary",
        "channelId": config.channel_id,
        "contentHtml": "<p>Policy update body</p>",
    }
    body = json.dumps(
        {
            "data": {
                "page": 1,
                "rows": config.page_size,
                "channelId": config.channel_id,
                "total": 1,
                "results": [record],
            }
        },
        separators=(",", ":"),
    ).encode()
    http_client = FakeCsrcHTTPClient(body)
    store = LocalDataSnapshotStore(tmp_path / "state")

    snapshot = collect_prospective_source_snapshot(
        job=_job(
            adapter_kind=ProspectiveCollectionAdapterKind.CSRC_NEWS,
            policy=policy,
            source_config=config.to_dict(),
        ),
        policy=policy,
        source_config=config.to_dict(),
        store=store,
        csrc_http_client=http_client,
        clock=lambda: RETRIEVED,
    )

    assert snapshot.coverage_complete is True
    assert len(snapshot.observations) == 1
    assert snapshot.query.source_policy_id == policy.policy_id
    assert store.get(snapshot.snapshot_id) == snapshot
    assert http_client.calls == 1


def test_tushare_collector_keeps_the_token_out_of_persisted_state(tmp_path: Path) -> None:
    config = load_tushare_observation_source(TUSHARE_CONFIG_PATH)
    provider = TushareObservationProvider("private-test-token", (config,))
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=config.capability,
        sources=(source,),
        window_start=WINDOW_START,
        parameters={
            "ts_code": "000300.SH",
            "start_date": "20260828",
            "end_date": "20270828",
        },
        poll_interval_seconds=86400,
        maximum_gap_seconds=172800,
    )
    response = json.dumps(
        {
            "code": 0,
            "msg": None,
            "data": {
                "fields": list(config.fields),
                "items": [
                    [
                        "000300.SH",
                        "20260828",
                        4000.0,
                        4050.0,
                        3990.0,
                        4030.0,
                        3980.0,
                        50.0,
                        1.2563,
                        1000000.0,
                        2000000.0,
                    ]
                ],
            },
        },
        separators=(",", ":"),
    ).encode()
    transport = FakeTushareTransport(response)
    store = LocalDataSnapshotStore(tmp_path / "state")

    snapshot = collect_prospective_source_snapshot(
        job=_job(
            adapter_kind=ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION,
            policy=policy,
            source_config=config.to_dict(),
        ),
        policy=policy,
        source_config=config.to_dict(),
        store=store,
        tushare_token="private-test-token",
        tushare_transport=transport,
        clock=lambda: RETRIEVED,
    )

    assert snapshot.coverage_complete is True
    assert len(snapshot.observations) == 1
    assert transport.calls == 1
    for path in (tmp_path / "state").rglob("*"):
        if path.is_file():
            assert b"private-test-token" not in path.read_bytes()
