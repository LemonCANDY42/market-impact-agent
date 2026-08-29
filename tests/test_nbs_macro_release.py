from __future__ import annotations

import asyncio
import io
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.cli import accept_nbs_macro_release_source
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSourceBinding,
    LocalDataSnapshotStore,
)
from market_impact_agent.nbs_macro_release import (
    NBS_MACRO_RELEASE_FEED_URL,
    NBS_MACRO_RELEASE_REVISION_STRATEGY,
    NBS_MACRO_RELEASE_RIGHTS_URL,
    NBS_MACRO_RELEASE_SEMANTIC_SCOPE,
    NbsMacroReleaseHTTPResponse,
    NbsMacroReleaseParseError,
    NbsMacroReleaseProvider,
    load_nbs_macro_release_capture_bundle,
    load_nbs_macro_release_source,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    OccurrenceBasis,
)
from market_impact_agent.prospective_collection_runtime import (
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionJob,
    ProspectiveCollectionRuntime,
)
from market_impact_agent.prospective_collectors import collect_prospective_source_snapshot
from market_impact_agent.prospective_data import ProspectiveCollectionPolicy
from market_impact_agent.source_acceptance import load_source_route_acceptance_report

RETRIEVED = datetime(2026, 8, 29, 5, 0, tzinfo=UTC)
WINDOW_START = datetime(2026, 8, 1, tzinfo=UTC)
CONFIG_PATH = Path("examples/providers/nbs-macro-release-cpi-ppi-v1.json")
CPI_URL = "https://www.stats.gov.cn/sj/zxfb/202608/t20260809_1965008.html"
PPI_URL = "https://www.stats.gov.cn/sj/zxfb/202608/t20260809_1965007.html"
CPI_XLSX = "https://www.stats.gov.cn/sj/zxfb/202608/P020260809325111279255.xlsx"
PPI_XLSX = "https://www.stats.gov.cn/sj/zxfb/202608/P020260809323921569637.xlsx"


class FakeHTTPClient:
    def __init__(self, responses: dict[str, NbsMacroReleaseHTTPResponse]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, *, max_response_bytes: int) -> NbsMacroReleaseHTTPResponse:
        response = self.responses[url]
        assert len(response.body) <= max_response_bytes
        self.calls.append(url)
        return response


def _xlsx() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("xl/workbook.xml", "<workbook/>")
    return output.getvalue()


def _feed(
    *,
    relevant: bool = True,
    include_ppi: bool = True,
    declaration: bytes = b"",
) -> bytes:
    items: list[str] = []
    if relevant:
        items.append(
            f"""
        <item>
          <title>2026年7月份居民消费价格同比上涨0.5%</title>
          <link>{CPI_URL}</link>
          <pubTime>2026-08-09 09:30:02</pubTime>
          <pubDate>2026-08-09 09:30:02</pubDate>
          <description>居民消费价格原始发布摘要。</description>
        </item>
        """
        )
    if relevant and include_ppi:
        items.append(
            f"""
        <item>
          <title>2026年7月份工业生产者出厂价格同比上涨3.5%</title>
          <link>{PPI_URL}</link>
          <pubTime>2026-08-09 09:30:01</pubTime>
          <pubDate>2026-08-09 09:30:01</pubDate>
          <description>工业生产者价格原始发布摘要。</description>
        </item>
        """
        )
    return (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        + declaration
        + f"""
        <rss version="2.0"><channel>
          <title>数据发布</title>
          <link>https://www.stats.gov.cn/sj/zxfb/</link>
          {"".join(items)}
          <item>
            <title>更早的其他数据发布</title>
            <link>https://www.stats.gov.cn/sj/zxfb/202607/t20260731_1964253.html</link>
            <pubTime>2026-07-31 09:30:01</pubTime>
            <pubDate>2026-07-31 09:30:01</pubDate>
            <description>更早的非目标发布。</description>
          </item>
        </channel></rss>
        """.encode()
    )


def _article(title: str, xlsx_name: str) -> bytes:
    return f"""
    <!DOCTYPE html><html><head>
      <meta name="ArticleTitle" content="{title}">
      <meta name="PubDate" content="2026/08/09 09:30">
    </head><body>
      <h1>{title}</h1><span>2026/08/09 09:30</span>
      <a href="./{xlsx_name}">相关数据表</a>
    </body></html>
    """.encode()


def _responses(*, relevant: bool = True, cpi_attachment: bool = True):  # type: ignore[no-untyped-def]
    responses = {
        NBS_MACRO_RELEASE_FEED_URL: NbsMacroReleaseHTTPResponse(
            body=_feed(relevant=relevant),
            final_url=NBS_MACRO_RELEASE_FEED_URL,
            content_type="text/xml",
        ),
        NBS_MACRO_RELEASE_RIGHTS_URL: NbsMacroReleaseHTTPResponse(
            body=b"<html><body>NBS service terms</body></html>",
            final_url=NBS_MACRO_RELEASE_RIGHTS_URL,
            content_type="text/html",
        ),
    }
    if relevant:
        responses.update(
            {
                CPI_URL: NbsMacroReleaseHTTPResponse(
                    body=_article(
                        "2026年7月份居民消费价格同比上涨0.5%",
                        CPI_XLSX.rsplit("/", 1)[-1],
                    ),
                    final_url=CPI_URL,
                    content_type="text/html",
                ),
                PPI_URL: NbsMacroReleaseHTTPResponse(
                    body=_article(
                        "2026年7月份工业生产者出厂价格同比上涨3.5%",
                        PPI_XLSX.rsplit("/", 1)[-1],
                    ),
                    final_url=PPI_URL,
                    content_type="text/html",
                ),
                PPI_XLSX: NbsMacroReleaseHTTPResponse(
                    body=_xlsx(),
                    final_url=PPI_XLSX,
                    content_type=(
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ),
                ),
            }
        )
        if cpi_attachment:
            responses[CPI_XLSX] = NbsMacroReleaseHTTPResponse(
                body=_xlsx(),
                final_url=CPI_XLSX,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    return responses


def _query(provider: NbsMacroReleaseProvider) -> tuple[DataQuery, DataSourceBinding]:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )
    return (
        DataQuery.build(
            capability=ObservationCapability.MACRO_VINTAGE,
            pit_lane=DataPITLane.PROSPECTIVE,
            as_of=RETRIEVED,
            window_start=WINDOW_START,
            source_policy_id="nbs-macro-release-fixture",
            parameters={"indicators": ["cpi", "ppi"]},
            sources=(source,),
            minimum_data_sources=1,
        ),
        source,
    )


def test_nbs_source_config_is_content_identified_and_canonical() -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)

    assert config.source_config_id == config.expected_source_config_id
    assert config.indicators == ("cpi", "ppi")
    assert config.require_spreadsheet is True


def test_nbs_provider_captures_original_releases_and_replays_identically(
    tmp_path: Path,
) -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    client = FakeHTTPClient(_responses())
    provider = NbsMacroReleaseProvider((config,), http_client=client, clock=lambda: RETRIEVED)
    query, _source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=WINDOW_START, parameters=query.parameters))[
        0
    ]
    store = LocalDataSnapshotStore(tmp_path / "state")
    harness = DataInputHarness(store)
    harness.register(provider.replay((capture,)))

    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is True
    assert len(snapshot.observations) == 2
    cpi = snapshot.observations[0]
    assert cpi.normalized_payload["record_type"] == "original_release"
    assert cpi.normalized_payload["indicator"] == "cpi"
    assert cpi.normalized_payload["reference_period"] == "2026-07"
    assert cpi.times.occurred_at == cpi.times.published_at
    assert cpi.times.source_updated_at is None
    assert cpi.times.occurrence_basis is OccurrenceBasis.SOURCE_REPORTED
    assert cpi.times.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
    assert cpi.times.available_at == cpi.authority_at == cpi.times.retrieved_at == RETRIEVED
    assert cpi.source_ref == CPI_URL
    assert cpi.normalized_payload["revision_lineage"] == []

    raw_response_hash = snapshot.attempts[0].raw_response_hash
    assert raw_response_hash is not None
    bundle = store.artifacts.get(
        raw_response_hash,
        media_type="application/octet-stream",
    ).path.read_bytes()
    stored = load_nbs_macro_release_capture_bundle(
        bundle,
        config=config,
        retrieved_at=RETRIEVED,
    )
    replay_store = LocalDataSnapshotStore(tmp_path / "replay")
    replay_harness = DataInputHarness(replay_store)
    replay_harness.register(provider.replay((stored,)))
    replayed = asyncio.run(replay_harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert replayed == snapshot


def test_nbs_provider_returns_completed_no_data_with_the_feed_payload() -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=FakeHTTPClient(_responses(relevant=False)),
        clock=lambda: RETRIEVED,
    )
    query, source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=WINDOW_START, parameters=query.parameters))[
        0
    ]

    response = provider.response_from_capture(query=query, source=source, capture=capture)

    assert response.status is DataFetchStatus.NO_DATA
    assert response.raw_payload is not None
    assert _feed(relevant=False) in response.raw_payload
    assert response.observations == ()


def test_nbs_rss_description_does_not_change_authoritative_observation_identity() -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    original_responses = _responses()
    changed_responses = _responses()
    feed = changed_responses[NBS_MACRO_RELEASE_FEED_URL]
    changed_responses[NBS_MACRO_RELEASE_FEED_URL] = NbsMacroReleaseHTTPResponse(
        body=feed.body.replace(
            "居民消费价格原始发布摘要。".encode(),
            "仅 RSS 中发生变化的发现摘要。".encode(),
        ),
        final_url=feed.final_url,
        content_type=feed.content_type,
    )
    original_provider = NbsMacroReleaseProvider(
        (config,), http_client=FakeHTTPClient(original_responses), clock=lambda: RETRIEVED
    )
    changed_provider = NbsMacroReleaseProvider(
        (config,), http_client=FakeHTTPClient(changed_responses), clock=lambda: RETRIEVED
    )
    query, source = _query(original_provider)
    original_capture = asyncio.run(
        original_provider.collect(window_start=WINDOW_START, parameters=query.parameters)
    )[0]
    changed_capture = asyncio.run(
        changed_provider.collect(window_start=WINDOW_START, parameters=query.parameters)
    )[0]

    original = original_provider.response_from_capture(
        query=query, source=source, capture=original_capture
    )
    changed = changed_provider.response_from_capture(
        query=query, source=source, capture=changed_capture
    )

    assert original.raw_payload != changed.raw_payload
    assert original.observations == changed.observations
    assert all(item.normalized_payload["release_summary"] is None for item in original.observations)


def test_nbs_provider_treats_a_missing_required_spreadsheet_as_source_error() -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    responses = _responses()
    article = responses[CPI_URL]
    responses[CPI_URL] = NbsMacroReleaseHTTPResponse(
        body=article.body.replace(b'href="./P020260809325111279255.xlsx"', b'href="./table.csv"'),
        final_url=CPI_URL,
        content_type="text/html",
    )
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=FakeHTTPClient(responses),
        clock=lambda: RETRIEVED,
    )
    query, source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=WINDOW_START, parameters=query.parameters))[
        0
    ]

    response = provider.response_from_capture(query=query, source=source, capture=capture)

    assert response.status is DataFetchStatus.ERROR
    assert response.error_kind == "required_spreadsheet_missing"


def test_nbs_provider_rejects_feed_dtd_or_entity_declarations() -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    feed = NbsMacroReleaseHTTPResponse(
        body=_feed(declaration=b'<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'),
        final_url=NBS_MACRO_RELEASE_FEED_URL,
        content_type="text/xml",
    )
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=FakeHTTPClient({**_responses(), NBS_MACRO_RELEASE_FEED_URL: feed}),
        clock=lambda: RETRIEVED,
    )
    query, source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=WINDOW_START, parameters=query.parameters))[
        0
    ]

    response = provider.response_from_capture(query=query, source=source, capture=capture)

    assert response.status is DataFetchStatus.ERROR
    assert response.error_kind == "source_parse_error"


@pytest.mark.parametrize(
    "case",
    (
        "feed_redirect",
        "feed_mime",
        "article_title",
        "article_pubdate",
        "article_visible_time",
        "truncated_article",
        "invalid_xlsx",
    ),
)
def test_nbs_provider_rejects_identity_and_attachment_drift(case: str) -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    responses = _responses()
    if case == "feed_redirect":
        feed = responses[NBS_MACRO_RELEASE_FEED_URL]
        responses[NBS_MACRO_RELEASE_FEED_URL] = NbsMacroReleaseHTTPResponse(
            body=feed.body,
            final_url="https://www.stats.gov.cn/sj/zxfb/redirected.xml",
            content_type=feed.content_type,
        )
    elif case == "feed_mime":
        feed = responses[NBS_MACRO_RELEASE_FEED_URL]
        responses[NBS_MACRO_RELEASE_FEED_URL] = NbsMacroReleaseHTTPResponse(
            body=feed.body,
            final_url=feed.final_url,
            content_type="application/xml",
        )
    elif case == "article_title":
        article = responses[CPI_URL]
        title = 'content="2026年7月份居民消费价格同比上涨0.5%"'.encode()
        responses[CPI_URL] = NbsMacroReleaseHTTPResponse(
            body=article.body.replace(title, b'content="mismatched title"', 1),
            final_url=article.final_url,
            content_type=article.content_type,
        )
    elif case == "article_pubdate":
        article = responses[CPI_URL]
        responses[CPI_URL] = NbsMacroReleaseHTTPResponse(
            body=article.body.replace(b'content="2026/08/09 09:30"', b'content="bad"', 1),
            final_url=article.final_url,
            content_type=article.content_type,
        )
    elif case == "article_visible_time":
        article = responses[CPI_URL]
        responses[CPI_URL] = NbsMacroReleaseHTTPResponse(
            body=article.body.replace(
                b"<span>2026/08/09 09:30</span>",
                b"<span>time removed</span>",
                1,
            ),
            final_url=article.final_url,
            content_type=article.content_type,
        )
    elif case == "truncated_article":
        article = responses[CPI_URL]
        responses[CPI_URL] = NbsMacroReleaseHTTPResponse(
            body=article.body.replace(b"</body></html>", b""),
            final_url=article.final_url,
            content_type=article.content_type,
        )
    else:
        attachment = responses[CPI_XLSX]
        responses[CPI_XLSX] = NbsMacroReleaseHTTPResponse(
            body=b"not an xlsx",
            final_url=attachment.final_url,
            content_type=attachment.content_type,
        )
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=FakeHTTPClient(responses),
        clock=lambda: RETRIEVED,
    )
    query, source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=WINDOW_START, parameters=query.parameters))[
        0
    ]

    response = provider.response_from_capture(query=query, source=source, capture=capture)

    assert response.status is DataFetchStatus.ERROR
    assert response.error_kind in {
        "source_identity_mismatch",
        "source_parse_error",
        "spreadsheet_parse_error",
    }


@pytest.mark.parametrize("case", ("date_order", "window_coverage"))
def test_nbs_provider_rejects_unproven_feed_window(case: str) -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    responses = _responses()
    window_start = WINDOW_START
    if case == "date_order":
        feed = responses[NBS_MACRO_RELEASE_FEED_URL]
        responses[NBS_MACRO_RELEASE_FEED_URL] = NbsMacroReleaseHTTPResponse(
            body=feed.body.replace(b"2026-07-31", b"2026-08-10"),
            final_url=feed.final_url,
            content_type=feed.content_type,
        )
    else:
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=FakeHTTPClient(responses),
        clock=lambda: RETRIEVED,
    )
    query, source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=window_start, parameters=query.parameters))[
        0
    ]

    response = provider.response_from_capture(query=query, source=source, capture=capture)

    assert response.status is DataFetchStatus.ERROR
    assert response.error_kind == "source_parse_error"


@pytest.mark.parametrize("case", ("tamper", "trailing_bytes"))
def test_nbs_capture_bundle_rejects_tamper_and_trailing_bytes(case: str) -> None:
    config = load_nbs_macro_release_source(CONFIG_PATH)
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=FakeHTTPClient(_responses()),
        clock=lambda: RETRIEVED,
    )
    query, source = _query(provider)
    capture = asyncio.run(provider.collect(window_start=WINDOW_START, parameters=query.parameters))[
        0
    ]
    response = provider.response_from_capture(query=query, source=source, capture=capture)
    assert response.raw_payload is not None
    if case == "tamper":
        changed = bytearray(response.raw_payload)
        changed[-10] ^= 1
        payload = bytes(changed)
    else:
        payload = response.raw_payload + b"unexpected"

    with pytest.raises(NbsMacroReleaseParseError):
        load_nbs_macro_release_capture_bundle(
            payload,
            config=config,
            retrieved_at=RETRIEVED,
        )


def test_nbs_acceptance_journals_and_passes_all_seven_generic_gates(tmp_path: Path) -> None:
    result = accept_nbs_macro_release_source(
        source_config_path=CONFIG_PATH,
        window_start=WINDOW_START,
        indicators=("cpi", "ppi"),
        poll_interval_seconds=3600,
        maximum_gap_seconds=90000,
        state_root=tmp_path / "state",
        provider_timeout_seconds=5.0,
        http_client=FakeHTTPClient(_responses()),
        clock=lambda: RETRIEVED,
    )

    assert result["accepted"] is True
    assert result["observation_count"] == 2
    assert result["semantic_scope"] == NBS_MACRO_RELEASE_SEMANTIC_SCOPE
    assert result["revision_strategy"] == NBS_MACRO_RELEASE_REVISION_STRATEGY
    gates = cast(list[dict[str, object]], result["gates"])
    assert all(item["status"] == "pass" for item in gates)
    assert result["historical_pit_claim"] is False
    assert result["evidence_promoted"] is False
    assert result["execution_capability"] is False


def test_nbs_acceptance_rejects_a_cli_indicator_subset(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly match"):
        accept_nbs_macro_release_source(
            source_config_path=CONFIG_PATH,
            window_start=WINDOW_START,
            indicators=("cpi",),
            poll_interval_seconds=3600,
            maximum_gap_seconds=90000,
            state_root=tmp_path / "state",
            provider_timeout_seconds=5.0,
            http_client=FakeHTTPClient(_responses()),
            clock=lambda: RETRIEVED,
        )


def test_nbs_acceptance_requires_every_configured_indicator_observation(tmp_path: Path) -> None:
    responses = _responses()
    feed = responses[NBS_MACRO_RELEASE_FEED_URL]
    responses[NBS_MACRO_RELEASE_FEED_URL] = NbsMacroReleaseHTTPResponse(
        body=_feed(include_ppi=False),
        final_url=feed.final_url,
        content_type=feed.content_type,
    )

    with pytest.raises(RuntimeError, match="every configured indicator"):
        accept_nbs_macro_release_source(
            source_config_path=CONFIG_PATH,
            window_start=WINDOW_START,
            indicators=("cpi", "ppi"),
            poll_interval_seconds=3600,
            maximum_gap_seconds=90000,
            state_root=tmp_path / "state",
            provider_timeout_seconds=5.0,
            http_client=FakeHTTPClient(responses),
            clock=lambda: RETRIEVED,
        )


def test_nbs_scheduled_collector_runtime_binding_materializes_a_snapshot(
    tmp_path: Path,
) -> None:
    acceptance = accept_nbs_macro_release_source(
        source_config_path=CONFIG_PATH,
        window_start=WINDOW_START,
        indicators=("cpi", "ppi"),
        poll_interval_seconds=3600,
        maximum_gap_seconds=90000,
        state_root=tmp_path / "acceptance",
        provider_timeout_seconds=5.0,
        http_client=FakeHTTPClient(_responses()),
        clock=lambda: RETRIEVED,
    )
    config = load_nbs_macro_release_source(CONFIG_PATH)
    provider = NbsMacroReleaseProvider((config,))
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.MACRO_VINTAGE,
        sources=(source,),
        window_start=WINDOW_START,
        parameters={"indicators": ["cpi", "ppi"]},
        poll_interval_seconds=3600,
        maximum_gap_seconds=90000,
    )
    report = load_source_route_acceptance_report(
        Path(cast(str, acceptance["source_route_acceptance_report_path"]))
    )
    starts_at = RETRIEVED + timedelta(minutes=1)
    narrow_policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.MACRO_VINTAGE,
        sources=(source,),
        window_start=WINDOW_START,
        parameters={"indicators": ["cpi"]},
        poll_interval_seconds=3600,
        maximum_gap_seconds=90000,
    )
    with pytest.raises(ValueError, match="exactly match"):
        ProspectiveCollectionJob.build(
            adapter_kind=ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE,
            collection_policy=narrow_policy,
            source_acceptance_report=report,
            source_config=config.to_dict(),
            starts_at=starts_at,
            misfire_grace_seconds=30,
            maximum_jitter_seconds=0,
            provider_timeout_seconds=5.0,
        )
    job = ProspectiveCollectionJob.build(
        adapter_kind=ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE,
        collection_policy=policy,
        source_acceptance_report=report,
        source_config=config.to_dict(),
        starts_at=starts_at,
        misfire_grace_seconds=30,
        maximum_jitter_seconds=0,
        provider_timeout_seconds=5.0,
    )
    scheduled_store = LocalDataSnapshotStore(tmp_path / "scheduled")
    runtime = ProspectiveCollectionRuntime(
        scheduled_store,
        clock=lambda: starts_at,
    )
    runtime.register(
        job,
        collection_policy=policy,
        source_acceptance_report=report,
        source_config=config.to_dict(),
        registered_at=RETRIEVED,
    )

    result = runtime.run_due(
        job.job_id,
        now=starts_at,
        collector=lambda bound_policy, source_config: collect_prospective_source_snapshot(
            job=job,
            policy=bound_policy,
            source_config=source_config,
            store=scheduled_store,
            nbs_http_client=FakeHTTPClient(_responses()),
            clock=lambda: starts_at,
        ),
    )

    assert result.outcome == "success"
    assert result.data_snapshot_id is not None
    assert scheduled_store.get(result.data_snapshot_id).coverage_complete is True

    incomplete_responses = _responses()
    feed = incomplete_responses[NBS_MACRO_RELEASE_FEED_URL]
    incomplete_responses[NBS_MACRO_RELEASE_FEED_URL] = NbsMacroReleaseHTTPResponse(
        body=_feed(include_ppi=False),
        final_url=feed.final_url,
        content_type=feed.content_type,
    )
    next_due = starts_at + timedelta(seconds=policy.poll_interval_seconds)
    incomplete = runtime.run_due(
        job.job_id,
        now=next_due,
        collector=lambda bound_policy, source_config: collect_prospective_source_snapshot(
            job=job,
            policy=bound_policy,
            source_config=source_config,
            store=scheduled_store,
            nbs_http_client=FakeHTTPClient(incomplete_responses),
            clock=lambda: next_due,
        ),
    )

    assert incomplete.outcome == "source_failure"
    assert incomplete.error_kind == "source_parse_error"
    assert incomplete.data_snapshot_id is not None
    incomplete_snapshot = scheduled_store.get(incomplete.data_snapshot_id)
    assert incomplete_snapshot.coverage_complete is False
    assert incomplete_snapshot.observations == ()
    assert incomplete_snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert runtime.opportunities(job.job_id)[-1].outcome == "source_failure"
