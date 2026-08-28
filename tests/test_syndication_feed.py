from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSourceBinding,
    LocalDataSnapshotStore,
)
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability
from market_impact_agent.syndication_feed import (
    SyndicationFeedProvider,
    SyndicationFeedSourceConfig,
    SyndicationHTTPResponse,
    load_syndication_feed_source,
)

RETRIEVED = datetime(2026, 8, 28, 6, 0, tzinfo=UTC)
AS_OF = datetime(2026, 8, 28, 6, 0, tzinfo=UTC)
WINDOW_START = datetime(2026, 8, 28, 4, 0, tzinfo=UTC)
FEED_URL = "https://official.example/feeds/releases.xml"
RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Official releases</title>
    <item>
      <title>Policy rate decision</title>
      <description>Committee lowered the policy rate.</description>
      <link>https://official.example/releases/rate-decision</link>
      <guid>release-rate-1</guid>
      <pubDate>Fri, 28 Aug 2026 05:30:00 GMT</pubDate>
    </item>
    <item>
      <title>Bank supervision notice</title>
      <description>Routine supervisory material.</description>
      <link>https://official.example/releases/supervision</link>
      <guid>release-supervision-1</guid>
      <pubDate>Fri, 28 Aug 2026 05:15:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

RSS_WITH_CONTENT_ENCODED = RSS.replace(
    b'<rss version="2.0">',
    b'<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">',
).replace(
    b"<description>Committee lowered the policy rate.</description>",
    b"<description>Committee lowered the policy rate.</description>"
    b"<content:encoded>Full licensed article body.</content:encoded>",
)

ATOM_WITH_CONTENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Official releases</title>
  <entry>
    <title>Policy rate decision</title>
    <link href="https://official.example/releases/rate-decision" />
    <id>release-rate-1</id>
    <updated>2026-08-28T05:30:00Z</updated>
    <content type="text">Full licensed article body.</content>
  </entry>
</feed>
"""


@dataclass(frozen=True)
class FixtureHTTPClient:
    response: SyndicationHTTPResponse

    def get(self, url: str, *, max_response_bytes: int) -> SyndicationHTTPResponse:
        assert url == FEED_URL
        assert max_response_bytes >= len(self.response.body)
        return self.response


@dataclass
class MappingHTTPClient:
    responses: dict[str, SyndicationHTTPResponse]
    calls: list[str] = field(default_factory=list[str])

    def get(self, url: str, *, max_response_bytes: int) -> SyndicationHTTPResponse:
        self.calls.append(url)
        response = self.responses[url]
        assert max_response_bytes >= len(response.body)
        return response


def _config() -> SyndicationFeedSourceConfig:
    return SyndicationFeedSourceConfig.build(
        source_id="official-releases",
        request_url=FEED_URL,
        expected_final_url=FEED_URL,
        publisher="Official Institution",
        license_scope="public_metadata_private_research",
    )


def _provider(
    *,
    body: bytes = RSS,
    final_url: str = FEED_URL,
    content_type: str = "application/rss+xml",
) -> SyndicationFeedProvider:
    return SyndicationFeedProvider(
        (_config(),),
        http_client=FixtureHTTPClient(
            SyndicationHTTPResponse(
                body=body,
                final_url=final_url,
                content_type=content_type,
            )
        ),
        clock=lambda: RETRIEVED,
    )


def _source(provider: SyndicationFeedProvider) -> DataSourceBinding:
    config = _config()
    return DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )


def _query(
    provider: SyndicationFeedProvider,
    *,
    parameters: dict[str, object] | None = None,
) -> DataQuery:
    return DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=AS_OF,
        window_start=WINDOW_START,
        source_policy_id="official-syndication-prospective-v1",
        parameters={"max_items": 20} if parameters is None else parameters,
        sources=(_source(provider),),
        minimum_data_sources=1,
    )


def test_source_config_is_content_identified_and_schema_valid(tmp_path: Path) -> None:
    config = _config()
    path = tmp_path / "source.json"
    path.write_text(
        json.dumps(config.to_dict(), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    loaded = load_syndication_feed_source(path)

    assert loaded == config
    assert (
        validate_agent_contract(
            loaded.to_dict(),
            "syndication-feed-source.schema.json",
        )
        == ()
    )


def test_provider_captures_prospective_receipts_and_exact_entry_bytes(tmp_path: Path) -> None:
    provider = _provider()
    replay_provider = provider.replay(asyncio.run(provider.collect()))
    store = LocalDataSnapshotStore(tmp_path / "data")
    harness = DataInputHarness(store)
    harness.register(replay_provider)

    snapshot = asyncio.run(harness.execute(_query(provider), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is True
    assert len(snapshot.observations) == 2
    assert snapshot.attempts[0].status is DataFetchStatus.DATA
    assert snapshot.observations[0].times.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
    assert snapshot.observations[0].times.available_at == RETRIEVED
    assert snapshot.observations[0].authority_at == RETRIEVED
    assert snapshot.observations[0].normalized_payload["headline"] == "Policy rate decision"
    assert snapshot.observations[0].normalized_payload["publisher"] == "Official Institution"
    raw = store.artifacts.get(
        snapshot.observations[0].raw_content_hash,
        media_type="application/octet-stream",
    ).path.read_bytes()
    assert raw.startswith(b"<item>")
    assert raw.endswith(b"</item>")
    assert b"Policy rate decision" in raw
    assert b"Bank supervision notice" not in raw
    source_config_artifact = store.artifacts.get(
        _config().artifact_hash,
        media_type="application/json",
    )
    assert source_config_artifact.size_bytes > 0


def test_collected_responses_freeze_at_last_actual_receipt_without_refetch(tmp_path: Path) -> None:
    first_config = _config()
    second_url = "https://second.example/feeds/releases.xml"
    second_config = SyndicationFeedSourceConfig.build(
        source_id="second-releases",
        request_url=second_url,
        expected_final_url=second_url,
        publisher="Second Institution",
        license_scope="public_metadata_private_research",
    )
    client = MappingHTTPClient(
        {
            FEED_URL: SyndicationHTTPResponse(
                body=RSS,
                final_url=FEED_URL,
                content_type="application/rss+xml",
            ),
            second_url: SyndicationHTTPResponse(
                body=RSS,
                final_url=second_url,
                content_type="application/rss+xml",
            ),
        }
    )
    receipt_times = iter(
        (
            datetime(2026, 8, 28, 6, 0, tzinfo=UTC),
            datetime(2026, 8, 28, 6, 1, tzinfo=UTC),
        )
    )
    provider = SyndicationFeedProvider(
        (first_config, second_config),
        http_client=client,
        clock=lambda: next(receipt_times),
    )

    captures = asyncio.run(provider.collect())
    replay_provider = provider.replay(captures)
    store = LocalDataSnapshotStore(tmp_path / "data")
    harness = DataInputHarness(store)
    harness.register(replay_provider)
    sources = tuple(
        DataSourceBinding(
            provider_id=replay_provider.manifest.provider_id,
            provider_version=replay_provider.manifest.provider_version,
            upstream_source=config.source_id,
            manifest_hash=canonical_hash(replay_provider.manifest.to_dict()),
            source_config_hash=config.artifact_hash,
            required=True,
        )
        for config in (first_config, second_config)
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=max(item.retrieved_at for item in captures),
        window_start=WINDOW_START,
        source_policy_id="official-syndication-prospective-v1",
        parameters={"max_items": 20},
        sources=sources,
        minimum_data_sources=2,
    )

    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert client.calls == [FEED_URL, second_url]
    assert snapshot.coverage_complete is True
    assert snapshot.query.as_of == captures[1].retrieved_at
    assert snapshot.completed_at == captures[1].retrieved_at
    assert snapshot.attempts[0].retrieved_at == captures[0].retrieved_at
    assert snapshot.observations[0].times.available_at == captures[0].retrieved_at
    assert all(
        item.times.available_at is not None and item.times.available_at <= snapshot.query.as_of
        for item in snapshot.observations
    )


def test_provider_applies_frozen_keyword_and_item_limit(tmp_path: Path) -> None:
    provider = _provider()
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(
        harness.execute(
            _query(provider, parameters={"keywords": ["rate"], "max_items": 1}),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )

    assert snapshot.coverage_complete is True
    assert [item.upstream_record_id for item in snapshot.observations] == ["release-rate-1"]


@pytest.mark.parametrize(
    "final_url",
    [
        "https://mirror.example/releases.xml",
        "http://official.example/feeds/releases.xml",
    ],
)
def test_provider_rejects_redirect_identity_drift(tmp_path: Path, final_url: str) -> None:
    provider = _provider(final_url=final_url)
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(harness.execute(_query(provider), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert snapshot.attempts[0].error_kind == "source_identity_mismatch"


def test_provider_rejects_malformed_or_unsafe_feed(tmp_path: Path) -> None:
    unsafe = b'<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY x "unsafe">]><rss></rss>'
    provider = _provider(body=unsafe)
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(harness.execute(_query(provider), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert snapshot.attempts[0].error_kind == "feed_parse_error"


@pytest.mark.parametrize("body", [b"", RSS_WITH_CONTENT_ENCODED, ATOM_WITH_CONTENT])
def test_provider_rejects_non_excerpt_feed_content_before_raw_retention(
    tmp_path: Path,
    body: bytes,
) -> None:
    provider = _provider(body=body)
    store = LocalDataSnapshotStore(tmp_path / "data")
    harness = DataInputHarness(store)
    harness.register(provider)

    snapshot = asyncio.run(harness.execute(_query(provider), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert snapshot.attempts[0].error_kind == "feed_parse_error"
    assert snapshot.attempts[0].raw_response_hash is None


def test_harness_rejects_source_config_hash_drift(tmp_path: Path) -> None:
    provider = _provider()
    source = _source(provider)
    drifted_source = DataSourceBinding(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        manifest_hash=source.manifest_hash,
        source_config_hash="0" * 64,
        required=True,
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=AS_OF,
        window_start=WINDOW_START,
        source_policy_id="official-syndication-prospective-v1",
        parameters={"max_items": 20},
        sources=(drifted_source,),
        minimum_data_sources=1,
    )
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.NOT_CONFIGURED
    assert snapshot.attempts[0].error_kind == "provider_source_config_mismatch"
