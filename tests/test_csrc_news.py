from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlencode

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.csrc_news import (
    CsrcNewsCapture,
    CsrcNewsHTTPResponse,
    CsrcNewsPageCapture,
    CsrcNewsParseError,
    CsrcNewsProvider,
    CsrcNewsSourceConfig,
    load_csrc_news_capture_bundle,
    load_csrc_news_source,
)
from market_impact_agent.data_inputs import (
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSourceBinding,
    LocalDataSnapshotStore,
)
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability

RETRIEVED = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
WINDOW_START = datetime(2026, 8, 1, tzinfo=UTC)


def _result(*, title: str, content_id: str, published_at: str) -> dict[str, object]:
    return {
        "title": title,
        "url": f"//www.csrc.gov.cn/csrc/c100028/{content_id}/content.shtml",
        "publishedTimeStr": published_at,
        "memo": f"{title}摘要",
        "channelId": "official-news-channel",
        "contentHtml": f"<p>{title}正文</p>",
    }


def _page(
    page: int,
    results: list[dict[str, object]],
    *,
    total: int,
) -> bytes:
    return json.dumps(
        {
            "data": {
                "page": page,
                "rows": 2,
                "channelId": "official-news-channel",
                "total": total,
                "results": results,
            }
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


class FakeHTTPClient:
    def __init__(self, pages: dict[int, bytes]) -> None:
        self.pages = pages
        self.calls: list[str] = []

    def get(self, url: str, *, max_response_bytes: int) -> CsrcNewsHTTPResponse:
        self.calls.append(url)
        page = int(url.rsplit("page=", 1)[1])
        body = self.pages[page]
        assert len(body) <= max_response_bytes
        return CsrcNewsHTTPResponse(
            body=body,
            final_url=url,
            content_type="application/json",
        )


def _config() -> CsrcNewsSourceConfig:
    return CsrcNewsSourceConfig.build(
        source_id="csrc-official-news",
        endpoint_url=("https://www.csrc.gov.cn/searchList/official-news-channel"),
        channel_id="official-news-channel",
        publisher="中国证监会",
        published_timezone="Asia/Shanghai",
        page_size=2,
        maximum_pages=3,
        rights_basis_url=("https://www.csrc.gov.cn/csrc/c100227/c1362477/content.shtml"),
        rights_reviewed_at=datetime(2026, 8, 28, tzinfo=UTC),
        license_scope="official_public_private_research_no_redistribution",
    )


def _source(provider: CsrcNewsProvider) -> DataSourceBinding:
    config = _config()
    return DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )


def _complete_capture(*bodies: bytes) -> CsrcNewsCapture:
    config = _config()
    return CsrcNewsCapture(
        source_id=config.source_id,
        retrieved_at=RETRIEVED,
        pages=tuple(
            CsrcNewsPageCapture(
                page=page,
                request_url=_capture_url(config, page),
                response=CsrcNewsHTTPResponse(
                    body=body,
                    final_url=_capture_url(config, page),
                    content_type="application/json",
                ),
            )
            for page, body in enumerate(bodies, start=1)
        ),
        coverage_complete=True,
    )


def _capture_url(config: CsrcNewsSourceConfig, page: int) -> str:
    query = urlencode(
        (
            ("_isAgg", "true"),
            ("_isJson", "true"),
            ("_pageSize", str(config.page_size)),
            ("_template", "index"),
            ("_rangeTimeGte", ""),
            ("_channelName", ""),
            ("page", str(page)),
        )
    )
    return f"{config.endpoint_url}?{query}"


def _capture_bundle(capture: CsrcNewsCapture) -> bytes:
    parts = [b"market-impact.csrc-news-capture.v1\n"]
    for item in capture.pages:
        body = item.response.body
        header = json.dumps(
            {
                "page": item.page,
                "request_url": item.request_url,
                "final_url": item.response.final_url,
                "content_type": item.response.content_type,
                "body_size": len(body),
                "body_sha256": sha256(body).hexdigest(),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        parts.extend((header, b"\n", body, b"\n"))
    return b"".join(parts)


def _replay_capture(
    provider: CsrcNewsProvider,
    capture: CsrcNewsCapture,
    store_root: Path,
):
    store = LocalDataSnapshotStore(store_root)
    harness = DataInputHarness(store)
    harness.register(provider.replay((capture,)))
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=RETRIEVED,
        window_start=WINDOW_START,
        source_policy_id="csrc-official-news-prospective-v1",
        parameters={"keywords": [], "max_items": 20},
        sources=(_source(provider),),
        minimum_data_sources=1,
    )
    return asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))


def test_checked_in_csrc_source_is_canonical_and_schema_valid() -> None:
    path = Path("examples/providers/csrc-official-news-v1.json")

    config = load_csrc_news_source(path)

    assert config.source_id == "csrc-official-news"
    assert config.publisher == "中国证监会"
    assert config.redistribution_allowed is False
    assert config.rights_basis_url.endswith("/c1362477/content.shtml")


def test_csrc_provider_preserves_prospective_receipt_and_exact_record_bytes(
    tmp_path: Path,
) -> None:
    first = _result(
        title="创业板改革意见",
        content_id="c7625372",
        published_at="2026-08-20 16:12:19",
    )
    second = _result(
        title="市场监管通报",
        content_id="c7625370",
        published_at="2026-08-03 09:47:42",
    )
    client = FakeHTTPClient({1: _page(1, [first, second], total=2)})
    provider = CsrcNewsProvider((_config(),), http_client=client, clock=lambda: RETRIEVED)
    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]
    replay = provider.replay((capture,))
    store = LocalDataSnapshotStore(tmp_path / "state")
    harness = DataInputHarness(store)
    harness.register(replay)
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=capture.retrieved_at,
        window_start=WINDOW_START,
        source_policy_id="csrc-official-news-prospective-v1",
        parameters={"keywords": [], "max_items": 20},
        sources=(_source(provider),),
        minimum_data_sources=1,
    )

    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is True
    assert [item.normalized_payload["headline"] for item in snapshot.observations] == [
        "创业板改革意见",
        "市场监管通报",
    ]
    assert all(
        item.times.available_at == RETRIEVED
        and item.authority_at == RETRIEVED
        and item.times.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
        for item in snapshot.observations
    )
    raw = store.artifacts.get(
        snapshot.observations[0].raw_content_hash,
        media_type="application/octet-stream",
    ).path.read_bytes()
    assert json.loads(raw)["title"] == "创业板改革意见"
    assert raw in capture.pages[0].response.body
    assert snapshot.observations[0].normalized_payload["content_scope"] == (
        "official_publication_private_research"
    )
    assert snapshot.observations[0].license_scope.endswith("no_redistribution")
    raw_response_hash = snapshot.attempts[0].raw_response_hash
    assert raw_response_hash is not None
    bundle = store.artifacts.get(
        raw_response_hash,
        media_type="application/octet-stream",
    ).path.read_bytes()
    stored_capture = load_csrc_news_capture_bundle(
        bundle,
        config=_config(),
        retrieved_at=RETRIEVED,
    )
    replay_store = LocalDataSnapshotStore(tmp_path / "replay")
    replay_harness = DataInputHarness(replay_store)
    replay_harness.register(provider.replay((stored_capture,)))

    replayed = asyncio.run(replay_harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert replayed == snapshot


def test_csrc_provider_paginates_until_the_query_window_is_covered(tmp_path: Path) -> None:
    pages = {
        1: _page(
            1,
            [
                _result(
                    title="八月二十日政策",
                    content_id="c1",
                    published_at="2026-08-20 12:00:00",
                ),
                _result(
                    title="八月十日政策",
                    content_id="c2",
                    published_at="2026-08-10 12:00:00",
                ),
            ],
            total=4,
        ),
        2: _page(
            2,
            [
                _result(
                    title="七月公告",
                    content_id="c3",
                    published_at="2026-07-31 12:00:00",
                ),
                _result(
                    title="更早公告",
                    content_id="c4",
                    published_at="2026-07-20 12:00:00",
                ),
            ],
            total=4,
        ),
    }
    client = FakeHTTPClient(pages)
    provider = CsrcNewsProvider((_config(),), http_client=client, clock=lambda: RETRIEVED)

    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]

    assert len(client.calls) == 2
    assert capture.coverage_complete is True
    assert len(capture.pages) == 2


def test_csrc_provider_accepts_non_monotonic_times_within_one_publication_date() -> None:
    client = FakeHTTPClient(
        {
            1: _page(
                1,
                [
                    _result(
                        title="同日较早发布",
                        content_id="c1",
                        published_at="2026-08-20 15:17:01",
                    ),
                    _result(
                        title="同日较晚发布",
                        content_id="c2",
                        published_at="2026-08-20 15:37:46",
                    ),
                ],
                total=2,
            )
        }
    )
    provider = CsrcNewsProvider((_config(),), http_client=client, clock=lambda: RETRIEVED)

    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]

    assert capture.coverage_complete is True


def test_csrc_provider_fails_closed_when_pagination_total_changes() -> None:
    client = FakeHTTPClient(
        {
            1: _page(
                1,
                [
                    _result(title="一", content_id="c1", published_at="2026-08-20 12:00:00"),
                    _result(title="二", content_id="c2", published_at="2026-08-19 12:00:00"),
                ],
                total=100,
            ),
            2: _page(
                2,
                [
                    _result(title="三", content_id="c3", published_at="2026-08-18 12:00:00"),
                    _result(title="四", content_id="c4", published_at="2026-08-17 12:00:00"),
                ],
                total=4,
            ),
        }
    )
    provider = CsrcNewsProvider((_config(),), http_client=client, clock=lambda: RETRIEVED)

    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]

    assert capture.coverage_complete is False
    assert capture.error_kind == "source_parse_error"


def test_csrc_replay_rejects_inconsistent_persisted_page_totals(tmp_path: Path) -> None:
    provider = CsrcNewsProvider((_config(),), clock=lambda: RETRIEVED)
    capture = _complete_capture(
        _page(
            1,
            [
                _result(title="一", content_id="c1", published_at="2026-08-20 12:00:00"),
                _result(title="二", content_id="c2", published_at="2026-08-19 12:00:00"),
            ],
            total=4,
        ),
        _page(
            2,
            [
                _result(title="三", content_id="c3", published_at="2026-08-18 12:00:00"),
                _result(title="四", content_id="c4", published_at="2026-08-17 12:00:00"),
            ],
            total=3,
        ),
    )

    snapshot = _replay_capture(provider, capture, tmp_path / "state")

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].error_kind == "source_parse_error"


def test_csrc_capture_bundle_rejects_inconsistent_page_totals() -> None:
    capture = _complete_capture(
        _page(
            1,
            [
                _result(title="一", content_id="c1", published_at="2026-08-20 12:00:00"),
                _result(title="二", content_id="c2", published_at="2026-08-19 12:00:00"),
            ],
            total=4,
        ),
        _page(
            2,
            [
                _result(title="三", content_id="c3", published_at="2026-08-18 12:00:00"),
                _result(title="四", content_id="c4", published_at="2026-08-17 12:00:00"),
            ],
            total=3,
        ),
    )

    with pytest.raises(CsrcNewsParseError, match="pagination total changed"):
        load_csrc_news_capture_bundle(
            _capture_bundle(capture),
            config=_config(),
            retrieved_at=RETRIEVED,
        )


def test_csrc_capture_bundle_rejects_empty_intermediate_page_with_remaining_total() -> None:
    capture = _complete_capture(
        _page(
            1,
            [
                _result(title="一", content_id="c1", published_at="2026-08-20 12:00:00"),
                _result(title="二", content_id="c2", published_at="2026-08-19 12:00:00"),
            ],
            total=4,
        ),
        _page(2, [], total=4),
    )

    with pytest.raises(CsrcNewsParseError, match="empty page"):
        load_csrc_news_capture_bundle(
            _capture_bundle(capture),
            config=_config(),
            retrieved_at=RETRIEVED,
        )


def test_csrc_replay_rejects_empty_intermediate_page_with_remaining_coverage(
    tmp_path: Path,
) -> None:
    provider = CsrcNewsProvider((_config(),), clock=lambda: RETRIEVED)
    capture = _complete_capture(
        _page(
            1,
            [
                _result(title="一", content_id="c1", published_at="2026-08-20 12:00:00"),
                _result(title="二", content_id="c2", published_at="2026-08-19 12:00:00"),
            ],
            total=4,
        ),
        _page(2, [], total=4),
    )

    snapshot = _replay_capture(provider, capture, tmp_path / "state")

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].error_kind == "source_parse_error"


def test_csrc_replay_rejects_mixed_route_records_in_a_persisted_capture(
    tmp_path: Path,
) -> None:
    provider = CsrcNewsProvider((_config(),), clock=lambda: RETRIEVED)
    wrong_route = _result(
        title="错误频道",
        content_id="c1",
        published_at="2026-08-20 12:00:00",
    )
    wrong_route["channelId"] = "another-channel"
    capture = _complete_capture(_page(1, [wrong_route], total=1))

    snapshot = _replay_capture(provider, capture, tmp_path / "state")

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].error_kind == "source_identity_mismatch"


def test_csrc_provider_fails_closed_on_empty_page_before_positive_total_is_covered() -> None:
    client = FakeHTTPClient({1: _page(1, [], total=2)})
    provider = CsrcNewsProvider((_config(),), http_client=client, clock=lambda: RETRIEVED)

    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]

    assert capture.coverage_complete is False
    assert capture.error_kind == "source_parse_error"


def test_csrc_provider_rejects_a_result_from_another_channel() -> None:
    result = _result(
        title="错误频道",
        content_id="c1",
        published_at="2026-08-20 12:00:00",
    )
    result["channelId"] = "another-channel"
    client = FakeHTTPClient({1: _page(1, [result], total=1)})
    provider = CsrcNewsProvider((_config(),), http_client=client, clock=lambda: RETRIEVED)

    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]

    assert capture.coverage_complete is False
    assert capture.error_kind == "source_identity_mismatch"


def test_csrc_provider_fails_closed_when_pagination_limit_cannot_cover_window(
    tmp_path: Path,
) -> None:
    config = CsrcNewsSourceConfig.build(
        source_id="csrc-official-news",
        endpoint_url="https://www.csrc.gov.cn/searchList/official-news-channel",
        channel_id="official-news-channel",
        publisher="中国证监会",
        published_timezone="Asia/Shanghai",
        page_size=2,
        maximum_pages=1,
        rights_basis_url="https://www.csrc.gov.cn/csrc/c100227/c1362477/content.shtml",
        rights_reviewed_at=datetime(2026, 8, 28, tzinfo=UTC),
        license_scope="official_public_private_research_no_redistribution",
    )
    client = FakeHTTPClient(
        {
            1: _page(
                1,
                [
                    _result(
                        title="窗口内一",
                        content_id="c1",
                        published_at="2026-08-20 12:00:00",
                    ),
                    _result(
                        title="窗口内二",
                        content_id="c2",
                        published_at="2026-08-10 12:00:00",
                    ),
                ],
                total=10,
            )
        }
    )
    provider = CsrcNewsProvider((config,), http_client=client, clock=lambda: RETRIEVED)
    capture = asyncio.run(
        provider.collect(
            window_start=WINDOW_START,
            parameters={"keywords": [], "max_items": 20},
        )
    )[0]
    replay = provider.replay((capture,))
    store = LocalDataSnapshotStore(tmp_path / "state")
    harness = DataInputHarness(store)
    harness.register(replay)
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=RETRIEVED,
        window_start=WINDOW_START,
        source_policy_id="csrc-official-news-prospective-v1",
        parameters={"keywords": [], "max_items": 20},
        sources=(source,),
        minimum_data_sources=1,
    )

    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))

    assert capture.coverage_complete is False
    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].error_kind == "pagination_limit_exceeded"
