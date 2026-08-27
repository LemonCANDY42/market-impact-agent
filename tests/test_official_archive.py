from __future__ import annotations

import gzip
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from market_impact_agent.archive_authority import (
    CommonCrawlLocator,
    VerifiedArchiveRecord,
)
from market_impact_agent.official_archive import (
    extract_csrc_regime_evidence,
    extract_csrc_transcript_segment,
    extract_nbs_macro_vintage,
    extract_state_council_regime_evidence,
)
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
)


def _archive_record() -> VerifiedArchiveRecord:
    payload = """
    <html><head><title>页面标题_中国证券监督管理委员会</title></head>
    <body><h2>国新办举行新闻发布会</h2>
    <p class="fl">日期\N{FULLWIDTH COLON}2024-09-24 来源\N{FULLWIDTH COLON}证监会</p></body></html>
    """.encode()
    locator = CommonCrawlLocator(
        collection="CC-MAIN-2024-42",
        target_url="http://www.csrc.gov.cn/csrc/c106311/c7508374/content.shtml",
        timestamp="20241007171301",
        filename="crawl-data/CC-MAIN-2024-42/segments/a/warc/a.warc.gz",
        offset=1,
        length=100,
        digest="sha1:" + "A" * 32,
        http_status=200,
    )
    return VerifiedArchiveRecord(
        provider_id="common-crawl-warc",
        archive_id="common-crawl",
        adapter_version="1.0.0",
        locator=locator,
        warc_record_id="<urn:uuid:test>",
        captured_at=datetime(2024, 10, 7, 17, 13, 1, tzinfo=UTC),
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        target_url=locator.target_url,
        http_status=200,
        media_type="text/html",
        archive_member_sha256="1" * 64,
        warc_block_sha256="2" * 64,
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=locator.digest,
        block_digest=None,
        truncated_reason=None,
        payload=payload,
    )


def test_csrc_archive_extracts_source_date_with_conservative_day_end() -> None:
    record = extract_csrc_regime_evidence(
        _archive_record(),
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="csrc-2024-09-24-financial-support-briefing",
        lineage_id="csrc-c7508374",
    )

    assert record.title == "国新办举行新闻发布会"
    assert record.published_at == datetime(2024, 9, 24, 15, 59, 59, tzinfo=UTC)
    assert record.available_at == record.published_at
    assert record.availability_basis is RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED
    assert record.authority_kind is RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE
    assert record.authority_at == datetime(2024, 10, 7, 17, 13, 1, tzinfo=UTC)
    assert record.content_hash == sha256(_archive_record().payload).hexdigest()
    assert record.source_id == "csrc-official-archive"
    assert record.provider_id == "csrc-web-archive"


def test_csrc_archive_prefers_exact_pubdate_when_archived_page_exposes_it() -> None:
    archived = _archive_record()
    payload = archived.payload.replace(
        b"<title>",
        b'<meta name="PubDate" content="2024-09-24 20:16:31"/><title>',
    )
    record = extract_csrc_regime_evidence(
        replace(archived, payload=payload, payload_sha256=sha256(payload).hexdigest()),
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="csrc-2024-09-24-merger-reform",
        lineage_id="csrc-c7508366",
    )

    assert record.published_at == datetime(2024, 9, 24, 12, 16, 31, tzinfo=UTC)
    assert record.available_at == record.published_at


def test_csrc_transcript_segment_uses_source_reported_live_timestamp() -> None:
    archived = _archive_record()
    payload = """
    <html><head><title>政策发布会_中国证券监督管理委员会</title></head><body>
    <h2>国新办举行新闻发布会</h2>
    <p>日期:2024-09-24 来源:证监会</p>
    <p>2024-09-24 09:00:58</p><p>开场介绍。</p>
    <p><span>2024-09-24 09:</span><span>10:58</span></p>
    <p>潘功胜:</p><p>我宣布下述几项政策。</p>
    <p>创设新的货币政策工具, 支持股票市场稳定发展。</p>
    <p>2024-09-24 09:19:36</p><p>下一位发言。</p>
    </body></html>
    """.encode()
    archived = replace(archived, payload=payload, payload_sha256=sha256(payload).hexdigest())

    snapshot = extract_csrc_transcript_segment(
        archived,
        segment_started_at=datetime(2024, 9, 24, 1, 10, 58, tzinfo=UTC),
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="csrc-2024-09-24-policy-revelation",
        lineage_id="csrc-c7508374-091058",
    )

    assert snapshot.record.occurred_at == datetime(2024, 9, 24, 1, 10, 58, tzinfo=UTC)
    assert snapshot.record.published_at == snapshot.record.occurred_at
    assert snapshot.record.available_at == snapshot.record.occurred_at
    assert snapshot.record.source_id == "csrc-official-archive"
    assert snapshot.record.authority_kind is RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE
    assert "我宣布下述几项政策" in snapshot.transcript_excerpt
    assert "下一位发言" not in snapshot.transcript_excerpt
    assert snapshot.to_research_document()["content_hash"] == snapshot.record.content_hash


def test_csrc_transcript_segment_rejects_missing_or_mismatched_timestamp() -> None:
    with pytest.raises(ValueError, match="transcript segment timestamp"):
        extract_csrc_transcript_segment(
            _archive_record(),
            segment_started_at=datetime(2024, 9, 24, 1, 10, 58, tzinfo=UTC),
            case_keys=("case",),
            claim_id="claim",
            lineage_id="lineage",
        )


def test_csrc_archive_rejects_wrong_host_and_truncated_capture() -> None:
    wrong_host = replace(
        _archive_record(),
        target_url="https://example.test/content.shtml",
    )
    with pytest.raises(ValueError, match="CSRC host"):
        extract_csrc_regime_evidence(
            wrong_host,
            case_keys=("case",),
            claim_id="claim",
            lineage_id="lineage",
        )

    with pytest.raises(ValueError, match="accepted archive capture"):
        extract_csrc_regime_evidence(
            replace(_archive_record(), truncated_reason="length"),
            case_keys=("case",),
            claim_id="claim",
            lineage_id="lineage",
        )


def test_state_council_archive_extracts_exact_updated_time() -> None:
    payload = b"""
    <html><head>
    <title>China unveils fresh stimulus to boost high-quality economic development</title>
    </head><body><span>Updated: September 25, 2024 08:58</span></body></html>
    """
    locator = replace(
        _archive_record().locator,
        target_url=(
            "https://english.www.gov.cn/news/202409/25/content_WS66f3602ec6d0868f4e8eb3c0.html"
        ),
        timestamp="20241009222054",
        digest="sha1:" + "B" * 32,
    )
    archive_record = replace(
        _archive_record(),
        locator=locator,
        target_url=locator.target_url,
        captured_at=datetime(2024, 10, 9, 22, 20, 54, tzinfo=UTC),
        payload=payload,
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=locator.digest,
    )

    record = extract_state_council_regime_evidence(
        archive_record,
        case_keys=("cn-2024-post-rally-whipsaw",),
        claim_id="gov-cn-2024-09-25-stimulus-summary",
        lineage_id="gov-cn-WS66f3602ec6d0868f4e8eb3c0",
    )

    assert record.published_at == datetime(2024, 9, 25, 0, 58, tzinfo=UTC)
    assert record.source_updated_at == record.published_at
    assert record.available_at == record.published_at
    assert record.source_id == "state-council-official-archive"
    assert record.provider_id == "gov-cn-web-archive"
    assert record.publisher_id == "state-council"


def test_state_council_archive_accepts_legacy_updated_time_format() -> None:
    payload = b"""
    <html><head><title>China mobilizes medical supplies to Wuhan</title></head>
    <body><span>Updated:</span> Jan 25,2020 8:54 PM <p>Source text.</p></body></html>
    """
    locator = replace(
        _archive_record().locator,
        target_url=(
            "http://english.gov.cn/statecouncil/ministries/202001/25/"
            "content_WS5e2c3a9cc6d019625c603f24.html"
        ),
        timestamp="20200128060802",
        digest="sha1:" + "D" * 32,
    )
    archive_record = replace(
        _archive_record(),
        locator=locator,
        target_url=locator.target_url,
        captured_at=datetime(2020, 1, 28, 6, 8, 2, tzinfo=UTC),
        payload=payload,
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=locator.digest,
    )

    record = extract_state_council_regime_evidence(
        archive_record,
        case_keys=("cn-2020-covid-closure-shock",),
        claim_id="gov-cn-2020-01-25-medical-supplies",
        lineage_id="gov-cn-WS5e2c3a9cc6d019625c603f24",
    )

    assert record.published_at == datetime(2020, 1, 25, 12, 54, tzinfo=UTC)
    assert record.source_updated_at == record.published_at


def test_state_council_archive_accepts_visible_chinese_publication_time() -> None:
    payload = """
    <html><head><title>中央经济工作会议在北京举行_滚动新闻_中国政府网</title></head>
    <body><span>2017-12-20 18:24 来源\N{FULLWIDTH COLON} 新华社</span>
    <p>会议正文。</p></body></html>
    """.encode()
    locator = replace(
        _archive_record().locator,
        target_url="http://www.gov.cn/xinwen/2017-12/20/content_5248899.htm",
        timestamp="20171222010430",
        digest="sha1:" + "E" * 32,
    )
    archive_record = replace(
        _archive_record(),
        locator=locator,
        target_url=locator.target_url,
        captured_at=datetime(2017, 12, 22, 1, 4, 30, tzinfo=UTC),
        payload=payload,
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=locator.digest,
    )

    record = extract_state_council_regime_evidence(
        archive_record,
        case_keys=("cn-2018-bear-market",),
        claim_id="gov-cn-2017-central-economic-work-conference",
        lineage_id="gov-cn-content-5248899",
    )

    assert record.published_at == datetime(2017, 12, 20, 10, 24, tzinfo=UTC)
    assert record.available_at == record.published_at


def test_nbs_archive_extracts_exact_macro_release_vintage() -> None:
    payload = """
    <html><head>
    <title>8月份国民经济运行总体平稳 - 国家统计局</title>
    <meta name="PubDate" content="2024/09/14 10:00">
    </head><body><time>2024/09/14 10:00</time></body></html>
    """.encode()
    locator = replace(
        _archive_record().locator,
        target_url="https://www.stats.gov.cn/sj/zxfb/202409/t20240914_1956487.html",
        timestamp="20240917114915",
        digest="sha1:" + "C" * 32,
    )
    archive_record = replace(
        _archive_record(),
        locator=locator,
        target_url=locator.target_url,
        captured_at=datetime(2024, 9, 17, 11, 49, 15, tzinfo=UTC),
        payload=payload,
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=locator.digest,
    )

    record = extract_nbs_macro_vintage(
        archive_record,
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="nbs-2024-08-national-economy",
        lineage_id="nbs-t20240914-1956487",
    )

    assert record.category == "macro_vintage"
    assert record.published_at == datetime(2024, 9, 14, 2, tzinfo=UTC)
    assert record.available_at == record.published_at
    assert record.source_id == "nbs-macro-vintage"
    assert record.provider_id == "nbs-release-archive"
    assert record.publisher_id == "nbs"


@pytest.mark.parametrize("compress", [False, True])
def test_nbs_archive_accepts_visible_legacy_publication_time(compress: bool) -> None:
    html = """
    <html><head><title>2017年12月份居民消费价格同比上涨1.8%</title></head>
    <body>来源\N{FULLWIDTH COLON}国家统计局 发布时间\N{FULLWIDTH COLON}2018-01-10 09:10</body>
    </html>
    """.encode()
    payload = gzip.compress(html, mtime=0) if compress else html
    locator = replace(
        _archive_record().locator,
        target_url="http://www.stats.gov.cn/tjsj/zxfb/201801/t20180110_1571525.html",
        timestamp="20180110090148",
        digest="sha1:" + "F" * 32,
    )
    archive_record = replace(
        _archive_record(),
        locator=locator,
        target_url=locator.target_url,
        captured_at=datetime(2018, 1, 10, 9, 1, 48, tzinfo=UTC),
        payload=payload,
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=locator.digest,
    )

    record = extract_nbs_macro_vintage(
        archive_record,
        case_keys=("cn-2018-bear-market",),
        claim_id="nbs-2017-12-cpi",
        lineage_id="nbs-t20180110-1571525",
    )

    assert record.published_at == datetime(2018, 1, 10, 1, 10, tzinfo=UTC)
