from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest

from market_impact_agent.publisher_evidence import (
    extract_publisher_news_evidence,
    extract_publisher_news_snapshot,
)
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
)


def test_xinhua_news_binds_exact_visible_publication_time_and_body_digest() -> None:
    payload = b"""
    <html><head>
      <meta name="publishdate" content="2024-09-23">
      <meta property="og:title" content="Chinese shares close higher Monday">
    </head><body><span>2024-09-23 16:06:15</span><p>Market report body.</p></body></html>
    """
    retrieved_at = datetime(2026, 8, 27, 4, tzinfo=UTC)

    record = extract_publisher_news_evidence(
        url="https://english.news.cn/20240923/example/c.html",
        payload=payload,
        retrieved_at=retrieved_at,
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="xinhua-2024-09-23-close",
        lineage_id="xinhua-example",
    )

    assert record.publisher_id == "xinhua"
    assert record.source_id == "xinhua-established-news"
    assert record.provider_id == "publisher-https-snapshot"
    assert record.title == "Chinese shares close higher Monday"
    assert record.published_at == datetime(2024, 9, 23, 8, 6, 15, tzinfo=UTC)
    assert record.source_updated_at is None
    assert record.available_at == datetime(2024, 9, 23, 8, 11, 15, tzinfo=UTC)
    assert record.availability_basis is RegimeEvidenceAvailabilityBasis.MODELED_LATENCY
    assert record.authority_kind is RegimeEvidenceAuthorityKind.PROVIDER_VERSION
    assert record.authority_at == retrieved_at
    assert record.content_hash == sha256(payload).hexdigest()
    assert record.license_scope == "private_licensed"


def test_scmp_news_uses_published_and_modified_metadata_for_current_version() -> None:
    payload = b"""
    <html><head>
      <meta property="og:title" content="Chinese stocks edge up slightly">
      <meta property="article:published_time" content="2024-09-18T10:29:06+08:00">
      <meta property="article:modified_time" content="2024-09-18T15:18:50+08:00">
    </head><body><p>Current article body.</p></body></html>
    """

    record = extract_publisher_news_evidence(
        url="https://www.scmp.com/business/china-business/article/3278947/example",
        payload=payload,
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="scmp-2024-09-18-stocks",
        lineage_id="scmp-3278947",
    )

    assert record.publisher_id == "scmp"
    assert record.source_id == "scmp-established-news"
    assert record.published_at == datetime(2024, 9, 18, 2, 29, 6, tzinfo=UTC)
    assert record.source_updated_at == datetime(2024, 9, 18, 7, 18, 50, tzinfo=UTC)
    assert record.available_at == datetime(2024, 9, 18, 7, 23, 50, tzinfo=UTC)


def test_publisher_snapshot_exposes_bounded_article_context_with_the_same_digest() -> None:
    payload = b"""
    <html><head>
      <meta name="publishdate" content="2024-09-23">
      <meta property="og:title" content="Chinese shares close higher Monday">
      <meta name="description" content="A concise publisher summary.">
    </head><body>
      <span>2024-09-23 16:06:15</span>
      <p>Source: Xinhua</p><p>First material paragraph.</p>
      <p>Second material paragraph with market context.</p>
    </body></html>
    """

    snapshot = extract_publisher_news_snapshot(
        url="https://english.news.cn/20240923/example/c.html",
        payload=payload,
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        case_keys=("cn-2024-policy-melt-up",),
        claim_id="xinhua-2024-09-23-close",
        lineage_id="xinhua-example",
    )

    assert snapshot.record.content_hash == sha256(payload).hexdigest()
    assert snapshot.description == "A concise publisher summary."
    assert snapshot.paragraphs == (
        "Source: Xinhua",
        "First material paragraph.",
        "Second material paragraph with market context.",
    )
    assert snapshot.to_research_document(max_characters=100) == {
        "title": "Chinese shares close higher Monday",
        "description": "A concise publisher summary.",
        "published_at": "2024-09-23T08:06:15Z",
        "source_updated_at": None,
        "publisher_id": "xinhua",
        "source_ref": "https://english.news.cn/20240923/example/c.html",
        "article_excerpt": (
            "Source: Xinhua First material paragraph. "
            "Second material paragraph with market context."
        ),
        "content_hash": sha256(payload).hexdigest(),
    }


def test_publisher_news_rejects_unknown_host_missing_exact_time_and_empty_body() -> None:
    with pytest.raises(ValueError, match="registered publisher host"):
        extract_publisher_news_evidence(
            url="https://example.test/article",
            payload=b"<html><title>Article</title><time>2024-09-23 16:00:00</time></html>",
            retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
            case_keys=("case",),
            claim_id="claim",
            lineage_id="lineage",
        )

    with pytest.raises(ValueError, match="exact publication time"):
        extract_publisher_news_evidence(
            url="https://english.news.cn/20240923/example/c.html",
            payload=b"<html><meta property='og:title' content='Article'></html>",
            retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
            case_keys=("case",),
            claim_id="claim",
            lineage_id="lineage",
        )

    with pytest.raises(ValueError, match="non-empty HTML"):
        extract_publisher_news_evidence(
            url="https://english.news.cn/20240923/example/c.html",
            payload=b"",
            retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
            case_keys=("case",),
            claim_id="claim",
            lineage_id="lineage",
        )
