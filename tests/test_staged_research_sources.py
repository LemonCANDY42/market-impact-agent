# ruff: noqa: RUF001

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.csrc_news import CsrcNewsHTTPResponse
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.research_thesis_runtime import research_evidence_metadata
from market_impact_agent.staged_research_sources import (
    capture_csrc_study_source,
    reopen_csrc_study_source,
)

URL = "https://www.csrc.gov.cn/csrc/c100028/c7461718/content.shtml"
WINDOW_ID = "2024-broad-rebound"
CAPTURED_AT = datetime(2026, 9, 6, 2, 0, tzinfo=UTC)
CUTOFF = datetime(2024, 2, 6, 2, 0, tzinfo=UTC)


class FakeHTTPClient:
    def __init__(self, body: bytes, *, final_url: str = URL) -> None:
        self.body = body
        self.final_url = final_url
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, *, max_response_bytes: int) -> CsrcNewsHTTPResponse:
        self.calls.append((url, max_response_bytes))
        return CsrcNewsHTTPResponse(
            body=self.body,
            final_url=self.final_url,
            content_type="text/html",
        )


def _page(
    *,
    title: str = "关于加强股票质押监管的通知",
    displayed_date: str | None = "2024-02-05",
    pubdate: str | None = "2024-02-05 19:30:00",
    others: str | None = None,
    paragraphs: tuple[str, ...] = (
        "【字号： 大 中 小 】",
        "为维护市场稳定，监管机构进一步完善股票质押风险管理安排。",
    ),
) -> bytes:
    meta = "" if pubdate is None else f'<meta name="PubDate" content="{pubdate}">'
    other_meta = "" if others is None else f'<meta name="others" content="{others}">'
    visible_date = "" if displayed_date is None else f"<p>日期：{displayed_date}</p>"
    body = "".join(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    return f"""
    <html><head><title>{title}_中国证券监督管理委员会</title>{meta}{other_meta}</head>
    <body><h2>{title}</h2>{visible_date}{body}
      <p>网站地图</p><p>版权所有：中国证券监督管理委员会</p>
    </body></html>
    """.encode()


def _capture(
    store: LocalDataSnapshotStore,
    client: FakeHTTPClient,
    *,
    cutoff: datetime = CUTOFF,
) -> str:
    return capture_csrc_study_source(
        URL,
        WINDOW_ID,
        cutoff,
        store,
        http_client=client,
        clock=lambda: CAPTURED_AT,
    )


def test_capture_reopens_current_csrc_page_as_explicitly_modeled_historical_evidence(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "source-store")
    client = FakeHTTPClient(_page())

    source_hash = _capture(store, client)
    reopened = reopen_csrc_study_source(store, source_hash, CUTOFF)

    assert len(client.calls) == 1
    assert client.calls[0][0] == URL
    assert client.calls[0][1] > len(client.body)
    assert reopened.window_id == WINDOW_ID
    assert reopened.reference.source_ref == URL
    assert reopened.reference.content_hash == canonical_hash(reopened.document)
    assert reopened.reference.available_at == datetime(2024, 2, 5, 11, 30, tzinfo=UTC)
    source_record_payload = store.artifacts.read_json(source_hash)
    assert isinstance(source_record_payload, dict)
    source_record = cast(dict[str, object], source_record_payload)
    raw_content_hash = source_record["raw_content_hash"]
    assert isinstance(raw_content_hash, str)
    assert source_record["document_hash"] == reopened.reference.content_hash
    records_payload = reopened.document["records"]
    assert isinstance(records_payload, list)
    records = cast(list[dict[str, object]], records_payload)
    assert len(records) == 1
    captured_record = records[0]
    assert reopened.document["records"] == [
        {
            "source_ref": URL,
            "content_hash": raw_content_hash,
            "evidence_record_id": captured_record["evidence_record_id"],
            "published_at": "2024-02-05T11:30:00Z",
            "available_at": "2024-02-05T11:30:00Z",
            "payload_status": "captured_current_official_page",
            "title": "关于加强股票质押监管的通知",
            "article_excerpt": "为维护市场稳定，监管机构进一步完善股票质押风险管理安排。",
            "retrieved_at": "2026-09-06T02:00:00Z",
            "historical_authentication": "modeled_pit_current_page_not_strict_historical_receipt",
            "availability_basis": "source_reported_exact_pubdate",
            "paragraph_count": 1,
        }
    ]
    metadata = research_evidence_metadata(reopened.reference, reopened.document, CUTOFF)
    assert metadata["category"] == "dated_publication"
    publication_records_payload = metadata["publication_records"]
    assert isinstance(publication_records_payload, list)
    publication_records = cast(list[dict[str, object]], publication_records_payload)
    assert len(publication_records) == 1
    assert publication_records[0]["evidence_record_id"] == captured_record["evidence_record_id"]
    assert publication_records[0]["published_at"] == captured_record["published_at"]
    assert publication_records[0]["modeled_published_at"] is None

    # Reopening only reads the stored CAS objects; it never fetches a current page again.
    replay = reopen_csrc_study_source(store, source_hash, CUTOFF)
    assert replay == reopened
    assert len(client.calls) == 1
    with pytest.raises(ValueError, match="cutoff differs"):
        reopen_csrc_study_source(store, source_hash, CUTOFF.replace(second=1))


def test_date_only_source_waits_until_eod_plus_five_minutes(tmp_path: Path) -> None:
    body = _page(pubdate=None)
    edge = datetime(2024, 2, 5, 16, 4, 59, tzinfo=UTC)

    with pytest.raises(ValueError, match="not available"):
        _capture(
            LocalDataSnapshotStore(tmp_path / "too-early"),
            FakeHTTPClient(body),
            cutoff=edge.replace(second=58),
        )

    store = LocalDataSnapshotStore(tmp_path / "at-edge")
    source_hash = _capture(store, FakeHTTPClient(body), cutoff=edge)
    reopened = reopen_csrc_study_source(store, source_hash, edge)
    records_payload = reopened.document["records"]
    assert isinstance(records_payload, list)
    records = cast(list[dict[str, object]], records_payload)
    assert len(records) == 1
    record = records[0]
    assert record["published_at"] == "2024-02-05T15:59:59Z"
    assert record["available_at"] == "2024-02-05T16:04:59Z"
    assert record["availability_basis"] == "modeled_source_date_eod_plus_5m"
    metadata = research_evidence_metadata(reopened.reference, reopened.document, edge)
    publication_record = cast(list[dict[str, object]], metadata["publication_records"])[0]
    assert publication_record["published_at"] is None
    assert publication_record["modeled_published_at"] == "2024-02-05T15:59:59Z"
    assert publication_record["effective_at"] is None
    assert publication_record["retrieved_at"] == "2026-09-06T02:00:00Z"
    assert publication_record["availability_basis"] == "modeled_source_date_eod_plus_5m"
    assert metadata["publication_age_seconds"] is None


def test_page_generation_time_matching_pubdate_uses_conservative_displayed_date(
    tmp_path: Path,
) -> None:
    generated = "2026-08-01 21:56:29"
    body = _page(pubdate=generated, others=f"页面生成时间 {generated}")
    cutoff = datetime(2024, 2, 5, 16, 4, 59, tzinfo=UTC)
    store = LocalDataSnapshotStore(tmp_path / "source-store")

    source_hash = _capture(store, FakeHTTPClient(body), cutoff=cutoff)
    reopened = reopen_csrc_study_source(store, source_hash, cutoff)

    records_payload = reopened.document["records"]
    assert isinstance(records_payload, list)
    records = cast(list[dict[str, object]], records_payload)
    assert len(records) == 1
    record = records[0]
    assert record["published_at"] == "2024-02-05T15:59:59Z"
    assert record["available_at"] == "2024-02-05T16:04:59Z"
    assert record["source_page_generated_at"] == "2026-08-01T13:56:29Z"
    assert record["availability_basis"] == "modeled_source_date_eod_plus_5m"


def test_capture_refuses_redirected_official_url(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="redirect changes the exact requested URL"):
        _capture(
            LocalDataSnapshotStore(tmp_path / "source-store"),
            FakeHTTPClient(
                _page(),
                final_url="https://www.csrc.gov.cn/csrc/c100028/c7461992/content.shtml",
            ),
        )


@pytest.mark.parametrize(
    ("body", "error"),
    [
        (_page(pubdate="2024-02-06 09:00:00"), "PubDate and displayed date disagree"),
        (
            _page(
                displayed_date="2024-02-07",
                pubdate="2026-08-01 21:56:29",
                others="页面生成时间 2026-08-01 21:56:29",
            ),
            "not available",
        ),
        (
            _page(
                pubdate="2026-08-01 21:56:29",
                others="页面生成时间 2026-08-01 21:56:28",
            ),
            "PubDate and displayed date disagree",
        ),
        (_page(title=""), "does not expose a title"),
        (_page(displayed_date=None), "displayed publication date"),
        (_page(paragraphs=("当前位置：首页 > 新闻",)), "meaningful body paragraphs"),
    ],
)
def test_capture_refuses_mismatched_future_or_incomplete_source_pages(
    tmp_path: Path,
    body: bytes,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        _capture(LocalDataSnapshotStore(tmp_path / error), FakeHTTPClient(body))


def test_reopen_refuses_raw_cas_tampering(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "source-store")
    source_hash = _capture(store, FakeHTTPClient(_page()))
    source_record_payload = store.artifacts.read_json(source_hash)
    assert isinstance(source_record_payload, dict)
    source_record = cast(dict[str, object], source_record_payload)
    raw_hash = source_record["raw_content_hash"]
    assert isinstance(raw_hash, str)
    (store.artifacts.root / raw_hash).write_bytes(b"tampered raw HTML")

    with pytest.raises(ValueError, match="artifact content does not match its identity"):
        reopen_csrc_study_source(store, source_hash, CUTOFF)
