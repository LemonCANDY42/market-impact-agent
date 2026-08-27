from __future__ import annotations

import base64
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha1

import pytest

from market_impact_agent.internet_archive import (
    INTERNET_ARCHIVE_REPLAY_ENDPOINT,
    InternetArchiveAdapter,
    InternetArchiveIndexAdapter,
    InternetArchiveLocator,
    ReplayResponse,
    internet_archive_locator_from_dict,
)

NOW = datetime(2026, 8, 27, 15, tzinfo=UTC)
PAYLOAD = b"<html><head><title>Archived source</title></head></html>"


def digest(payload: bytes) -> str:
    encoded = base64.b32encode(sha1(payload, usedforsecurity=False).digest()).decode("ascii")
    return f"sha1:{encoded.rstrip('=')}"


def locator() -> InternetArchiveLocator:
    return InternetArchiveLocator(
        target_url="http://www.csrc.gov.cn/csrc/c100028/c7508366/content.shtml",
        timestamp="20240924142738",
        digest=digest(PAYLOAD),
        http_status=200,
        media_type="text/html",
    )


class FakeReplayTransport:
    def __init__(self, response: ReplayResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, timeout_seconds: float) -> ReplayResponse:
        self.calls.append((url, timeout_seconds))
        return self.response


def test_internet_archive_locator_round_trips_and_rejects_self_asserted_identity() -> None:
    item = locator()

    assert internet_archive_locator_from_dict(item.to_dict()) == item

    changed = item.to_dict()
    changed["source_version_id"] = "internet-archive-record-" + "0" * 64
    with pytest.raises(ValueError, match="does not match content"):
        internet_archive_locator_from_dict(changed)


def test_internet_archive_index_selects_latest_exact_capture_before_cutoff() -> None:
    calls: list[tuple[str, str, float]] = []

    def transport(endpoint: str, target_url: str, timeout_seconds: float) -> str:
        calls.append((endpoint, target_url, timeout_seconds))
        return (
            '[["timestamp","original","statuscode","digest","mimetype"],'
            '["20240924142738","http://www.csrc.gov.cn/csrc/c100028/c7508366/'
            'content.shtml","200","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","text/html"],'
            '["20241009142738","http://www.csrc.gov.cn/csrc/c100028/c7508366/'
            'content.shtml","200","BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB","text/html"]]'
        )

    item = InternetArchiveIndexAdapter(transport=transport).locate_latest(
        target_url="https://www.csrc.gov.cn/csrc/c100028/c7508366/content.shtml",
        not_after=datetime(2024, 10, 8, 1, 25, tzinfo=UTC),
    )

    assert item is not None
    assert item.timestamp == "20240924142738"
    assert item.target_url.startswith("http://www.csrc.gov.cn/")
    assert item.digest == "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    assert calls == [
        (
            "https://web.archive.org/cdx/search/cdx",
            "https://www.csrc.gov.cn/csrc/c100028/c7508366/content.shtml",
            30.0,
        )
    ]


def test_internet_archive_index_accepts_equivalent_explicit_default_port() -> None:
    item = InternetArchiveIndexAdapter(
        transport=lambda endpoint, target_url, timeout_seconds: (
            '[["timestamp","original","statuscode","digest","mimetype"],'
            '["20200125184255","http://english.www.gov.cn:80/source",'
            '"200","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","text/html"]]'
        )
    ).locate_latest(
        target_url="https://english.www.gov.cn/source",
        not_after=datetime(2020, 2, 3, tzinfo=UTC),
    )

    assert item is not None
    assert item.target_url == "http://english.www.gov.cn:80/source"


def test_internet_archive_index_ignores_different_target_after_cutoff() -> None:
    item = InternetArchiveIndexAdapter(
        transport=lambda endpoint, target_url, timeout_seconds: (
            '[["timestamp","original","statuscode","digest","mimetype"],'
            '["20171222010430","http://www.gov.cn/source","200",'
            '"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","text/html"],'
            '["20230513185311","http://www.gov.cn//source","200",'
            '"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB","text/html"]]'
        )
    ).locate_latest(
        target_url="https://www.gov.cn/source",
        not_after=datetime(2018, 1, 25, tzinfo=UTC),
    )

    assert item is not None
    assert item.timestamp == "20171222010430"


def test_internet_archive_replay_validates_exact_body_digest() -> None:
    item = locator()
    transport = FakeReplayTransport(
        ReplayResponse(
            status=200,
            headers={"content-type": "text/html; charset=UTF-8"},
            body=PAYLOAD,
        )
    )

    record = InternetArchiveAdapter(transport=transport, clock=lambda: NOW).fetch(item)

    assert record.payload == PAYLOAD
    assert record.payload_digest == item.digest
    assert record.captured_at == datetime(2024, 9, 24, 14, 27, 38, tzinfo=UTC)
    assert record.archive_capture_accepted is True
    assert record.locator.source_version_id.startswith("internet-archive-record-")
    assert "payload" not in record.to_dict()
    assert record.to_dict()["payload_retained_in_report"] is False
    assert transport.calls == [
        (
            f"{INTERNET_ARCHIVE_REPLAY_ENDPOINT}/{item.timestamp}id_/{item.target_url}",
            30.0,
        )
    ]


def test_internet_archive_replay_fails_closed_on_status_digest_and_origin() -> None:
    item = locator()
    with pytest.raises(ValueError, match="HTTP 200"):
        InternetArchiveAdapter(
            transport=FakeReplayTransport(ReplayResponse(status=302, headers={}, body=PAYLOAD)),
            clock=lambda: NOW,
        ).fetch(item)

    with pytest.raises(ValueError, match="digest"):
        InternetArchiveAdapter(
            transport=FakeReplayTransport(
                ReplayResponse(status=200, headers={"content-type": "text/html"}, body=b"changed")
            ),
            clock=lambda: NOW,
        ).fetch(item)

    with pytest.raises(ValueError, match="official HTTPS origin"):
        InternetArchiveAdapter(replay_endpoint="https://example.test/web")

    with pytest.raises(ValueError, match="different target"):
        InternetArchiveIndexAdapter(
            transport=lambda endpoint, target_url, timeout_seconds: (
                '[["timestamp","original","statuscode","digest","mimetype"],'
                '["20240924142738","https://other.test/source","200",'
                '"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","text/html"]]'
            )
        ).locate_latest(
            target_url="https://example.test/source",
            not_after=datetime(2024, 10, 8, tzinfo=UTC),
        )


def test_internet_archive_locator_rejects_invalid_fields() -> None:
    with pytest.raises(ValueError, match="HTTP status"):
        replace(locator(), http_status=302)
    with pytest.raises(ValueError, match="media type"):
        replace(locator(), media_type="application/pdf")
    with pytest.raises(ValueError, match="timezone-aware"):
        InternetArchiveIndexAdapter(
            transport=lambda endpoint, target_url, timeout_seconds: "[]"
        ).locate_latest(
            target_url="https://example.test/source",
            not_after=datetime(2024, 10, 8),
        )
