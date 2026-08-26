from __future__ import annotations

import base64
import gzip
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha1

import pytest

from market_impact_agent.archive_authority import (
    COMMON_CRAWL_DATA_ENDPOINT,
    CommonCrawlArchiveAdapter,
    CommonCrawlLocator,
    RangeResponse,
    common_crawl_locator_from_dict,
)

NOW = datetime(2026, 8, 26, 20, tzinfo=UTC)
CAPTURED = datetime(2024, 11, 30, 14, 52, 51, tzinfo=UTC)


def digest(value: bytes) -> str:
    encoded = base64.b32encode(sha1(value, usedforsecurity=False).digest()).decode("ascii")
    return f"sha1:{encoded.rstrip('=')}"


def warc_member(
    *,
    payload: bytes = b"<html><title>Archived source</title></html>",
    target_url: str = "https://example.test/source",
    truncated: str | None = None,
) -> tuple[bytes, bytes, str]:
    block = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/html; charset=UTF-8\r\n"
        b"Date: Sat, 30 Nov 2024 14:52:51 GMT\r\n"
        b"\r\n" + payload
    )
    headers = [
        "WARC/1.0",
        "WARC-Type: response",
        "WARC-Date: 2024-11-30T14:52:51Z",
        "WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000001>",
        f"WARC-Target-URI: {target_url}",
        f"WARC-Payload-Digest: {digest(payload)}",
        f"WARC-Block-Digest: {digest(block)}",
        "WARC-Identified-Payload-Type: text/html",
        f"Content-Length: {len(block)}",
        "Content-Type: application/http; msgtype=response",
    ]
    if truncated is not None:
        headers.append(f"WARC-Truncated: {truncated}")
    raw = "\r\n".join(headers).encode("ascii") + b"\r\n\r\n" + block + b"\r\n\r\n"
    return gzip.compress(raw, mtime=0), raw, digest(payload)


def locator(compressed: bytes, payload_digest: str) -> CommonCrawlLocator:
    return CommonCrawlLocator(
        collection="CC-MAIN-2024-51",
        target_url="https://example.test/source",
        timestamp="20241130145251",
        filename=(
            "crawl-data/CC-MAIN-2024-51/segments/fixed/warc/CC-MAIN-20241130145248-00000.warc.gz"
        ),
        offset=910,
        length=len(compressed),
        digest=payload_digest,
        http_status=200,
    )


class FakeRangeTransport:
    def __init__(self, response: RangeResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, int, int, float]] = []

    def __call__(self, url: str, start: int, end: int, timeout_seconds: float) -> RangeResponse:
        self.calls.append((url, start, end, timeout_seconds))
        return self.response


def response_for(compressed: bytes, item: CommonCrawlLocator) -> RangeResponse:
    return RangeResponse(
        status=206,
        headers={"Content-Range": f"bytes {item.offset}-{item.range_end}/999999"},
        body=compressed,
    )


def test_common_crawl_adapter_validates_exact_range_warc_and_digests() -> None:
    compressed, _raw, payload_digest = warc_member()
    item = locator(compressed, payload_digest)
    transport = FakeRangeTransport(response_for(compressed, item))

    record = CommonCrawlArchiveAdapter(
        transport=transport,
        clock=lambda: NOW,
    ).fetch(item)

    assert record.captured_at == CAPTURED
    assert record.retrieved_at == NOW
    assert record.payload == b"<html><title>Archived source</title></html>"
    assert record.payload_digest == payload_digest
    assert record.archive_capture_accepted is True
    assert record.locator.source_version_id.startswith("common-crawl-record-")
    report = record.to_dict()
    assert report["payload_retained_in_report"] is False
    assert "payload" not in report
    assert report["execution_capability"] == "none"
    admission = report["historical_evidence_admission"]
    assert isinstance(admission, str)
    assert admission.startswith("requires_source_specific")
    url, start, end, timeout = transport.calls[0]
    assert url == f"{COMMON_CRAWL_DATA_ENDPOINT}/{item.filename}"
    assert (start, end, timeout) == (item.offset, item.range_end, 30.0)


def test_common_crawl_truncation_is_validated_but_not_receipt_eligible() -> None:
    compressed, _raw, payload_digest = warc_member(truncated="length")
    item = locator(compressed, payload_digest)

    record = CommonCrawlArchiveAdapter(
        transport=FakeRangeTransport(response_for(compressed, item)),
        clock=lambda: NOW,
    ).fetch(item)

    assert record.truncated_reason == "length"
    assert record.archive_capture_accepted is False


def test_common_crawl_locator_round_trips_and_rejects_self_asserted_identity() -> None:
    compressed, _raw, payload_digest = warc_member()
    item = locator(compressed, payload_digest)

    assert common_crawl_locator_from_dict(item.to_dict()) == item

    changed = item.to_dict()
    changed["source_version_id"] = "common-crawl-record-" + "0" * 64
    with pytest.raises(ValueError, match="does not match content"):
        common_crawl_locator_from_dict(changed)


def test_common_crawl_range_contract_fails_closed() -> None:
    compressed, _raw, payload_digest = warc_member()
    item = locator(compressed, payload_digest)
    base = response_for(compressed, item)
    failures = (
        (replace(base, status=200), "HTTP 206"),
        (replace(base, headers={"Content-Range": "bytes 0-1/2"}), "Content-Range"),
        (replace(base, body=b"short"), "length"),
    )

    for response, message in failures:
        with pytest.raises(ValueError, match=message):
            CommonCrawlArchiveAdapter(
                transport=FakeRangeTransport(response),
                clock=lambda: NOW,
            ).fetch(item)


def test_common_crawl_rejects_digest_target_and_timestamp_mismatch() -> None:
    compressed, _raw, payload_digest = warc_member()
    valid = locator(compressed, payload_digest)

    changed_digest = replace(valid, digest="sha1:" + "A" * 32)
    with pytest.raises(ValueError, match="digest"):
        CommonCrawlArchiveAdapter(
            transport=FakeRangeTransport(response_for(compressed, changed_digest)),
            clock=lambda: NOW,
        ).fetch(changed_digest)

    wrong_target_compressed, _raw, target_digest = warc_member(
        target_url="https://example.test/other"
    )
    wrong_target_locator = locator(wrong_target_compressed, target_digest)
    with pytest.raises(ValueError, match="target"):
        CommonCrawlArchiveAdapter(
            transport=FakeRangeTransport(
                response_for(wrong_target_compressed, wrong_target_locator)
            ),
            clock=lambda: NOW,
        ).fetch(wrong_target_locator)

    timestamp_raw = gzip.decompress(compressed).replace(
        b"WARC-Date: 2024-11-30T14:52:51Z",
        b"WARC-Date: 2024-11-30T14:52:52Z",
    )
    timestamp_compressed = gzip.compress(timestamp_raw, mtime=0)
    timestamp_locator = locator(timestamp_compressed, payload_digest)
    with pytest.raises(ValueError, match="WARC-Date"):
        CommonCrawlArchiveAdapter(
            transport=FakeRangeTransport(response_for(timestamp_compressed, timestamp_locator)),
            clock=lambda: NOW,
        ).fetch(timestamp_locator)


def test_common_crawl_rejects_widened_origin_path_and_multiple_members() -> None:
    with pytest.raises(ValueError, match="official HTTPS origin"):
        CommonCrawlArchiveAdapter(data_endpoint="https://example.test")
    with pytest.raises(ValueError, match="fixed collection"):
        CommonCrawlLocator(
            collection="CC-MAIN-2024-51",
            target_url="https://example.test/source",
            timestamp="20241130145251",
            filename="crawl-data/CC-MAIN-2024-51/../secret.warc.gz",
            offset=0,
            length=1,
            digest="sha1:" + "A" * 32,
            http_status=200,
        )

    compressed, _raw, payload_digest = warc_member()
    combined = compressed + compressed
    item = locator(combined, payload_digest)
    with pytest.raises(ValueError, match="exactly one"):
        CommonCrawlArchiveAdapter(
            transport=FakeRangeTransport(response_for(combined, item)),
            clock=lambda: NOW,
        ).fetch(item)


def test_common_crawl_rejects_a_gzip_bomb_before_materializing_it() -> None:
    compressed = gzip.compress(b"0" * (21 * 1024 * 1024), mtime=0)
    item = locator(compressed, digest(b"0"))

    with pytest.raises(ValueError, match="decompressed output exceeds the bounded contract"):
        CommonCrawlArchiveAdapter(
            transport=FakeRangeTransport(response_for(compressed, item)),
            clock=lambda: NOW,
        ).fetch(item)
