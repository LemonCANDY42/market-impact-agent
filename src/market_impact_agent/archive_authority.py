from __future__ import annotations

import base64
import json
import re
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha1, sha256
from http.client import HTTPMessage
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from market_impact_agent.domain import require_aware

COMMON_CRAWL_DATA_ENDPOINT = "https://data.commoncrawl.org"
COMMON_CRAWL_PROVIDER_ID = "common-crawl-warc"
COMMON_CRAWL_ARCHIVE_ID = "common-crawl"
COMMON_CRAWL_ADAPTER_VERSION = "1.0.0"
COMMON_CRAWL_LOCATOR_SCHEMA = "market-impact.common-crawl-locator.v1"

_COLLECTION_PATTERN = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")
_DIGEST_PATTERN = re.compile(r"sha1:[A-Z2-7]{32}")
_MAX_COMPRESSED_RECORD_BYTES = 20 * 1024 * 1024
_MAX_DECOMPRESSED_RECORD_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class CommonCrawlLocator:
    collection: str
    target_url: str
    timestamp: str
    filename: str
    offset: int
    length: int
    digest: str
    http_status: int

    def __post_init__(self) -> None:
        if _COLLECTION_PATTERN.fullmatch(self.collection) is None:
            raise ValueError("Common Crawl collection must be a fixed CC-MAIN identifier")
        parsed_url = urlparse(self.target_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("Common Crawl target_url must be an absolute HTTP(S) URL")
        _capture_timestamp(self.timestamp)
        expected_prefix = f"crawl-data/{self.collection}/"
        if (
            not self.filename.startswith(expected_prefix)
            or not self.filename.endswith(".warc.gz")
            or ".." in self.filename.split("/")
        ):
            raise ValueError("Common Crawl filename must remain inside the fixed collection")
        if self.offset < 0:
            raise ValueError("Common Crawl offset must not be negative")
        if not 1 <= self.length <= _MAX_COMPRESSED_RECORD_BYTES:
            raise ValueError("Common Crawl record length is outside the bounded contract")
        normalized_digest = _normalize_digest(self.digest)
        object.__setattr__(self, "digest", normalized_digest)
        if not 100 <= self.http_status <= 599:
            raise ValueError("Common Crawl HTTP status is invalid")

    @property
    def captured_at(self) -> datetime:
        return _capture_timestamp(self.timestamp)

    @property
    def range_end(self) -> int:
        return self.offset + self.length - 1

    @property
    def source_version_id(self) -> str:
        identity = "\n".join(
            (
                self.collection,
                self.target_url,
                self.timestamp,
                self.filename,
                str(self.offset),
                str(self.length),
                self.digest,
                str(self.http_status),
            )
        ).encode()
        return f"common-crawl-record-{sha256(identity).hexdigest()}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": COMMON_CRAWL_LOCATOR_SCHEMA,
            "collection": self.collection,
            "target_url": self.target_url,
            "timestamp": self.timestamp,
            "filename": self.filename,
            "offset": self.offset,
            "length": self.length,
            "digest": self.digest,
            "http_status": self.http_status,
            "source_version_id": self.source_version_id,
        }


@dataclass(frozen=True, slots=True)
class RangeResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class RangeTransport(Protocol):
    def __call__(
        self,
        url: str,
        start: int,
        end: int,
        timeout_seconds: float,
    ) -> RangeResponse: ...


@dataclass(frozen=True, slots=True)
class VerifiedArchiveRecord:
    provider_id: str
    archive_id: str
    adapter_version: str
    locator: CommonCrawlLocator
    warc_record_id: str
    captured_at: datetime
    retrieved_at: datetime
    target_url: str
    http_status: int
    media_type: str | None
    archive_member_sha256: str
    warc_block_sha256: str
    payload_sha256: str
    payload_digest: str
    block_digest: str | None
    truncated_reason: str | None
    payload: bytes

    def __post_init__(self) -> None:
        require_aware(self.captured_at, "archive captured_at")
        require_aware(self.retrieved_at, "archive retrieved_at")
        if self.captured_at > self.retrieved_at:
            raise ValueError("archive capture must not follow local retrieval")
        for name in ("archive_member_sha256", "warc_block_sha256", "payload_sha256"):
            if re.fullmatch(r"[0-9a-f]{64}", getattr(self, name)) is None:
                raise ValueError(f"{name} must be a SHA-256 digest")
        if not self.payload:
            raise ValueError("verified archive payload must not be empty")

    @property
    def archive_capture_accepted(self) -> bool:
        return self.truncated_reason is None and self.http_status == 200

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "archive_id": self.archive_id,
            "adapter_version": self.adapter_version,
            "locator": self.locator.to_dict(),
            "warc_record_id": self.warc_record_id,
            "captured_at": _timestamp(self.captured_at),
            "retrieved_at": _timestamp(self.retrieved_at),
            "target_url": self.target_url,
            "http_status": self.http_status,
            "media_type": self.media_type,
            "archive_member_sha256": self.archive_member_sha256,
            "warc_block_sha256": self.warc_block_sha256,
            "payload_sha256": self.payload_sha256,
            "payload_digest": self.payload_digest,
            "block_digest": self.block_digest,
            "truncated_reason": self.truncated_reason,
            "archive_capture_accepted": self.archive_capture_accepted,
            "historical_evidence_admission": (
                "requires_source_specific_published_at_and_latency_calibration"
            ),
            "payload_retained_in_report": False,
            "execution_capability": "none",
        }


class CommonCrawlArchiveAdapter:
    def __init__(
        self,
        *,
        data_endpoint: str = COMMON_CRAWL_DATA_ENDPOINT,
        timeout_seconds: float = 30.0,
        transport: RangeTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if data_endpoint != COMMON_CRAWL_DATA_ENDPOINT:
            raise ValueError("Common Crawl adapter endpoint must remain the official HTTPS origin")
        if timeout_seconds <= 0:
            raise ValueError("Common Crawl timeout_seconds must be positive")
        self._data_endpoint = data_endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _range_get if transport is None else transport
        self._clock = clock

    def fetch(self, locator: CommonCrawlLocator) -> VerifiedArchiveRecord:
        url = f"{self._data_endpoint}/{locator.filename}"
        response = self._transport(
            url,
            locator.offset,
            locator.range_end,
            self._timeout_seconds,
        )
        _validate_range_response(response, locator)
        retrieved_at = self._clock()
        require_aware(retrieved_at, "Common Crawl retrieval clock")
        decompressed = _decompress_single_gzip_member(response.body)
        return validate_common_crawl_record(
            decompressed,
            locator=locator,
            retrieved_at=retrieved_at,
            archive_member_sha256=sha256(response.body).hexdigest(),
        )


def common_crawl_locator_from_dict(payload: object) -> CommonCrawlLocator:
    if not isinstance(payload, dict):
        raise TypeError("Common Crawl locator must be an object")
    typed_payload = cast(dict[str, object], payload)
    expected_keys = {
        "schema_version",
        "collection",
        "target_url",
        "timestamp",
        "filename",
        "offset",
        "length",
        "digest",
        "http_status",
        "source_version_id",
    }
    if set(typed_payload) != expected_keys:
        raise ValueError("Common Crawl locator fields do not match the closed contract")
    if typed_payload["schema_version"] != COMMON_CRAWL_LOCATOR_SCHEMA:
        raise ValueError("unsupported Common Crawl locator schema version")
    locator = CommonCrawlLocator(
        collection=_string(typed_payload, "collection"),
        target_url=_string(typed_payload, "target_url"),
        timestamp=_string(typed_payload, "timestamp"),
        filename=_string(typed_payload, "filename"),
        offset=_integer(typed_payload, "offset"),
        length=_integer(typed_payload, "length"),
        digest=_string(typed_payload, "digest"),
        http_status=_integer(typed_payload, "http_status"),
    )
    if typed_payload["source_version_id"] != locator.source_version_id:
        raise ValueError("Common Crawl locator source_version_id does not match content")
    return locator


def load_common_crawl_locator(path: Path) -> CommonCrawlLocator:
    return common_crawl_locator_from_dict(json.loads(path.read_text(encoding="utf-8")))


def validate_common_crawl_record(
    raw_record: bytes,
    *,
    locator: CommonCrawlLocator,
    retrieved_at: datetime,
    archive_member_sha256: str | None = None,
) -> VerifiedArchiveRecord:
    require_aware(retrieved_at, "Common Crawl retrieved_at")
    warc_header_bytes, remainder = _split_header(raw_record, "WARC")
    if not warc_header_bytes.startswith(b"WARC/1.0\r\n"):
        raise ValueError("Common Crawl record must use WARC/1.0 with CRLF framing")
    warc_headers = _headers(warc_header_bytes.split(b"\r\n")[1:], "WARC")
    if _one(warc_headers, "warc-type", "WARC-Type") != "response":
        raise ValueError("Common Crawl record must be a WARC response")
    target_url = _one(warc_headers, "warc-target-uri", "WARC-Target-URI")
    if target_url != locator.target_url:
        raise ValueError("Common Crawl record target does not match the locator")
    captured_at = _parse_timestamp(_one(warc_headers, "warc-date", "WARC-Date"))
    if captured_at != locator.captured_at:
        raise ValueError("Common Crawl WARC-Date does not match the locator timestamp")
    content_length = _integer_header(warc_headers, "content-length", "WARC Content-Length")
    if len(remainder) < content_length:
        raise ValueError("Common Crawl WARC block is shorter than Content-Length")
    block = remainder[:content_length]
    trailing = remainder[content_length:]
    if trailing not in {b"", b"\r\n\r\n"}:
        raise ValueError("Common Crawl WARC record has unexpected trailing bytes")

    http_header_bytes, payload = _split_header(block, "HTTP")
    status_line, *http_header_lines = http_header_bytes.split(b"\r\n")
    status_match = re.fullmatch(rb"HTTP/[0-9]+\.[0-9]+ ([0-9]{3})(?: .*)?", status_line)
    if status_match is None:
        raise ValueError("Common Crawl record has an invalid HTTP status line")
    http_status = int(status_match.group(1))
    if http_status != locator.http_status:
        raise ValueError("Common Crawl HTTP status does not match the locator")
    http_headers = _headers(http_header_lines, "HTTP", allow_duplicates=True)

    payload_digest = _normalize_digest(
        _one(warc_headers, "warc-payload-digest", "WARC-Payload-Digest")
    )
    computed_payload_digest = _sha1_base32(payload)
    if payload_digest != computed_payload_digest or payload_digest != locator.digest:
        raise ValueError("Common Crawl payload digest does not match the WARC and locator")
    block_digest = _optional_one(warc_headers, "warc-block-digest", "WARC-Block-Digest")
    if block_digest is not None:
        block_digest = _normalize_digest(block_digest)
        if block_digest != _sha1_base32(block):
            raise ValueError("Common Crawl WARC block digest does not match content")
    truncated_reason = _optional_one(warc_headers, "warc-truncated", "WARC-Truncated")
    media_type = _optional_one(
        warc_headers,
        "warc-identified-payload-type",
        "WARC-Identified-Payload-Type",
    )
    if media_type is None:
        content_types = http_headers.get("content-type", ())
        media_type = content_types[-1].split(";", 1)[0].strip() if content_types else None
    warc_record_id = _one(warc_headers, "warc-record-id", "WARC-Record-ID")
    return VerifiedArchiveRecord(
        provider_id=COMMON_CRAWL_PROVIDER_ID,
        archive_id=COMMON_CRAWL_ARCHIVE_ID,
        adapter_version=COMMON_CRAWL_ADAPTER_VERSION,
        locator=locator,
        warc_record_id=warc_record_id,
        captured_at=captured_at,
        retrieved_at=retrieved_at,
        target_url=target_url,
        http_status=http_status,
        media_type=media_type,
        archive_member_sha256=(
            sha256(raw_record).hexdigest()
            if archive_member_sha256 is None
            else archive_member_sha256
        ),
        warc_block_sha256=sha256(block).hexdigest(),
        payload_sha256=sha256(payload).hexdigest(),
        payload_digest=payload_digest,
        block_digest=block_digest,
        truncated_reason=truncated_reason,
        payload=payload,
    )


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        return None


def _range_get(url: str, start: int, end: int, timeout_seconds: float) -> RangeResponse:
    request = Request(
        url,
        headers={
            "Range": f"bytes={start}-{end}",
            "User-Agent": (
                "market-impact-agent/0.1 (+https://github.com/LemonCANDY42/market-impact-agent)"
            ),
        },
        method="GET",
    )
    try:
        with build_opener(_NoRedirectHandler()).open(
            request,
            timeout=timeout_seconds,
        ) as response:
            return RangeResponse(
                status=response.status,
                headers={key.casefold(): value for key, value in response.headers.items()},
                body=response.read(_MAX_COMPRESSED_RECORD_BYTES + 1),
            )
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError("Common Crawl range request failed") from exc


def _validate_range_response(response: RangeResponse, locator: CommonCrawlLocator) -> None:
    if response.status != 206:
        raise ValueError("Common Crawl range request must return HTTP 206")
    if len(response.body) != locator.length:
        raise ValueError("Common Crawl range response length does not match the locator")
    headers = {key.casefold(): value for key, value in response.headers.items()}
    content_range = headers.get("content-range")
    expected_prefix = f"bytes {locator.offset}-{locator.range_end}/"
    if content_range is None or not content_range.startswith(expected_prefix):
        raise ValueError("Common Crawl Content-Range does not match the locator")


def _decompress_single_gzip_member(value: bytes) -> bytes:
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        result = decompressor.decompress(value, _MAX_DECOMPRESSED_RECORD_BYTES + 1)
        if len(result) > _MAX_DECOMPRESSED_RECORD_BYTES:
            raise ValueError("Common Crawl decompressed output exceeds the bounded contract")
    except zlib.error as exc:
        raise ValueError("Common Crawl range is not a valid gzip member") from exc
    if not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
        raise ValueError("Common Crawl range must contain exactly one complete gzip member")
    return result


def _split_header(value: bytes, name: str) -> tuple[bytes, bytes]:
    parts = value.split(b"\r\n\r\n", 1)
    if len(parts) != 2:
        raise ValueError(f"{name} header terminator is missing")
    return parts[0] + b"\r\n", parts[1]


def _headers(
    lines: list[bytes],
    name: str,
    *,
    allow_duplicates: bool = False,
) -> dict[str, tuple[str, ...]]:
    result: dict[str, list[str]] = {}
    for line in lines:
        if not line:
            continue
        key_bytes, separator, value_bytes = line.partition(b":")
        if not separator:
            raise ValueError(f"{name} header line is invalid")
        try:
            key = key_bytes.decode("ascii").strip().casefold()
            value = value_bytes.decode("latin-1").strip()
        except UnicodeDecodeError as exc:
            raise ValueError(f"{name} header encoding is invalid") from exc
        if not key or not value:
            raise ValueError(f"{name} header name and value must not be empty")
        values = result.setdefault(key, [])
        if values and not allow_duplicates and key != "warc-protocol":
            raise ValueError(f"duplicate authority-bearing {name} header: {key}")
        values.append(value)
    return {key: tuple(values) for key, values in result.items()}


def _one(headers: Mapping[str, tuple[str, ...]], key: str, name: str) -> str:
    values = headers.get(key, ())
    if len(values) != 1:
        raise ValueError(f"{name} must occur exactly once")
    return values[0]


def _optional_one(headers: Mapping[str, tuple[str, ...]], key: str, name: str) -> str | None:
    values = headers.get(key, ())
    if not values:
        return None
    if len(values) != 1:
        raise ValueError(f"{name} must not be repeated")
    return values[0]


def _integer_header(headers: Mapping[str, tuple[str, ...]], key: str, name: str) -> int:
    value = _one(headers, key, name)
    if re.fullmatch(r"0|[1-9][0-9]*", value) is None:
        raise ValueError(f"{name} must be a canonical non-negative integer")
    return int(value)


def _capture_timestamp(value: str) -> datetime:
    if re.fullmatch(r"[0-9]{14}", value) is None:
        raise ValueError("Common Crawl timestamp must use YYYYMMDDhhmmss")
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError("Common Crawl timestamp is invalid") from exc


def _parse_timestamp(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError("WARC-Date must be a UTC timestamp")
    try:
        result = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("WARC-Date is invalid") from exc
    require_aware(result, "WARC-Date")
    return result


def _normalize_digest(value: str) -> str:
    normalized = value if value.startswith("sha1:") else f"sha1:{value}"
    normalized = normalized.upper().replace("SHA1:", "sha1:", 1)
    if _DIGEST_PATTERN.fullmatch(normalized) is None:
        raise ValueError("Common Crawl digest must be a SHA-1 Base32 value")
    return normalized


def _sha1_base32(value: bytes) -> str:
    digest = base64.b32encode(sha1(value, usedforsecurity=False).digest()).decode("ascii")
    return f"sha1:{digest.rstrip('=')}"


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise TypeError(f"Common Crawl locator {key} must be a string")
    return value


def _integer(payload: Mapping[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Common Crawl locator {key} must be an integer")
    return value
