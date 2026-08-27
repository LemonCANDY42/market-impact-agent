from __future__ import annotations

import base64
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha1, sha256
from http.client import HTTPMessage
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from market_impact_agent.domain import require_aware

INTERNET_ARCHIVE_CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
INTERNET_ARCHIVE_REPLAY_ENDPOINT = "https://web.archive.org/web"
INTERNET_ARCHIVE_PROVIDER_ID = "internet-archive-replay"
INTERNET_ARCHIVE_ARCHIVE_ID = "internet-archive"
INTERNET_ARCHIVE_ADAPTER_VERSION = "1.0.0"
INTERNET_ARCHIVE_LOCATOR_SCHEMA = "market-impact.internet-archive-locator.v1"

_DIGEST_PATTERN = re.compile(r"sha1:[A-Z2-7]{32}")
_MAX_BODY_BYTES = 20 * 1024 * 1024
_MAX_INDEX_RESPONSE_BYTES = 10 * 1024 * 1024
_TIMESTAMP_PATTERN = re.compile(r"[0-9]{14}")


@dataclass(frozen=True, slots=True)
class InternetArchiveLocator:
    target_url: str
    timestamp: str
    digest: str
    http_status: int
    media_type: str

    def __post_init__(self) -> None:
        parsed = urlparse(self.target_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Internet Archive target_url must be an absolute HTTP(S) URL")
        _capture_timestamp(self.timestamp)
        object.__setattr__(self, "digest", _normalize_digest(self.digest))
        if self.http_status != 200:
            raise ValueError("Internet Archive locator HTTP status must be 200")
        normalized_media_type = self.media_type.split(";", 1)[0].strip().casefold()
        if normalized_media_type not in {"text/html", "application/xhtml+xml"}:
            raise ValueError("Internet Archive locator media type must be HTML")
        object.__setattr__(self, "media_type", normalized_media_type)

    @property
    def captured_at(self) -> datetime:
        return _capture_timestamp(self.timestamp)

    @property
    def source_version_id(self) -> str:
        identity = "\n".join(
            (
                self.target_url,
                self.timestamp,
                self.digest,
                str(self.http_status),
                self.media_type,
            )
        ).encode()
        return f"internet-archive-record-{sha256(identity).hexdigest()}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": INTERNET_ARCHIVE_LOCATOR_SCHEMA,
            "target_url": self.target_url,
            "timestamp": self.timestamp,
            "digest": self.digest,
            "http_status": self.http_status,
            "media_type": self.media_type,
            "source_version_id": self.source_version_id,
        }


@dataclass(frozen=True, slots=True)
class ReplayResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class ReplayTransport(Protocol):
    def __call__(self, url: str, timeout_seconds: float) -> ReplayResponse: ...


class IndexTransport(Protocol):
    def __call__(
        self,
        endpoint: str,
        target_url: str,
        timeout_seconds: float,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class VerifiedInternetArchiveRecord:
    provider_id: str
    archive_id: str
    adapter_version: str
    locator: InternetArchiveLocator
    captured_at: datetime
    retrieved_at: datetime
    target_url: str
    http_status: int
    media_type: str
    replay_url: str
    payload_sha256: str
    payload_digest: str
    payload: bytes

    def __post_init__(self) -> None:
        require_aware(self.captured_at, "Internet Archive captured_at")
        require_aware(self.retrieved_at, "Internet Archive retrieved_at")
        if self.captured_at > self.retrieved_at:
            raise ValueError("archive capture must not follow local retrieval")
        if re.fullmatch(r"[0-9a-f]{64}", self.payload_sha256) is None:
            raise ValueError("payload_sha256 must be a SHA-256 digest")
        if not self.payload:
            raise ValueError("verified archive payload must not be empty")

    @property
    def archive_capture_accepted(self) -> bool:
        return self.http_status == 200 and self.payload_digest == self.locator.digest

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "archive_id": self.archive_id,
            "adapter_version": self.adapter_version,
            "locator": self.locator.to_dict(),
            "captured_at": _timestamp(self.captured_at),
            "retrieved_at": _timestamp(self.retrieved_at),
            "target_url": self.target_url,
            "http_status": self.http_status,
            "media_type": self.media_type,
            "replay_url": self.replay_url,
            "payload_sha256": self.payload_sha256,
            "payload_digest": self.payload_digest,
            "archive_capture_accepted": self.archive_capture_accepted,
            "historical_evidence_admission": (
                "requires_source_specific_published_at_and_latency_calibration"
            ),
            "payload_retained_in_report": False,
            "execution_capability": "none",
        }


class InternetArchiveIndexAdapter:
    def __init__(
        self,
        *,
        index_endpoint: str = INTERNET_ARCHIVE_CDX_ENDPOINT,
        timeout_seconds: float = 30.0,
        transport: IndexTransport | None = None,
    ) -> None:
        if index_endpoint != INTERNET_ARCHIVE_CDX_ENDPOINT:
            raise ValueError(
                "Internet Archive index endpoint must remain the official HTTPS origin"
            )
        if timeout_seconds <= 0:
            raise ValueError("Internet Archive timeout_seconds must be positive")
        self._index_endpoint = index_endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _index_get if transport is None else transport

    def locate_latest(
        self,
        *,
        target_url: str,
        not_after: datetime,
    ) -> InternetArchiveLocator | None:
        parsed = urlparse(target_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Internet Archive target_url must be an absolute HTTP(S) URL")
        require_aware(not_after, "Internet Archive index cutoff")
        raw = self._transport(self._index_endpoint, target_url, self._timeout_seconds)
        try:
            decoded = cast(object, json.loads(raw))
        except json.JSONDecodeError as exc:
            raise ValueError("Internet Archive index response is not valid JSON") from exc
        if not isinstance(decoded, list):
            raise ValueError("Internet Archive index response must be an array")
        payload = cast(list[object], decoded)
        if not payload:
            return None
        header = payload[0]
        expected_header = ["timestamp", "original", "statuscode", "digest", "mimetype"]
        if header != expected_header:
            raise ValueError("Internet Archive index fields do not match the closed contract")
        candidates: list[InternetArchiveLocator] = []
        for raw_row in payload[1:]:
            if not isinstance(raw_row, list):
                raise ValueError("Internet Archive index record is malformed")
            row = cast(list[object], raw_row)
            if len(row) != len(expected_header):
                raise ValueError("Internet Archive index record is malformed")
            values = dict(zip(expected_header, row, strict=True))
            if not all(isinstance(value, str) for value in values.values()):
                raise ValueError("Internet Archive index record fields must be strings")
            string_values = cast(dict[str, str], values)
            captured_url = string_values["original"]
            if not _same_archive_target(target_url, captured_url):
                raise ValueError("Internet Archive index returned a different target URL")
            item = InternetArchiveLocator(
                target_url=captured_url,
                timestamp=string_values["timestamp"],
                digest=string_values["digest"],
                http_status=_decimal_integer(string_values["statuscode"], "statuscode"),
                media_type=string_values["mimetype"],
            )
            if item.captured_at <= not_after:
                candidates.append(item)
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item.captured_at, item.source_version_id))


class InternetArchiveAdapter:
    def __init__(
        self,
        *,
        replay_endpoint: str = INTERNET_ARCHIVE_REPLAY_ENDPOINT,
        timeout_seconds: float = 30.0,
        transport: ReplayTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if replay_endpoint != INTERNET_ARCHIVE_REPLAY_ENDPOINT:
            raise ValueError(
                "Internet Archive replay endpoint must remain the official HTTPS origin"
            )
        if timeout_seconds <= 0:
            raise ValueError("Internet Archive timeout_seconds must be positive")
        self._replay_endpoint = replay_endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _replay_get if transport is None else transport
        self._clock = clock

    def fetch(self, locator: InternetArchiveLocator) -> VerifiedInternetArchiveRecord:
        replay_url = f"{self._replay_endpoint}/{locator.timestamp}id_/{locator.target_url}"
        response = self._transport(replay_url, self._timeout_seconds)
        if response.status != 200:
            raise ValueError("Internet Archive replay must return HTTP 200 without redirects")
        if not response.body:
            raise ValueError("Internet Archive replay body must not be empty")
        if len(response.body) > _MAX_BODY_BYTES:
            raise ValueError("Internet Archive replay body exceeds the bounded contract")
        payload_digest = _sha1_base32(response.body)
        if payload_digest != locator.digest:
            raise ValueError("Internet Archive replay digest does not match the locator")
        content_type = response.headers.get("content-type")
        media_type = locator.media_type
        if content_type is not None:
            replay_media_type = content_type.split(";", 1)[0].strip().casefold()
            if replay_media_type != locator.media_type:
                raise ValueError("Internet Archive replay media type does not match the locator")
            media_type = replay_media_type
        retrieved_at = self._clock()
        require_aware(retrieved_at, "Internet Archive retrieval clock")
        return VerifiedInternetArchiveRecord(
            provider_id=INTERNET_ARCHIVE_PROVIDER_ID,
            archive_id=INTERNET_ARCHIVE_ARCHIVE_ID,
            adapter_version=INTERNET_ARCHIVE_ADAPTER_VERSION,
            locator=locator,
            captured_at=locator.captured_at,
            retrieved_at=retrieved_at,
            target_url=locator.target_url,
            http_status=locator.http_status,
            media_type=media_type,
            replay_url=replay_url,
            payload_sha256=sha256(response.body).hexdigest(),
            payload_digest=payload_digest,
            payload=response.body,
        )


def internet_archive_locator_from_dict(payload: object) -> InternetArchiveLocator:
    if not isinstance(payload, dict):
        raise TypeError("Internet Archive locator must be an object")
    typed_payload = cast(dict[str, object], payload)
    expected_keys = {
        "schema_version",
        "target_url",
        "timestamp",
        "digest",
        "http_status",
        "media_type",
        "source_version_id",
    }
    if set(typed_payload) != expected_keys:
        raise ValueError("Internet Archive locator fields do not match the closed contract")
    if typed_payload["schema_version"] != INTERNET_ARCHIVE_LOCATOR_SCHEMA:
        raise ValueError("unsupported Internet Archive locator schema version")
    locator = InternetArchiveLocator(
        target_url=_string(typed_payload, "target_url"),
        timestamp=_string(typed_payload, "timestamp"),
        digest=_string(typed_payload, "digest"),
        http_status=_integer(typed_payload, "http_status"),
        media_type=_string(typed_payload, "media_type"),
    )
    if typed_payload["source_version_id"] != locator.source_version_id:
        raise ValueError("Internet Archive locator source_version_id does not match content")
    return locator


def load_internet_archive_locator(path: Path) -> InternetArchiveLocator:
    return internet_archive_locator_from_dict(json.loads(path.read_text(encoding="utf-8")))


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


def _index_get(endpoint: str, target_url: str, timeout_seconds: float) -> str:
    query = urlencode(
        {
            "url": target_url,
            "output": "json",
            "filter": "statuscode:200",
            "fl": "timestamp,original,statuscode,digest,mimetype",
            "collapse": "digest",
        }
    )
    response = _bounded_get(
        f"{endpoint}?{query}",
        timeout_seconds=timeout_seconds,
        max_bytes=_MAX_INDEX_RESPONSE_BYTES,
        accept="application/json",
        error_label="Internet Archive index request",
    )
    try:
        return response.body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Internet Archive index response is not UTF-8") from exc


def _replay_get(url: str, timeout_seconds: float) -> ReplayResponse:
    return _bounded_get(
        url,
        timeout_seconds=timeout_seconds,
        max_bytes=_MAX_BODY_BYTES,
        accept="text/html,application/xhtml+xml",
        error_label="Internet Archive replay request",
    )


def _bounded_get(
    url: str,
    *,
    timeout_seconds: float,
    max_bytes: int,
    accept: str,
    error_label: str,
) -> ReplayResponse:
    request = Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": (
                "market-impact-agent/0.1 (+https://github.com/LemonCANDY42/market-impact-agent)"
            ),
        },
        method="GET",
    )
    try:
        with build_opener(_NoRedirectHandler()).open(request, timeout=timeout_seconds) as response:
            body = response.read(max_bytes + 1)
            result = ReplayResponse(
                status=response.status,
                headers={key.casefold(): value for key, value in response.headers.items()},
                body=body,
            )
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"{error_label} failed") from exc
    if result.status != 200:
        raise RuntimeError(f"{error_label} did not return HTTP 200")
    if len(result.body) > max_bytes:
        raise ValueError(f"{error_label} exceeds the bounded contract")
    return result


def _capture_timestamp(value: str) -> datetime:
    if _TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise ValueError("Internet Archive timestamp must be YYYYMMDDhhmmss")
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError("Internet Archive timestamp is invalid") from exc


def _normalize_digest(value: str) -> str:
    normalized = value.upper()
    if not normalized.startswith("SHA1:"):
        normalized = f"SHA1:{normalized}"
    normalized = normalized.replace("SHA1:", "sha1:", 1)
    if _DIGEST_PATTERN.fullmatch(normalized) is None:
        raise ValueError("Internet Archive digest must be SHA-1 Base32")
    return normalized


def _sha1_base32(payload: bytes) -> str:
    encoded = base64.b32encode(sha1(payload, usedforsecurity=False).digest()).decode("ascii")
    return f"sha1:{encoded.rstrip('=')}"


def _same_archive_target(requested_url: str, captured_url: str) -> bool:
    requested = urlparse(requested_url)
    captured = urlparse(captured_url)
    return (
        requested.scheme in {"http", "https"}
        and captured.scheme in {"http", "https"}
        and requested.netloc.casefold() == captured.netloc.casefold()
        and requested.path == captured.path
        and requested.params == captured.params
        and requested.query == captured.query
        and requested.fragment == captured.fragment
    )


def _decimal_integer(value: str, field: str) -> int:
    if re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError(f"Internet Archive index {field} must be an integer")
    return int(value)


def _string(payload: Mapping[str, object], field: str) -> str:
    value = payload[field]
    if not isinstance(value, str):
        raise TypeError(f"Internet Archive locator {field} must be a string")
    return value


def _integer(payload: Mapping[str, object], field: str) -> int:
    value = payload[field]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Internet Archive locator {field} must be an integer")
    return value


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
