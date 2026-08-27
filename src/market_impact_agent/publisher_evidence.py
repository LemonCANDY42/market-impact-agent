from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from html.parser import HTMLParser
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
    RegimeEvidenceRecord,
)

_NEWS_LATENCY = timedelta(minutes=5)
_NEWS_LATENCY_MODEL_ID = "publisher-http-five-minute-latency-v1"
_NEWS_LATENCY_MODEL_HASH = canonical_hash(
    {
        "model_id": _NEWS_LATENCY_MODEL_ID,
        "latency_seconds": int(_NEWS_LATENCY.total_seconds()),
        "semantics": "current publisher version becomes usable five minutes after its timestamp",
    }
)
_PUBLISHERS = {
    "english.news.cn": ("xinhua", "xinhua-established-news"),
    "www.xinhuanet.com": ("xinhua", "xinhua-established-news"),
    "xinhuanet.com": ("xinhua", "xinhua-established-news"),
    "news.xinhuanet.com": ("xinhua", "xinhua-established-news"),
    "www.scmp.com": ("scmp", "scmp-established-news"),
    "scmp.com": ("scmp", "scmp-established-news"),
}
_MAX_HTML_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class PublisherNewsSnapshot:
    record: RegimeEvidenceRecord
    description: str | None
    paragraphs: tuple[str, ...]

    def to_research_document(self, *, max_characters: int = 1_000) -> dict[str, object]:
        if max_characters < 1:
            raise ValueError("publisher research excerpt limit must be positive")
        excerpt = " ".join(self.paragraphs)
        if len(excerpt) > max_characters:
            excerpt = excerpt[: max_characters - 1].rstrip() + "…"
        return {
            "title": self.record.title,
            "description": self.description,
            "published_at": _timestamp(self.record.published_at),
            "source_updated_at": (
                None
                if self.record.source_updated_at is None
                else _timestamp(self.record.source_updated_at)
            ),
            "publisher_id": self.record.publisher_id,
            "source_ref": self.record.source_ref,
            "article_excerpt": excerpt,
            "content_hash": self.record.content_hash,
        }


def capture_publisher_news_evidence(
    *,
    url: str,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
    timeout_seconds: float = 20.0,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RegimeEvidenceRecord:
    final_url, payload = _fetch_publisher_payload(url, timeout_seconds=timeout_seconds)
    return extract_publisher_news_evidence(
        url=final_url,
        payload=payload,
        retrieved_at=clock(),
        case_keys=case_keys,
        claim_id=claim_id,
        lineage_id=lineage_id,
    )


def capture_publisher_news_snapshot(
    *,
    url: str,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
    timeout_seconds: float = 20.0,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PublisherNewsSnapshot:
    final_url, payload = _fetch_publisher_payload(url, timeout_seconds=timeout_seconds)
    return extract_publisher_news_snapshot(
        url=final_url,
        payload=payload,
        retrieved_at=clock(),
        case_keys=case_keys,
        claim_id=claim_id,
        lineage_id=lineage_id,
    )


def _fetch_publisher_payload(url: str, *, timeout_seconds: float) -> tuple[str, bytes]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in _PUBLISHERS:
        raise ValueError("publisher evidence requires a registered publisher host")
    request = Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": "market-impact-agent/0.1 research-evidence",
        },
        method="GET",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        final_url = str(response.geturl())
        final_host = urlparse(final_url).hostname
        if final_host not in _PUBLISHERS or _PUBLISHERS[final_host] != _PUBLISHERS[parsed.hostname]:
            raise ValueError("publisher evidence redirected outside the registered publisher")
        content_type = response.headers.get_content_type()
        if content_type not in {"text/html", "application/xhtml+xml"}:
            raise ValueError("publisher evidence response must be HTML")
        payload = response.read(_MAX_HTML_BYTES + 1)
        if len(payload) > _MAX_HTML_BYTES:
            raise ValueError("publisher evidence HTML exceeds the size limit")
    return final_url, payload


class _PublisherHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.meta: dict[str, str] = {}
        self.text_parts: list[str] = []
        self.title_parts: list[str] = []
        self._in_title = False
        self.paragraphs: list[str] = []
        self._paragraph_depth = 0
        self._paragraph_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.casefold()
        if normalized == "title":
            self._in_title = True
        if normalized == "p":
            self._paragraph_depth += 1
        if normalized == "meta":
            values = {key.casefold(): value for key, value in attrs if value is not None}
            key = values.get("property") or values.get("name")
            content = values.get("content")
            if key and content:
                self.meta[key.casefold()] = content.strip()

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "title":
            self._in_title = False
        if tag.casefold() == "p" and self._paragraph_depth:
            paragraph = " ".join(" ".join(self._paragraph_parts).split())
            if paragraph:
                self.paragraphs.append(paragraph)
            self._paragraph_parts.clear()
            self._paragraph_depth -= 1

    def handle_data(self, data: str) -> None:
        normalized = " ".join(data.split())
        if normalized:
            self.text_parts.append(normalized)
            if self._in_title:
                self.title_parts.append(normalized)
            if self._paragraph_depth:
                self._paragraph_parts.append(normalized)


def extract_publisher_news_snapshot(
    *,
    url: str,
    payload: bytes,
    retrieved_at: datetime,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
) -> PublisherNewsSnapshot:
    record = extract_publisher_news_evidence(
        url=url,
        payload=payload,
        retrieved_at=retrieved_at,
        case_keys=case_keys,
        claim_id=claim_id,
        lineage_id=lineage_id,
    )
    parser = _parse_html(payload)
    description = parser.meta.get("description") or parser.meta.get("og:description")
    return PublisherNewsSnapshot(
        record=record,
        description=description,
        paragraphs=tuple(dict.fromkeys(parser.paragraphs)),
    )


def extract_publisher_news_evidence(
    *,
    url: str,
    payload: bytes,
    retrieved_at: datetime,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
) -> RegimeEvidenceRecord:
    if not payload:
        raise ValueError("publisher evidence requires non-empty HTML")
    if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
        raise ValueError("publisher evidence retrieved_at must be timezone-aware")
    parsed_url = urlparse(url)
    if parsed_url.scheme != "https" or parsed_url.hostname not in _PUBLISHERS:
        raise ValueError("publisher evidence requires a registered publisher host")
    parser = _parse_html(payload)
    publisher_id, source_id = _PUBLISHERS[parsed_url.hostname]
    title = (
        parser.meta.get("og:title")
        or parser.meta.get("twitter:title")
        or " ".join(parser.title_parts).strip()
    )
    if not title:
        raise ValueError("publisher evidence requires an exact title")
    if publisher_id == "xinhua":
        published_at = _xinhua_publication_time(parser)
        source_updated_at = None
    else:
        published_at = _iso_meta_time(parser, "article:published_time", required=True)
        if published_at is None:
            raise ValueError("publisher evidence requires an exact publication time")
        source_updated_at = _iso_meta_time(parser, "article:modified_time", required=False)
        if source_updated_at is not None and source_updated_at < published_at:
            raise ValueError("publisher modified time cannot precede publication")
    version_time = source_updated_at or published_at
    available_at = version_time + _NEWS_LATENCY
    content_hash = sha256(payload).hexdigest()
    authority_hash = canonical_hash(
        {
            "provider_id": "publisher-https-snapshot",
            "url": url,
            "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
            "content_hash": content_hash,
        }
    )
    return RegimeEvidenceRecord.build(
        case_keys=case_keys,
        category="established_news",
        source_id=source_id,
        provider_id="publisher-https-snapshot",
        publisher_id=publisher_id,
        source_ref=url,
        claim_id=claim_id,
        lineage_id=lineage_id,
        title=title,
        occurred_at=None,
        published_at=published_at,
        source_updated_at=source_updated_at,
        available_at=available_at,
        availability_basis=RegimeEvidenceAvailabilityBasis.MODELED_LATENCY,
        latency_model_id=_NEWS_LATENCY_MODEL_ID,
        latency_model_hash=_NEWS_LATENCY_MODEL_HASH,
        authority_kind=RegimeEvidenceAuthorityKind.PROVIDER_VERSION,
        authority_id=f"publisher-https-snapshot-{content_hash}",
        authority_at=retrieved_at.astimezone(UTC),
        authority_hash=authority_hash,
        content_hash=content_hash,
        supersedes_id=None,
        license_scope="private_licensed",
    )


def _xinhua_publication_time(parser: _PublisherHtmlParser) -> datetime:
    import re

    matches = tuple(
        dict.fromkeys(
            re.findall(
                r"\b(20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\b",
                " ".join(parser.text_parts),
            )
        )
    )
    publish_date = parser.meta.get("publishdate")
    candidates = tuple(
        item for item in matches if publish_date is None or item.startswith(publish_date)
    )
    if len(candidates) != 1:
        raise ValueError("publisher evidence requires exactly one exact publication time")
    return (
        datetime.strptime(candidates[0], "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        .astimezone(UTC)
    )


def _iso_meta_time(
    parser: _PublisherHtmlParser,
    name: str,
    *,
    required: bool,
) -> datetime | None:
    value = parser.meta.get(name)
    if value is None:
        if required:
            raise ValueError("publisher evidence requires an exact publication time")
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("publisher evidence has an invalid exact publication time") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("publisher evidence timestamp must contain an offset")
    return parsed.astimezone(UTC)


def _parse_html(payload: bytes) -> _PublisherHtmlParser:
    try:
        html = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("publisher evidence HTML must be UTF-8") from exc
    parser = _PublisherHtmlParser()
    parser.feed(html)
    parser.close()
    return parser


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
