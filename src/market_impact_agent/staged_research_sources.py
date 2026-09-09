"""Bounded current-page captures used to supplement staged historical studies.

These captures preserve the raw official page and model historical availability from
the source's displayed publication information.  They are deliberately distinct
from ``official_archive`` evidence: a current page is not a verified historical
archive receipt.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import cast
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import EvidenceReference, canonical_hash
from market_impact_agent.csrc_news import (
    CsrcNewsHTTPClient,
    UrllibCsrcNewsHTTPClient,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.official_archive import (
    _CsrcHtmlParser,  # pyright: ignore[reportPrivateUsage]
    _decode_html,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.research import EvidenceTier

_SOURCE_SCHEMA = "market-impact.staged-csrc-study-source.v1"
_DOCUMENT_SCHEMA = "market-impact.staged-csrc-study-document.v1"
_CSRC_HOSTS = frozenset({"csrc.gov.cn", "www.csrc.gov.cn"})
_CSRC_DATE = re.compile(r"日期\s*[\uff1a:]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})")
_CSRC_PAGE_GENERATED_AT = re.compile(
    r"页面生成时间\s*(20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"
)
_SOURCE_ZONE = ZoneInfo("Asia/Shanghai")
_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_HISTORICAL_AUTHENTICATION = "modeled_pit_current_page_not_strict_historical_receipt"
_FOOTER_OR_NAVIGATION_MARKERS = (
    "版权所有",
    "网站地图",
    "联系我们",
    "网站标识码",
    "京ICP备",
    "建议使用",
    "中国证券监督管理委员会",
    "政府网站工作年度报表",
)
_NAVIGATION_PARAGRAPHS = frozenset(
    {
        "首页",
        "新闻",
        "要闻",
        "政策法规",
        "信息公开",
        "政务服务",
        "互动交流",
        "投资者保护",
        "无障碍浏览",
        "繁体",
        "English",
    }
)


@dataclass(frozen=True, slots=True)
class ReopenedCsrcStudySource:
    """A source document that has been revalidated from the captured raw HTML."""

    reference: EvidenceReference
    document: dict[str, object]
    source_hash: str
    window_id: str


def capture_csrc_study_source(
    url: str,
    window_id: str,
    cutoff: datetime,
    store: LocalDataSnapshotStore,
    *,
    http_client: CsrcNewsHTTPClient | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> str:
    """Capture one official CSRC current page and return its immutable record hash.

    ``fetched_at`` is the actual capture clock, never the historical study cutoff.
    A current capture is admitted only when the page's modeled source availability is
    no later than that cutoff.
    """

    _validate_window_id(window_id)
    requested_url = _validated_csrc_url(url, "CSRC study source URL")
    normalized_cutoff = _utc_timestamp(cutoff, "CSRC study cutoff")
    client = http_client or UrllibCsrcNewsHTTPClient()
    response = client.get(requested_url, max_response_bytes=_MAX_RESPONSE_BYTES)
    if response.final_url != requested_url:
        raise ValueError("CSRC study source redirect changes the exact requested URL")
    _validated_csrc_url(response.final_url, "CSRC study source final URL")
    if response.content_type not in {"text/html", "application/xhtml+xml"}:
        raise ValueError("CSRC study source response must be HTML")
    if len(response.body) > _MAX_RESPONSE_BYTES:
        raise ValueError("CSRC study source response exceeds byte limit")
    fetched_at = _utc_timestamp(clock(), "CSRC study source capture clock")
    raw_artifact = store.artifacts.put_bytes(response.body, media_type=response.content_type)
    document = _document_from_raw(
        raw=response.body,
        source_ref=requested_url,
        raw_content_hash=raw_artifact.content_hash,
        retrieved_at=fetched_at,
        cutoff=normalized_cutoff,
    )
    document_artifact = store.artifacts.put_json(document)
    source_record = {
        "schema_version": _SOURCE_SCHEMA,
        "capture_kind": "current_official_page",
        "historical_authentication": _HISTORICAL_AUTHENTICATION,
        "window_id": window_id,
        "cutoff": _timestamp(normalized_cutoff),
        "requested_url": requested_url,
        "final_url": response.final_url,
        "content_type": response.content_type,
        "fetched_at": _timestamp(fetched_at),
        "raw_content_hash": raw_artifact.content_hash,
        "raw_size_bytes": len(response.body),
        "document_hash": document_artifact.content_hash,
    }
    return store.artifacts.put_json(source_record).content_hash


def reopen_csrc_study_source(
    store: LocalDataSnapshotStore,
    source_hash: str,
    cutoff: datetime,
) -> ReopenedCsrcStudySource:
    """Reopen a captured source offline and verify its raw-to-document binding."""

    record = _source_record(store.artifacts.read_json(source_hash))
    normalized_cutoff = _utc_timestamp(cutoff, "CSRC study cutoff")
    if _parse_timestamp(_string(record, "cutoff"), "CSRC study source cutoff") != normalized_cutoff:
        raise ValueError("CSRC study source cutoff differs from its capture record")
    requested_url = _validated_csrc_url(
        _string(record, "requested_url"), "CSRC study source requested URL"
    )
    if _string(record, "final_url") != requested_url:
        raise ValueError("CSRC study source record final URL differs from its request")
    content_type = _string(record, "content_type")
    if content_type not in {"text/html", "application/xhtml+xml"}:
        raise ValueError("CSRC study source record has a non-HTML content type")
    raw_content_hash = _sha256_text(_string(record, "raw_content_hash"), "raw content hash")
    raw = store.artifacts.read_bytes(raw_content_hash)
    if len(raw) != _integer(record, "raw_size_bytes"):
        raise ValueError("CSRC study source raw artifact size differs from its record")
    if sha256(raw).hexdigest() != raw_content_hash:
        raise ValueError("CSRC study source raw artifact hash differs from its record")
    document = _document_from_raw(
        raw=raw,
        source_ref=requested_url,
        raw_content_hash=raw_content_hash,
        retrieved_at=_parse_timestamp(
            _string(record, "fetched_at"), "CSRC study source fetched_at"
        ),
        cutoff=normalized_cutoff,
    )
    document_hash = _sha256_text(_string(record, "document_hash"), "document hash")
    if canonical_hash(document) != document_hash:
        raise ValueError("CSRC study source raw content no longer reproduces its document")
    stored_document = store.artifacts.read_json(document_hash)
    if stored_document != document:
        raise ValueError("CSRC study source document artifact differs from parsed raw content")
    row = _objects(document["records"])[0]
    available_at = _parse_timestamp(_string(row, "available_at"), "CSRC study available_at")
    reference = EvidenceReference(
        evidence_id=f"csrc-study-source-{source_hash}",
        claim_id=_string(row, "evidence_record_id"),
        source_ref=requested_url,
        source_tier=EvidenceTier.OFFICIAL,
        available_at=available_at,
        content_hash=document_hash,
        summary=_string(row, "title"),
    )
    return ReopenedCsrcStudySource(
        reference=reference,
        document=document,
        source_hash=source_hash,
        window_id=_string(record, "window_id"),
    )


def _document_from_raw(
    *,
    raw: bytes,
    source_ref: str,
    raw_content_hash: str,
    retrieved_at: datetime,
    cutoff: datetime,
) -> dict[str, object]:
    parser = _CsrcHtmlParser()
    parser.feed(_decode_html(raw))
    parser.close()
    title = _source_title(parser)
    published_at, available_at, availability_basis, source_page_generated_at = _publication_times(
        parser
    )
    if available_at > cutoff:
        raise ValueError("CSRC study source is not available by the historical cutoff")
    paragraphs = _meaningful_paragraphs(parser.paragraphs)
    if not paragraphs:
        raise ValueError("CSRC study source does not expose meaningful body paragraphs")
    article_excerpt = "\n".join(paragraphs)
    evidence_core = {
        "source_ref": source_ref,
        "raw_content_hash": raw_content_hash,
        "title": title,
        "published_at": _timestamp(published_at),
        "available_at": _timestamp(available_at),
    }
    evidence_record_id = f"csrc-study-record-{canonical_hash(evidence_core)}"
    record = {
        "source_ref": source_ref,
        "content_hash": raw_content_hash,
        "evidence_record_id": evidence_record_id,
        "published_at": _timestamp(published_at),
        "available_at": _timestamp(available_at),
        "payload_status": "captured_current_official_page",
        "title": title,
        "article_excerpt": article_excerpt,
        "retrieved_at": _timestamp(retrieved_at),
        "historical_authentication": _HISTORICAL_AUTHENTICATION,
        "availability_basis": availability_basis,
        "paragraph_count": len(paragraphs),
    }
    if source_page_generated_at is not None:
        record["source_page_generated_at"] = _timestamp(source_page_generated_at)
    return {
        "schema_version": _DOCUMENT_SCHEMA,
        "records": [record],
        "retrieved_at": _timestamp(retrieved_at),
    }


def _source_title(parser: _CsrcHtmlParser) -> str:
    heading = " ".join(parser.heading_parts).strip()
    page_title = (
        " ".join(parser.title_parts).strip().removesuffix("_中国证券监督管理委员会").strip()
    )
    title = heading or page_title
    if not title:
        raise ValueError("CSRC study source does not expose a title")
    return title


def _publication_times(
    parser: _CsrcHtmlParser,
) -> tuple[datetime, datetime, str, datetime | None]:
    dates = tuple(dict.fromkeys(_CSRC_DATE.findall(" ".join(parser.text_parts))))
    if len(dates) != 1:
        raise ValueError("CSRC study source must expose exactly one displayed publication date")
    try:
        displayed_date = datetime.strptime(dates[0], "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("CSRC study source displayed publication date is invalid") from exc
    exact_pubdate = parser.meta.get("pubdate")
    if exact_pubdate is not None:
        try:
            source_time = datetime.strptime(exact_pubdate, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=_SOURCE_ZONE
            )
        except ValueError as exc:
            raise ValueError("CSRC study source PubDate is invalid") from exc
        if source_time.date() != displayed_date:
            generated_at = _page_generated_at(parser)
            if generated_at is not None and generated_at == source_time:
                return _conservative_displayed_date_times(displayed_date, generated_at)
            raise ValueError("CSRC study source PubDate and displayed date disagree")
        published_at = source_time.astimezone(UTC)
        return published_at, published_at, "source_reported_exact_pubdate", None
    return _conservative_displayed_date_times(displayed_date, None)


def _page_generated_at(parser: _CsrcHtmlParser) -> datetime | None:
    raw = parser.meta.get("others")
    if raw is None:
        return None
    match = _CSRC_PAGE_GENERATED_AT.fullmatch(raw)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=_SOURCE_ZONE)
    except ValueError as exc:
        raise ValueError("CSRC study source page-generation timestamp is invalid") from exc


def _conservative_displayed_date_times(
    displayed_date: date,
    source_page_generated_at: datetime | None,
) -> tuple[datetime, datetime, str, datetime | None]:
    published_at = datetime.combine(
        displayed_date, time(23, 59, 59), tzinfo=_SOURCE_ZONE
    ).astimezone(UTC)
    return (
        published_at,
        published_at + timedelta(minutes=5),
        "modeled_source_date_eod_plus_5m",
        None if source_page_generated_at is None else source_page_generated_at.astimezone(UTC),
    )


def _meaningful_paragraphs(paragraphs: list[str]) -> tuple[str, ...]:
    retained: list[str] = []
    for value in paragraphs:
        paragraph = " ".join(value.split())
        compact = paragraph.replace(" ", "")
        if not compact or _CSRC_DATE.fullmatch(paragraph) or _CSRC_DATE.fullmatch(compact):
            continue
        if compact.startswith("【字号") and compact.endswith("】"):
            continue
        if (
            compact in _NAVIGATION_PARAGRAPHS
            or "当前位置" in compact
            or "网站首页" in compact
            or ">" in paragraph
            or "\uff1e" in paragraph
        ):
            continue
        if any(marker in compact for marker in _FOOTER_OR_NAVIGATION_MARKERS):
            continue
        if paragraph not in retained:
            retained.append(paragraph)
    return tuple(retained)


def _source_record(value: object) -> dict[str, object]:
    record = _object(value)
    required = {
        "schema_version",
        "capture_kind",
        "historical_authentication",
        "window_id",
        "cutoff",
        "requested_url",
        "final_url",
        "content_type",
        "fetched_at",
        "raw_content_hash",
        "raw_size_bytes",
        "document_hash",
    }
    if set(record) != required or record.get("schema_version") != _SOURCE_SCHEMA:
        raise ValueError("unsupported CSRC study source record")
    if record.get("capture_kind") != "current_official_page":
        raise ValueError("CSRC study source record capture kind is invalid")
    if record.get("historical_authentication") != _HISTORICAL_AUTHENTICATION:
        raise ValueError("CSRC study source record historical authentication is invalid")
    _validate_window_id(_string(record, "window_id"))
    return record


def _validated_csrc_url(value: str, name: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.hostname.casefold() not in _CSRC_HOSTS
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or not parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{name} must be an exact public HTTPS csrc.gov.cn URL")
    return value


def _validate_window_id(value: str) -> None:
    if not value or value != value.strip():
        raise ValueError("CSRC study window_id must be non-empty trimmed text")


def _utc_timestamp(value: datetime, name: str) -> datetime:
    require_aware(value, name)
    return value.astimezone(UTC)


def _parse_timestamp(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    return _utc_timestamp(parsed, name)


def _timestamp(value: datetime) -> str:
    return _utc_timestamp(value, "CSRC study timestamp").isoformat().replace("+00:00", "Z")


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("CSRC study source artifact must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise ValueError("CSRC study source artifact must be an object")
    return {cast(str, key): item for key, item in raw.items()}


def _objects(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError("CSRC study document records must be an array")
    return [_object(item) for item in cast(list[object], value)]


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item or item != item.strip():
        raise ValueError(f"CSRC study source {key} must be non-empty trimmed text")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item < 1:
        raise ValueError(f"CSRC study source {key} must be a positive integer")
    return item


def _sha256_text(value: str, name: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"CSRC study source {name} must be a SHA-256 digest")
    return value
