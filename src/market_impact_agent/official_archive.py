from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, time
from html.parser import HTMLParser
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.archive_authority import VerifiedArchiveRecord
from market_impact_agent.internet_archive import VerifiedInternetArchiveRecord
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
    RegimeEvidenceRecord,
)

_CSRC_HOSTS = frozenset({"csrc.gov.cn", "www.csrc.gov.cn"})
_STATE_COUNCIL_HOSTS = frozenset({"english.www.gov.cn", "www.gov.cn"})
_NBS_HOSTS = frozenset({"stats.gov.cn", "www.stats.gov.cn"})
_CSRC_DATE = re.compile(r"日期\s*[\uff1a:]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})")
_CSRC_TRANSCRIPT_TIMESTAMP = re.compile(
    r"(20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"
)
_STATE_COUNCIL_UPDATED = re.compile(
    r"Updated:\s*([A-Za-z]+ [0-9]{1,2}, [0-9]{4} [0-9]{2}:[0-9]{2})"
)


class _CsrcHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._stack: list[str] = []
        self.title_parts: list[str] = []
        self.heading_parts: list[str] = []
        self.text_parts: list[str] = []
        self.meta: dict[str, str] = {}
        self.paragraphs: list[str] = []
        self._paragraph_depth = 0
        self._paragraph_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.casefold()
        if normalized == "meta":
            self._capture_meta(attrs)
        if normalized == "p":
            self._paragraph_depth += 1
        self._stack.append(normalized)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() == "meta":
            self._capture_meta(attrs)

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.casefold()
        if normalized == "p" and self._paragraph_depth:
            paragraph = " ".join(" ".join(self._paragraph_parts).split())
            if paragraph:
                self.paragraphs.append(paragraph)
            self._paragraph_parts.clear()
            self._paragraph_depth -= 1
        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index] == normalized:
                del self._stack[index:]
                break

    def handle_data(self, data: str) -> None:
        normalized = " ".join(data.split())
        if not normalized:
            return
        self.text_parts.append(normalized)
        if "title" in self._stack:
            self.title_parts.append(normalized)
        if "h2" in self._stack:
            self.heading_parts.append(normalized)
        if self._paragraph_depth:
            self._paragraph_parts.append(normalized)

    def _capture_meta(self, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.casefold(): value for key, value in attrs if value is not None}
        name = values.get("name")
        content = values.get("content")
        if name and content:
            self.meta[name.casefold()] = content.strip()


@dataclass(frozen=True, slots=True)
class CsrcTranscriptSegmentSnapshot:
    record: RegimeEvidenceRecord
    transcript_excerpt: str
    paragraph_count: int

    def to_research_document(self) -> dict[str, object]:
        return {
            "title": self.record.title,
            "published_at": _timestamp(self.record.published_at),
            "occurred_at": _timestamp(self.record.occurred_at),
            "publisher_id": self.record.publisher_id,
            "source_ref": self.record.source_ref,
            "transcript_excerpt": self.transcript_excerpt,
            "paragraph_count": self.paragraph_count,
            "content_hash": self.record.content_hash,
        }


def extract_csrc_transcript_segment(
    archive_record: VerifiedArchiveRecord | VerifiedInternetArchiveRecord,
    *,
    segment_started_at: datetime,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
) -> CsrcTranscriptSegmentSnapshot:
    if not archive_record.archive_capture_accepted:
        raise ValueError("CSRC transcript extraction requires an accepted archive capture")
    host = urlparse(archive_record.target_url).hostname
    if host is None or host.casefold() not in _CSRC_HOSTS:
        raise ValueError("CSRC transcript extraction requires the official CSRC host")
    if archive_record.media_type not in {"text/html", "application/xhtml+xml"}:
        raise ValueError("CSRC transcript extraction requires an HTML archive payload")
    if segment_started_at.tzinfo is None or segment_started_at.utcoffset() is None:
        raise ValueError("CSRC transcript segment timestamp must be timezone-aware")

    parser = _CsrcHtmlParser()
    parser.feed(_decode_html(archive_record.payload))
    parser.close()
    source_zone = ZoneInfo("Asia/Shanghai")
    requested_local = segment_started_at.astimezone(source_zone)
    requested_text = requested_local.strftime("%Y-%m-%d %H:%M:%S")
    timestamp_positions: list[tuple[int, str]] = []
    for index, paragraph in enumerate(parser.paragraphs):
        normalized_paragraph = re.sub(r"\s*:\s*", ":", paragraph)
        matches = tuple(dict.fromkeys(_CSRC_TRANSCRIPT_TIMESTAMP.findall(normalized_paragraph)))
        if len(matches) == 1 and normalized_paragraph == matches[0]:
            timestamp_positions.append((index, matches[0]))
    matches = [item for item in timestamp_positions if item[1] == requested_text]
    if len(matches) != 1:
        raise ValueError("CSRC transcript segment timestamp must match exactly one source marker")
    start_index = matches[0][0]
    next_index = next(
        (index for index, _value in timestamp_positions if index > start_index),
        len(parser.paragraphs),
    )
    segment_paragraphs = tuple(
        paragraph for paragraph in parser.paragraphs[start_index + 1 : next_index] if paragraph
    )
    if not segment_paragraphs:
        raise ValueError("CSRC transcript segment must contain source text")
    transcript_excerpt = "\n".join(segment_paragraphs)
    heading = " ".join(parser.heading_parts).strip()
    page_title = " ".join(parser.title_parts).strip()
    source_title = heading or page_title.removesuffix("_中国证券监督管理委员会").strip()
    if not source_title:
        raise ValueError("CSRC transcript page does not expose a title")
    published_at = requested_local.astimezone(UTC)
    segment_core: dict[str, object] = {
        "source_ref": archive_record.target_url,
        "source_version_id": archive_record.locator.source_version_id,
        "segment_started_at": _timestamp(published_at),
        "paragraphs": list(segment_paragraphs),
    }
    content_hash = canonical_hash(segment_core)
    record = RegimeEvidenceRecord.build(
        case_keys=case_keys,
        category="official_context",
        source_id="csrc-official-archive",
        provider_id="csrc-web-archive",
        publisher_id="csrc",
        source_ref=archive_record.target_url,
        claim_id=claim_id,
        lineage_id=lineage_id,
        title=f"{source_title} — {requested_text} transcript segment",
        occurred_at=published_at,
        published_at=published_at,
        source_updated_at=None,
        available_at=published_at,
        availability_basis=RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED,
        latency_model_id=None,
        latency_model_hash=None,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id=archive_record.locator.source_version_id,
        authority_at=archive_record.captured_at,
        authority_hash=_authority_hash(archive_record),
        content_hash=content_hash,
        supersedes_id=None,
        license_scope="public_document",
    )
    return CsrcTranscriptSegmentSnapshot(
        record=record,
        transcript_excerpt=transcript_excerpt,
        paragraph_count=len(segment_paragraphs),
    )


def extract_csrc_regime_evidence(
    archive_record: VerifiedArchiveRecord | VerifiedInternetArchiveRecord,
    *,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
) -> RegimeEvidenceRecord:
    if not archive_record.archive_capture_accepted:
        raise ValueError("CSRC extraction requires an accepted archive capture")
    host = urlparse(archive_record.target_url).hostname
    if host is None or host.casefold() not in _CSRC_HOSTS:
        raise ValueError("CSRC extraction requires the official CSRC host")
    if archive_record.media_type not in {"text/html", "application/xhtml+xml"}:
        raise ValueError("CSRC extraction requires an HTML archive payload")

    text = _decode_html(archive_record.payload)
    parser = _CsrcHtmlParser()
    parser.feed(text)
    parser.close()
    heading = " ".join(parser.heading_parts).strip()
    page_title = " ".join(parser.title_parts).strip()
    title = heading or page_title.removesuffix("_中国证券监督管理委员会").strip()
    if not title:
        raise ValueError("CSRC archived page does not expose a title")
    date_matches = tuple(dict.fromkeys(_CSRC_DATE.findall(" ".join(parser.text_parts))))
    if len(date_matches) != 1:
        raise ValueError("CSRC archived page must expose exactly one source publication date")
    try:
        publication_date = datetime.strptime(date_matches[0], "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("CSRC archived page publication date is invalid") from exc
    exact_pubdate = parser.meta.get("pubdate")
    if exact_pubdate is None:
        published_at = datetime.combine(
            publication_date,
            time(23, 59, 59),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).astimezone(UTC)
    else:
        try:
            source_time = datetime.strptime(exact_pubdate, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=ZoneInfo("Asia/Shanghai")
            )
        except ValueError as exc:
            raise ValueError("CSRC archived page PubDate is invalid") from exc
        if source_time.date() != publication_date:
            raise ValueError("CSRC archived page PubDate and displayed date disagree")
        published_at = source_time.astimezone(UTC)
    authority_hash = _authority_hash(archive_record)
    return RegimeEvidenceRecord.build(
        case_keys=case_keys,
        category="official_context",
        source_id="csrc-official-archive",
        provider_id="csrc-web-archive",
        publisher_id="csrc",
        source_ref=archive_record.target_url,
        claim_id=claim_id,
        lineage_id=lineage_id,
        title=title,
        occurred_at=None,
        published_at=published_at,
        source_updated_at=None,
        available_at=published_at,
        availability_basis=RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED,
        latency_model_id=None,
        latency_model_hash=None,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id=archive_record.locator.source_version_id,
        authority_at=archive_record.captured_at,
        authority_hash=authority_hash,
        content_hash=archive_record.payload_sha256,
        supersedes_id=None,
        license_scope="public_document",
    )


def extract_state_council_regime_evidence(
    archive_record: VerifiedArchiveRecord | VerifiedInternetArchiveRecord,
    *,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
) -> RegimeEvidenceRecord:
    if not archive_record.archive_capture_accepted:
        raise ValueError("State Council extraction requires an accepted archive capture")
    host = urlparse(archive_record.target_url).hostname
    if host is None or host.casefold() not in _STATE_COUNCIL_HOSTS:
        raise ValueError("State Council extraction requires an official gov.cn host")
    if archive_record.media_type not in {"text/html", "application/xhtml+xml"}:
        raise ValueError("State Council extraction requires an HTML archive payload")

    parser = _CsrcHtmlParser()
    parser.feed(_decode_html(archive_record.payload))
    parser.close()
    title = " ".join(parser.title_parts).strip()
    if not title:
        raise ValueError("State Council archived page does not expose a title")
    publish_date = parser.meta.get("publishdate")
    if (
        publish_date is not None
        and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", publish_date) is None
    ):
        raise ValueError("State Council archived page publishdate is not canonical")
    updated_matches = tuple(
        dict.fromkeys(_STATE_COUNCIL_UPDATED.findall(" ".join(parser.text_parts)))
    )
    if len(updated_matches) != 1:
        raise ValueError("State Council archived page must expose one exact Updated time")
    try:
        updated_local = datetime.strptime(updated_matches[0], "%B %d, %Y %H:%M").replace(
            tzinfo=ZoneInfo("Asia/Shanghai")
        )
    except ValueError as exc:
        raise ValueError("State Council archived page Updated time is invalid") from exc
    if publish_date is not None and updated_local.date().isoformat() != publish_date:
        raise ValueError("State Council publishdate and Updated date disagree")
    published_at = updated_local.astimezone(UTC)
    return RegimeEvidenceRecord.build(
        case_keys=case_keys,
        category="official_context",
        source_id="state-council-official-archive",
        provider_id="gov-cn-web-archive",
        publisher_id="state-council",
        source_ref=archive_record.target_url,
        claim_id=claim_id,
        lineage_id=lineage_id,
        title=title,
        occurred_at=None,
        published_at=published_at,
        source_updated_at=published_at,
        available_at=published_at,
        availability_basis=RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED,
        latency_model_id=None,
        latency_model_hash=None,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id=archive_record.locator.source_version_id,
        authority_at=archive_record.captured_at,
        authority_hash=_authority_hash(archive_record),
        content_hash=archive_record.payload_sha256,
        supersedes_id=None,
        license_scope="public_document",
    )


def extract_nbs_macro_vintage(
    archive_record: VerifiedArchiveRecord | VerifiedInternetArchiveRecord,
    *,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
) -> RegimeEvidenceRecord:
    if not archive_record.archive_capture_accepted:
        raise ValueError("NBS extraction requires an accepted archive capture")
    host = urlparse(archive_record.target_url).hostname
    if host is None or host.casefold() not in _NBS_HOSTS:
        raise ValueError("NBS extraction requires the official stats.gov.cn host")
    if archive_record.media_type not in {"text/html", "application/xhtml+xml"}:
        raise ValueError("NBS extraction requires an HTML archive payload")

    parser = _CsrcHtmlParser()
    parser.feed(_decode_html(archive_record.payload))
    parser.close()
    title = " ".join(parser.title_parts).strip().removesuffix(" - 国家统计局").strip()
    if not title:
        raise ValueError("NBS archived page does not expose a title")
    publication_time = parser.meta.get("pubdate")
    if publication_time is None:
        raise ValueError("NBS archived page must expose an exact PubDate")
    try:
        published_local = datetime.strptime(publication_time, "%Y/%m/%d %H:%M").replace(
            tzinfo=ZoneInfo("Asia/Shanghai")
        )
    except ValueError as exc:
        raise ValueError("NBS archived page PubDate is invalid") from exc
    if publication_time not in " ".join(parser.text_parts):
        raise ValueError("NBS archived page PubDate is not corroborated in visible content")
    published_at = published_local.astimezone(UTC)
    return RegimeEvidenceRecord.build(
        case_keys=case_keys,
        category="macro_vintage",
        source_id="nbs-macro-vintage",
        provider_id="nbs-release-archive",
        publisher_id="nbs",
        source_ref=archive_record.target_url,
        claim_id=claim_id,
        lineage_id=lineage_id,
        title=title,
        occurred_at=None,
        published_at=published_at,
        source_updated_at=None,
        available_at=published_at,
        availability_basis=RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED,
        latency_model_id=None,
        latency_model_hash=None,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id=archive_record.locator.source_version_id,
        authority_at=archive_record.captured_at,
        authority_hash=_authority_hash(archive_record),
        content_hash=archive_record.payload_sha256,
        supersedes_id=None,
        license_scope="public_document",
    )


def _authority_hash(
    archive_record: VerifiedArchiveRecord | VerifiedInternetArchiveRecord,
) -> str:
    if isinstance(archive_record, VerifiedInternetArchiveRecord):
        return canonical_hash(
            {
                "locator": archive_record.locator.to_dict(),
                "replay_url": archive_record.replay_url,
                "payload_sha256": archive_record.payload_sha256,
                "payload_digest": archive_record.payload_digest,
            }
        )
    return canonical_hash(
        {
            "locator": archive_record.locator.to_dict(),
            "warc_record_id": archive_record.warc_record_id,
            "archive_member_sha256": archive_record.archive_member_sha256,
            "warc_block_sha256": archive_record.warc_block_sha256,
            "payload_sha256": archive_record.payload_sha256,
        }
    )


def _decode_html(payload: bytes) -> str:
    for encoding in ("utf-8", "gb18030"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("CSRC archived page encoding is not supported")


def _timestamp(value: datetime | None) -> str:
    if value is None or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("official evidence timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
