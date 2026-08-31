from __future__ import annotations

import asyncio
import io
import json
import re
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from html.parser import HTMLParser
from itertools import pairwise
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import SplitResult, urljoin, urlsplit
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataProvider,
    DataQuery,
    DataSourceBinding,
    ProviderDataResponse,
    SourceObservation,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import (
    OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
    AvailabilityBasis,
    ObservationCapability,
    ObservationProviderManifest,
    ObservationTimes,
    ObservationTrustTier,
    OccurrenceBasis,
)
from market_impact_agent.providers import ProviderTransport

NBS_MACRO_RELEASE_SOURCE_SCHEMA = "market-impact.nbs-macro-release-source.v1"
NBS_MACRO_RELEASE_CAPTURE_SCHEMA = "market-impact.nbs-macro-release-capture.v1"
NBS_MACRO_RELEASE_PROVIDER_ID = "nbs-macro-release"
NBS_MACRO_RELEASE_PROVIDER_VERSION = "1"
NBS_MACRO_RELEASE_FEED_URL = "https://www.stats.gov.cn/sj/zxfb/rss.xml"
NBS_MACRO_RELEASE_RIGHTS_URL = "https://www.stats.gov.cn/wzgl/202302/t20230217_1912857.html"
NBS_MACRO_RELEASE_PUBLISHER = "国家统计局"
NBS_MACRO_RELEASE_TIMEZONE = "Asia/Shanghai"
NBS_MACRO_RELEASE_LICENSE_SCOPE = "official_public_private_research_no_redistribution"
NBS_MACRO_RELEASE_CONTENT_SCOPE = "official_nbs_cpi_ppi_original_release_private_research"
NBS_MACRO_RELEASE_SEMANTIC_SCOPE = "official_nbs_cpi_ppi_original_release_actual_receipt_only"
NBS_MACRO_RELEASE_REVISION_STRATEGY = (
    "append_only_content_versions_without_asserted_revision_relation"
)

_ALLOWED_INDICATORS = ("cpi", "ppi")
_FEED_CONTENT_TYPE = "text/xml"
_ARTICLE_CONTENT_TYPE = "text/html"
_XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_ARTICLE_PATH = re.compile(r"^/sj/zxfb/\d{6}/t\d{8}_\d+\.html$")
_ATTACHMENT_NAME = re.compile(r"^P\d+\.xlsx$")
_TITLE_PATTERNS = {
    "cpi": re.compile(r"^(?P<year>\d{4})年(?P<month>[1-9]|1[0-2])月份居民消费价格(?:\s|同比|环比)"),
    "ppi": re.compile(
        r"^(?P<year>\d{4})年(?P<month>[1-9]|1[0-2])月份工业生产者出厂价格"
        r"(?:\s|同比|环比)"
    ),
}
_XML_DECLARATION = re.compile(rb"<!\s*(?:DOCTYPE|ENTITY)\b", re.IGNORECASE)
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


class NbsMacroReleaseError(RuntimeError):
    error_kind = "nbs_macro_release_error"


class NbsMacroReleaseHTTPError(NbsMacroReleaseError):
    error_kind = "http_error"

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"NBS macro release route returned HTTP {status_code}")


class NbsMacroReleaseNetworkError(NbsMacroReleaseError):
    error_kind = "network_error"


class NbsMacroReleaseIdentityError(NbsMacroReleaseError):
    error_kind = "source_identity_mismatch"


class NbsMacroReleaseParseError(NbsMacroReleaseError):
    error_kind = "source_parse_error"


class NbsMacroReleaseSpreadsheetError(NbsMacroReleaseError):
    error_kind = "spreadsheet_parse_error"


class NbsMacroReleaseSpreadsheetMissingError(NbsMacroReleaseError):
    error_kind = "required_spreadsheet_missing"


@dataclass(frozen=True, slots=True)
class NbsMacroReleaseSourceConfig:
    source_config_id: str
    source_id: str
    feed_url: str
    publisher: str
    published_timezone: str
    indicators: tuple[str, ...]
    rights_basis_url: str
    rights_reviewed_at: datetime
    license_scope: str
    content_scope: str
    redistribution_allowed: bool
    require_spreadsheet: bool
    max_feed_bytes: int
    max_article_bytes: int
    max_attachment_bytes: int
    max_capture_bytes: int
    schema_version: str = NBS_MACRO_RELEASE_SOURCE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != NBS_MACRO_RELEASE_SOURCE_SCHEMA:
            raise ValueError("unsupported NBS macro release source schema_version")
        _identifier(self.source_id, "NBS macro release source_id")
        if self.feed_url != NBS_MACRO_RELEASE_FEED_URL:
            raise ValueError("NBS macro release feed_url must be the exact official feed")
        if self.publisher != NBS_MACRO_RELEASE_PUBLISHER:
            raise ValueError("NBS macro release publisher must be the official publisher")
        if self.published_timezone != NBS_MACRO_RELEASE_TIMEZONE:
            raise ValueError("NBS macro release published_timezone must be Asia/Shanghai")
        _indicator_tuple(self.indicators, "NBS macro release indicators")
        if self.rights_basis_url != NBS_MACRO_RELEASE_RIGHTS_URL:
            raise ValueError(
                "NBS macro release rights_basis_url must be the official service terms"
            )
        _strict_utc(self.rights_reviewed_at, "NBS macro release rights_reviewed_at")
        if self.license_scope != NBS_MACRO_RELEASE_LICENSE_SCOPE:
            raise ValueError(
                f"NBS macro release license_scope must be {NBS_MACRO_RELEASE_LICENSE_SCOPE}"
            )
        if self.content_scope != NBS_MACRO_RELEASE_CONTENT_SCOPE:
            raise ValueError(
                f"NBS macro release content_scope must be {NBS_MACRO_RELEASE_CONTENT_SCOPE}"
            )
        if self.redistribution_allowed:
            raise ValueError("NBS macro release source cannot declare redistribution permission")
        if not self.require_spreadsheet:
            raise ValueError("NBS macro release source must require the official spreadsheet")
        for name in (
            "max_feed_bytes",
            "max_article_bytes",
            "max_attachment_bytes",
            "max_capture_bytes",
        ):
            value = cast(int, getattr(self, name))
            if not 1 <= value <= 256 * 1024 * 1024:
                raise ValueError(f"NBS macro release {name} must be between 1 and 268435456")
        required_capture_capacity = self.max_feed_bytes + len(self.indicators) * (
            self.max_article_bytes + self.max_attachment_bytes
        )
        if self.max_capture_bytes < required_capture_capacity:
            raise ValueError("NBS macro release max_capture_bytes cannot hold the configured route")
        if self.source_config_id != self.expected_source_config_id:
            raise ValueError("NBS macro release source_config_id does not match content")

    @property
    def expected_source_config_id(self) -> str:
        return f"nbs-macro-release-source-{canonical_hash(self.core_dict())}"

    @property
    def artifact_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "feed_url": self.feed_url,
            "publisher": self.publisher,
            "published_timezone": self.published_timezone,
            "indicators": list(self.indicators),
            "rights_basis_url": self.rights_basis_url,
            "rights_reviewed_at": _timestamp(self.rights_reviewed_at),
            "license_scope": self.license_scope,
            "content_scope": self.content_scope,
            "redistribution_allowed": self.redistribution_allowed,
            "require_spreadsheet": self.require_spreadsheet,
            "max_feed_bytes": self.max_feed_bytes,
            "max_article_bytes": self.max_article_bytes,
            "max_attachment_bytes": self.max_attachment_bytes,
            "max_capture_bytes": self.max_capture_bytes,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "source_config_id": self.source_config_id}

    @classmethod
    def build(
        cls,
        *,
        source_id: str,
        indicators: tuple[str, ...],
        rights_reviewed_at: datetime,
        max_feed_bytes: int,
        max_article_bytes: int,
        max_attachment_bytes: int,
        max_capture_bytes: int,
    ) -> NbsMacroReleaseSourceConfig:
        core = {
            "schema_version": NBS_MACRO_RELEASE_SOURCE_SCHEMA,
            "source_id": source_id,
            "feed_url": NBS_MACRO_RELEASE_FEED_URL,
            "publisher": NBS_MACRO_RELEASE_PUBLISHER,
            "published_timezone": NBS_MACRO_RELEASE_TIMEZONE,
            "indicators": list(indicators),
            "rights_basis_url": NBS_MACRO_RELEASE_RIGHTS_URL,
            "rights_reviewed_at": _timestamp(rights_reviewed_at),
            "license_scope": NBS_MACRO_RELEASE_LICENSE_SCOPE,
            "content_scope": NBS_MACRO_RELEASE_CONTENT_SCOPE,
            "redistribution_allowed": False,
            "require_spreadsheet": True,
            "max_feed_bytes": max_feed_bytes,
            "max_article_bytes": max_article_bytes,
            "max_attachment_bytes": max_attachment_bytes,
            "max_capture_bytes": max_capture_bytes,
        }
        return cls(
            source_config_id=f"nbs-macro-release-source-{canonical_hash(core)}",
            source_id=source_id,
            feed_url=NBS_MACRO_RELEASE_FEED_URL,
            publisher=NBS_MACRO_RELEASE_PUBLISHER,
            published_timezone=NBS_MACRO_RELEASE_TIMEZONE,
            indicators=indicators,
            rights_basis_url=NBS_MACRO_RELEASE_RIGHTS_URL,
            rights_reviewed_at=rights_reviewed_at,
            license_scope=NBS_MACRO_RELEASE_LICENSE_SCOPE,
            content_scope=NBS_MACRO_RELEASE_CONTENT_SCOPE,
            redistribution_allowed=False,
            require_spreadsheet=True,
            max_feed_bytes=max_feed_bytes,
            max_article_bytes=max_article_bytes,
            max_attachment_bytes=max_attachment_bytes,
            max_capture_bytes=max_capture_bytes,
        )


@dataclass(frozen=True, slots=True)
class NbsMacroReleaseHTTPResponse:
    body: bytes
    final_url: str
    content_type: str

    def __post_init__(self) -> None:
        if not self.body:
            raise NbsMacroReleaseParseError("NBS macro release response is empty")
        _trimmed(self.final_url, "NBS macro release response final_url")
        _trimmed(self.content_type, "NBS macro release response content_type")


class NbsMacroReleaseHTTPClient(Protocol):
    def get(self, url: str, *, max_response_bytes: int) -> NbsMacroReleaseHTTPResponse: ...


@dataclass(frozen=True, slots=True)
class UrllibNbsMacroReleaseHTTPClient:
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("NBS macro release timeout_seconds must be positive")

    def get(self, url: str, *, max_response_bytes: int) -> NbsMacroReleaseHTTPResponse:
        if max_response_bytes < 1:
            raise ValueError("NBS macro release max_response_bytes must be positive")
        request = Request(
            url,
            headers={
                "Accept": (
                    "text/xml, text/html, "
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                "User-Agent": "market-impact-agent/0.1 (+private-research)",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read(max_response_bytes + 1)
                if len(body) > max_response_bytes:
                    raise NbsMacroReleaseParseError("NBS macro release response exceeds byte limit")
                return NbsMacroReleaseHTTPResponse(
                    body=body,
                    final_url=response.geturl(),
                    content_type=response.headers.get_content_type(),
                )
        except HTTPError as exc:
            raise NbsMacroReleaseHTTPError(exc.code) from exc
        except URLError as exc:
            raise NbsMacroReleaseNetworkError(str(exc.reason)) from exc


@dataclass(frozen=True, slots=True)
class NbsMacroReleaseArticleCapture:
    indicator: str
    article: NbsMacroReleaseHTTPResponse
    attachment: NbsMacroReleaseHTTPResponse

    def __post_init__(self) -> None:
        if self.indicator not in _ALLOWED_INDICATORS:
            raise ValueError("unsupported NBS macro release capture indicator")


@dataclass(frozen=True, slots=True)
class NbsMacroReleaseCapture:
    source_id: str
    retrieved_at: datetime
    feed: NbsMacroReleaseHTTPResponse | None
    articles: tuple[NbsMacroReleaseArticleCapture, ...]
    coverage_complete: bool
    failure_status: DataFetchStatus | None = None
    error_kind: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.source_id, "NBS macro release capture source_id")
        _strict_utc(self.retrieved_at, "NBS macro release capture retrieved_at")
        indicators = tuple(item.indicator for item in self.articles)
        if len(indicators) != len(set(indicators)):
            raise ValueError("NBS macro release capture indicators must be unique")
        if self.coverage_complete:
            if self.feed is None:
                raise ValueError("complete NBS macro release capture requires the RSS feed")
            if self.failure_status is not None or self.error_kind is not None:
                raise ValueError("complete NBS macro release capture cannot carry a failure")
        elif self.failure_status is None or self.error_kind is None:
            raise ValueError("incomplete NBS macro release capture requires a typed failure")


@dataclass(frozen=True, slots=True)
class _FeedEntry:
    title: str
    url: str
    published_at: datetime
    indicator: str | None
    reference_period: str | None


@dataclass(frozen=True, slots=True)
class _ArticleData:
    title: str
    published_at: datetime
    attachment_url: str


class NbsMacroReleaseProvider(DataProvider):
    def __init__(
        self,
        source_configs: tuple[NbsMacroReleaseSourceConfig, ...],
        *,
        http_client: NbsMacroReleaseHTTPClient | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not source_configs:
            raise ValueError("NBS macro release provider requires at least one source")
        sources = {item.source_id: item for item in source_configs}
        if len(sources) != len(source_configs):
            raise ValueError("NBS macro release source IDs must be unique")
        self._sources = sources
        self._http_client = (
            UrllibNbsMacroReleaseHTTPClient() if http_client is None else http_client
        )
        self._clock = (lambda: datetime.now(UTC)) if clock is None else clock
        self._manifest = ObservationProviderManifest(
            schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
            provider_id=NBS_MACRO_RELEASE_PROVIDER_ID,
            provider_version=NBS_MACRO_RELEASE_PROVIDER_VERSION,
            transport=ProviderTransport.HTTP,
            declared_capabilities=frozenset(
                {
                    ObservationCapability.EVENT_REVELATION,
                    ObservationCapability.MACRO_VINTAGE,
                }
            ),
            verified_capabilities=frozenset(
                {
                    ObservationCapability.EVENT_REVELATION,
                    ObservationCapability.MACRO_VINTAGE,
                }
            ),
            upstream_sources=tuple(item.source_id for item in source_configs),
            auth_required=False,
            provides_source_updated_at=False,
            provides_aggregator_fetched_at=False,
            provides_historical_occurrence_at=False,
            provides_revision_history=False,
            enabled=True,
            trust_tier=ObservationTrustTier.CONTRACT_VALIDATED,
            license_note=(
                "Official NBS CPI/PPI articles and required XLSX attachments are retained only "
                "in private research storage under the source-specific rights review. The route "
                "asserts original releases and actual receipt only; redistribution, historical "
                "availability, correction lineage, Evidence promotion, and execution are absent."
            ),
        )
        self._manifest.assert_valid()

    @property
    def manifest(self) -> ObservationProviderManifest:
        return self._manifest

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]:
        return self._sources[upstream_source].to_dict()

    async def collect(
        self,
        *,
        window_start: datetime,
        parameters: Mapping[str, object],
        require_complete_indicator_scope: bool = False,
    ) -> tuple[NbsMacroReleaseCapture, ...]:
        require_aware(window_start, "NBS macro release collection window_start")
        captures: list[NbsMacroReleaseCapture] = []
        for config in self._sources.values():
            indicators = _query_indicators(config, parameters)
            captures.append(
                await self._collect_one(
                    config,
                    window_start=window_start.astimezone(UTC),
                    indicators=indicators,
                    require_complete_indicator_scope=require_complete_indicator_scope,
                )
            )
        return tuple(captures)

    def replay(
        self,
        captures: tuple[NbsMacroReleaseCapture, ...],
    ) -> _CapturedNbsMacroReleaseProvider:
        return _CapturedNbsMacroReleaseProvider(self, captures)

    async def fetch(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
    ) -> ProviderDataResponse:
        config = self._sources.get(source.upstream_source)
        if config is None:
            return self._failed_response(
                source=source,
                retrieved_at=_utc_now(self._clock),
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_config_missing",
            )
        try:
            indicators = _query_indicators(config, query.parameters)
        except NbsMacroReleaseError as exc:
            return self._failed_response(
                source=source,
                retrieved_at=_utc_now(self._clock),
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind=exc.error_kind,
            )
        capture = await self._collect_one(
            config,
            window_start=query.window_start or query.as_of,
            indicators=indicators,
        )
        return self.response_from_capture(query=query, source=source, capture=capture)

    async def _collect_one(
        self,
        config: NbsMacroReleaseSourceConfig,
        *,
        window_start: datetime,
        indicators: tuple[str, ...],
        require_complete_indicator_scope: bool = False,
    ) -> NbsMacroReleaseCapture:
        feed: NbsMacroReleaseHTTPResponse | None = None
        articles: list[NbsMacroReleaseArticleCapture] = []
        try:
            feed = await asyncio.to_thread(
                self._http_client.get,
                config.feed_url,
                max_response_bytes=config.max_feed_bytes,
            )
            _validate_exact_response(
                feed,
                expected_url=config.feed_url,
                expected_content_type=_FEED_CONTENT_TYPE,
                kind="RSS feed",
            )
            entries = _parse_feed(feed.body, config=config)
            _validate_feed_window(entries, config=config, window_start=window_start)
            selected = _latest_entries(
                entries,
                indicators=indicators,
                window_start=window_start,
            )
            if require_complete_indicator_scope and selected and len(selected) != len(indicators):
                raise NbsMacroReleaseParseError(
                    "NBS macro release feed is missing a requested indicator in the query window"
                )
            total_size = len(feed.body)
            for entry in selected:
                article = await asyncio.to_thread(
                    self._http_client.get,
                    entry.url,
                    max_response_bytes=config.max_article_bytes,
                )
                _validate_exact_response(
                    article,
                    expected_url=entry.url,
                    expected_content_type=_ARTICLE_CONTENT_TYPE,
                    kind="article",
                )
                article_data = _parse_article(article.body, entry=entry, config=config)
                attachment = await asyncio.to_thread(
                    self._http_client.get,
                    article_data.attachment_url,
                    max_response_bytes=config.max_attachment_bytes,
                )
                _validate_exact_response(
                    attachment,
                    expected_url=article_data.attachment_url,
                    expected_content_type=_XLSX_CONTENT_TYPE,
                    kind="spreadsheet",
                )
                _validate_xlsx(attachment.body)
                total_size += len(article.body) + len(attachment.body)
                if total_size > config.max_capture_bytes:
                    raise NbsMacroReleaseParseError("NBS macro release capture exceeds byte limit")
                assert entry.indicator is not None
                articles.append(
                    NbsMacroReleaseArticleCapture(
                        indicator=entry.indicator,
                        article=article,
                        attachment=attachment,
                    )
                )
            retrieved_at = _utc_now(self._clock)
            if any(item.published_at > retrieved_at for item in selected):
                raise NbsMacroReleaseParseError(
                    "NBS macro release publication time is after actual receipt"
                )
            return NbsMacroReleaseCapture(
                source_id=config.source_id,
                retrieved_at=retrieved_at,
                feed=feed,
                articles=tuple(articles),
                coverage_complete=True,
            )
        except NbsMacroReleaseHTTPError as exc:
            return NbsMacroReleaseCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                feed=feed,
                articles=tuple(articles),
                coverage_complete=False,
                failure_status=(
                    DataFetchStatus.RATE_LIMITED
                    if exc.status_code == 429
                    else DataFetchStatus.ERROR
                ),
                error_kind=f"http_{exc.status_code}",
            )
        except NbsMacroReleaseError as exc:
            return NbsMacroReleaseCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                feed=feed,
                articles=tuple(articles),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        except Exception:
            return NbsMacroReleaseCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                feed=feed,
                articles=tuple(articles),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind="unexpected_provider_error",
            )

    def response_from_capture(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
        capture: NbsMacroReleaseCapture,
    ) -> ProviderDataResponse:
        config = self._sources.get(source.upstream_source)
        if config is None or capture.source_id != source.upstream_source:
            return self._failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_config_missing",
            )
        if query.capability not in {
            ObservationCapability.EVENT_REVELATION,
            ObservationCapability.MACRO_VINTAGE,
        }:
            return self._failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="unsupported_capability",
            )
        if not capture.coverage_complete:
            assert capture.failure_status is not None
            assert capture.error_kind is not None
            return self._failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=capture.failure_status,
                error_kind=capture.error_kind,
            )
        try:
            indicators = _query_indicators(config, query.parameters)
            observations = _observations_from_capture(
                capture,
                config=config,
                query=query,
                indicators=indicators,
            )
            raw_payload = _capture_bundle(capture, config=config)
        except NbsMacroReleaseError as exc:
            return self._failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        return ProviderDataResponse(
            status=DataFetchStatus.DATA if observations else DataFetchStatus.NO_DATA,
            provider_id=self.manifest.provider_id,
            provider_version=self.manifest.provider_version,
            upstream_source=source.upstream_source,
            retrieved_at=capture.retrieved_at,
            raw_payload=raw_payload,
            observations=tuple(item[0] for item in observations),
            raw_records=tuple((item.observation_id, raw) for item, raw in observations),
        )

    def _failed_response(
        self,
        *,
        source: DataSourceBinding,
        retrieved_at: datetime,
        status: DataFetchStatus,
        error_kind: str,
    ) -> ProviderDataResponse:
        return ProviderDataResponse(
            status=status,
            provider_id=self.manifest.provider_id,
            provider_version=self.manifest.provider_version,
            upstream_source=source.upstream_source,
            retrieved_at=retrieved_at,
            raw_payload=None,
            observations=(),
            raw_records=(),
            error_kind=error_kind,
        )


class _CapturedNbsMacroReleaseProvider(DataProvider):
    def __init__(
        self,
        provider: NbsMacroReleaseProvider,
        captures: tuple[NbsMacroReleaseCapture, ...],
    ) -> None:
        capture_by_source = {item.source_id: item for item in captures}
        if len(capture_by_source) != len(captures) or set(capture_by_source) != set(
            provider.manifest.upstream_sources
        ):
            raise ValueError(
                "NBS macro release captures must cover each registered source exactly once"
            )
        self._provider = provider
        self._captures = capture_by_source

    @property
    def manifest(self) -> ObservationProviderManifest:
        return self._provider.manifest

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]:
        return self._provider.public_source_config(upstream_source)

    async def fetch(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
    ) -> ProviderDataResponse:
        return self._provider.response_from_capture(
            query=query,
            source=source,
            capture=self._captures[source.upstream_source],
        )


def load_nbs_macro_release_source(path: Path) -> NbsMacroReleaseSourceConfig:
    return nbs_macro_release_source_from_dict(json.loads(path.read_text(encoding="utf-8")))


def nbs_macro_release_source_from_dict(payload: object) -> NbsMacroReleaseSourceConfig:
    errors = validate_agent_contract(payload, "nbs-macro-release-source.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    fields = _mapping(payload, "NBS macro release source")
    config = NbsMacroReleaseSourceConfig(
        schema_version=_string(fields, "schema_version"),
        source_config_id=_string(fields, "source_config_id"),
        source_id=_string(fields, "source_id"),
        feed_url=_string(fields, "feed_url"),
        publisher=_string(fields, "publisher"),
        published_timezone=_string(fields, "published_timezone"),
        indicators=_string_tuple(fields.get("indicators"), "indicators"),
        rights_basis_url=_string(fields, "rights_basis_url"),
        rights_reviewed_at=_datetime(fields.get("rights_reviewed_at"), "rights_reviewed_at"),
        license_scope=_string(fields, "license_scope"),
        content_scope=_string(fields, "content_scope"),
        redistribution_allowed=_boolean(fields, "redistribution_allowed"),
        require_spreadsheet=_boolean(fields, "require_spreadsheet"),
        max_feed_bytes=_integer(fields, "max_feed_bytes"),
        max_article_bytes=_integer(fields, "max_article_bytes"),
        max_attachment_bytes=_integer(fields, "max_attachment_bytes"),
        max_capture_bytes=_integer(fields, "max_capture_bytes"),
    )
    if config.to_dict() != fields:
        raise ValueError("NBS macro release source is not canonical")
    return config


def load_nbs_macro_release_capture_bundle(
    payload: bytes,
    *,
    config: NbsMacroReleaseSourceConfig,
    retrieved_at: datetime,
) -> NbsMacroReleaseCapture:
    _strict_utc(retrieved_at, "NBS macro release capture bundle retrieved_at")
    prefix = f"{NBS_MACRO_RELEASE_CAPTURE_SCHEMA}\n".encode()
    if not payload.startswith(prefix):
        raise NbsMacroReleaseParseError("NBS macro release capture bundle has an invalid header")
    offset = len(prefix)
    top, offset = _bundle_header(payload, offset, "capture header")
    if _string(top, "source_id") != config.source_id:
        raise NbsMacroReleaseIdentityError("NBS macro release capture source ID mismatch")
    if _string(top, "source_config_id") != config.source_config_id:
        raise NbsMacroReleaseIdentityError("NBS macro release capture config ID mismatch")
    component_count = _integer(top, "component_count")
    if not 1 <= component_count <= 1 + 2 * len(config.indicators):
        raise NbsMacroReleaseParseError("NBS macro release component count is invalid")
    responses: list[tuple[Mapping[str, object], NbsMacroReleaseHTTPResponse]] = []
    for _index in range(component_count):
        header, offset = _bundle_header(payload, offset, "component header")
        size = _integer(header, "body_size")
        if size < 1:
            raise NbsMacroReleaseParseError("NBS macro release component size is invalid")
        end = offset + size
        if end >= len(payload) or payload[end : end + 1] != b"\n":
            raise NbsMacroReleaseParseError("NBS macro release component body is truncated")
        body = payload[offset:end]
        if sha256(body).hexdigest() != _string(header, "body_sha256"):
            raise NbsMacroReleaseParseError("NBS macro release component hash mismatch")
        responses.append(
            (
                header,
                NbsMacroReleaseHTTPResponse(
                    body=body,
                    final_url=_string(header, "final_url"),
                    content_type=_string(header, "content_type"),
                ),
            )
        )
        offset = end + 1
    if offset != len(payload):
        raise NbsMacroReleaseParseError("NBS macro release capture bundle has trailing bytes")
    feed_header, feed = responses[0]
    if _string(feed_header, "kind") != "feed":
        raise NbsMacroReleaseIdentityError("NBS macro release feed component is missing")
    _validate_exact_response(
        feed,
        expected_url=config.feed_url,
        expected_content_type=_FEED_CONTENT_TYPE,
        kind="RSS feed",
    )
    if (len(responses) - 1) % 2 != 0:
        raise NbsMacroReleaseParseError("NBS macro release article components are incomplete")
    articles: list[NbsMacroReleaseArticleCapture] = []
    for index in range(1, len(responses), 2):
        article_header, article = responses[index]
        attachment_header, attachment = responses[index + 1]
        indicator = _string(article_header, "indicator")
        if _string(article_header, "kind") != "article":
            raise NbsMacroReleaseIdentityError("NBS macro release article component mismatch")
        if (
            _string(attachment_header, "kind") != "attachment"
            or _string(attachment_header, "indicator") != indicator
        ):
            raise NbsMacroReleaseIdentityError("NBS macro release attachment component mismatch")
        _article_url(article.final_url)
        _attachment_url(attachment.final_url, article_url=article.final_url)
        _validate_exact_response(
            article,
            expected_url=article.final_url,
            expected_content_type=_ARTICLE_CONTENT_TYPE,
            kind="article",
        )
        _validate_exact_response(
            attachment,
            expected_url=attachment.final_url,
            expected_content_type=_XLSX_CONTENT_TYPE,
            kind="spreadsheet",
        )
        _validate_xlsx(attachment.body)
        articles.append(
            NbsMacroReleaseArticleCapture(
                indicator=indicator,
                article=article,
                attachment=attachment,
            )
        )
    if len(payload) > config.max_capture_bytes + 16_384:
        raise NbsMacroReleaseParseError("NBS macro release capture bundle exceeds byte limit")
    return NbsMacroReleaseCapture(
        source_id=config.source_id,
        retrieved_at=retrieved_at,
        feed=feed,
        articles=tuple(articles),
        coverage_complete=True,
    )


def _parse_feed(body: bytes, *, config: NbsMacroReleaseSourceConfig) -> tuple[_FeedEntry, ...]:
    if _XML_DECLARATION.search(body):
        raise NbsMacroReleaseParseError("NBS macro release feed contains a DTD or entity")
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        raise NbsMacroReleaseParseError("NBS macro release feed is malformed XML") from exc
    if root.tag != "rss" or root.attrib != {"version": "2.0"}:
        raise NbsMacroReleaseIdentityError("NBS macro release feed root identity mismatch")
    channels = root.findall("channel")
    if len(channels) != 1:
        raise NbsMacroReleaseIdentityError("NBS macro release feed channel is not unique")
    channel = channels[0]
    if _unique_child_text(channel, "title") != "数据发布":
        raise NbsMacroReleaseIdentityError("NBS macro release feed title mismatch")
    if _unique_child_text(channel, "link") != "https://www.stats.gov.cn/sj/zxfb/":
        raise NbsMacroReleaseIdentityError("NBS macro release feed channel URL mismatch")
    entries: list[_FeedEntry] = []
    seen_urls: set[str] = set()
    for item in channel.findall("item"):
        title = _unique_child_text(item, "title")
        url = _unique_child_text(item, "link")
        _article_url(url)
        if url in seen_urls:
            raise NbsMacroReleaseParseError("NBS macro release feed contains duplicate URLs")
        seen_urls.add(url)
        published_at = _feed_published_at(
            _unique_child_text(item, "pubDate"),
            config=config,
        )
        pub_time = _unique_child_text(item, "pubTime")
        if _feed_published_at(pub_time, config=config) != published_at:
            raise NbsMacroReleaseIdentityError("NBS macro release feed publication fields disagree")
        indicator, reference_period = _title_identity(title)
        summary_nodes = item.findall("description")
        if len(summary_nodes) != 1:
            raise NbsMacroReleaseParseError("NBS macro release feed description is not unique")
        entries.append(
            _FeedEntry(
                title=title,
                url=url,
                published_at=published_at,
                indicator=indicator,
                reference_period=reference_period,
            )
        )
    if not entries:
        raise NbsMacroReleaseParseError("NBS macro release feed contains no entries")
    return tuple(entries)


def _validate_feed_window(
    entries: Sequence[_FeedEntry],
    *,
    config: NbsMacroReleaseSourceConfig,
    window_start: datetime,
) -> None:
    require_aware(window_start, "NBS macro release window_start")
    timezone = ZoneInfo(config.published_timezone)
    dates = tuple(item.published_at.astimezone(timezone).date() for item in entries)
    if any(left < right for left, right in pairwise(dates)):
        raise NbsMacroReleaseParseError(
            "NBS macro release feed is not ordered by publication date descending"
        )
    start_date = window_start.astimezone(timezone).date()
    if dates[-1] >= start_date:
        raise NbsMacroReleaseParseError("NBS macro release feed does not cover the query window")


def _latest_entries(
    entries: Sequence[_FeedEntry],
    *,
    indicators: tuple[str, ...],
    window_start: datetime,
    not_after: datetime | None = None,
) -> tuple[_FeedEntry, ...]:
    upper = datetime.max.replace(tzinfo=UTC) if not_after is None else not_after
    selected: list[_FeedEntry] = []
    for indicator in indicators:
        matches = tuple(
            item
            for item in entries
            if item.indicator == indicator
            and item.published_at >= window_start
            and item.published_at <= upper
        )
        if matches:
            selected.append(max(matches, key=lambda item: (item.published_at, item.url)))
    return tuple(selected)


class _NbsArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.article_titles: list[str] = []
        self.pub_dates: list[str] = []
        self.xlsx_hrefs: list[str] = []
        self.visible_text: list[str] = []
        self._hidden_depth = 0
        self._structural_stack: list[str] = []
        self._structural_starts = {"html": 0, "head": 0, "body": 0}
        self._structural_ends = {"html": 0, "head": 0, "body": 0}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        values = {key.casefold(): value for key, value in attrs}
        if tag in self._structural_starts:
            self._start_structural_tag(tag)
        if tag in {"script", "style"}:
            self._hidden_depth += 1
        if tag == "meta":
            name = values.get("name")
            content = values.get("content")
            if name == "ArticleTitle" and content is not None:
                self.article_titles.append(content)
            if name == "PubDate" and content is not None:
                self.pub_dates.append(content)
        if tag == "a":
            href = values.get("href")
            if href is not None and urlsplit(href).path.casefold().endswith(".xlsx"):
                self.xlsx_hrefs.append(href)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag in self._structural_ends:
            if not self._structural_stack or self._structural_stack[-1] != tag:
                raise NbsMacroReleaseParseError(
                    "NBS macro release article has unbalanced structural HTML"
                )
            self._structural_stack.pop()
            self._structural_ends[tag] += 1
        if tag in {"script", "style"} and self._hidden_depth:
            self._hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._hidden_depth:
            text = _clean_text(data)
            if text:
                self.visible_text.append(text)

    def assert_complete_document(self) -> None:
        if (
            self._structural_stack
            or self._structural_starts != {"html": 1, "head": 1, "body": 1}
            or self._structural_ends != {"html": 1, "head": 1, "body": 1}
            or self._hidden_depth
        ):
            raise NbsMacroReleaseParseError(
                "NBS macro release article must be a complete balanced HTML document"
            )

    def _start_structural_tag(self, tag: str) -> None:
        if self._structural_starts[tag]:
            raise NbsMacroReleaseParseError(
                "NBS macro release article has duplicate structural HTML"
            )
        expected_parent = {"html": [], "head": ["html"], "body": ["html"]}[tag]
        if self._structural_stack != expected_parent:
            raise NbsMacroReleaseParseError(
                "NBS macro release article has invalid structural HTML nesting"
            )
        if tag == "head" and self._structural_starts["body"]:
            raise NbsMacroReleaseParseError("NBS macro release article head cannot follow its body")
        self._structural_starts[tag] += 1
        self._structural_stack.append(tag)


def _parse_article(
    body: bytes,
    *,
    entry: _FeedEntry,
    config: NbsMacroReleaseSourceConfig,
) -> _ArticleData:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NbsMacroReleaseParseError("NBS macro release article is not UTF-8") from exc
    parser = _NbsArticleParser()
    try:
        parser.feed(text)
        parser.close()
        parser.assert_complete_document()
    except Exception as exc:
        raise NbsMacroReleaseParseError("NBS macro release article HTML is malformed") from exc
    if parser.article_titles != [entry.title]:
        raise NbsMacroReleaseIdentityError(
            "NBS macro release ArticleTitle does not uniquely match the RSS title"
        )
    if len(parser.pub_dates) != 1:
        raise NbsMacroReleaseIdentityError("NBS macro release PubDate is not unique")
    raw_pub_date = parser.pub_dates[0]
    try:
        local_published = datetime.strptime(raw_pub_date, "%Y/%m/%d %H:%M").replace(
            tzinfo=ZoneInfo(config.published_timezone)
        )
    except ValueError as exc:
        raise NbsMacroReleaseParseError("NBS macro release article PubDate is invalid") from exc
    visible = " ".join(parser.visible_text)
    if raw_pub_date not in visible:
        raise NbsMacroReleaseIdentityError(
            "NBS macro release article PubDate is not visibly corroborated"
        )
    published_at = local_published.astimezone(UTC)
    if published_at != entry.published_at.replace(second=0, microsecond=0):
        raise NbsMacroReleaseIdentityError(
            "NBS macro release RSS and article publication times do not match to the minute"
        )
    resolved = {urljoin(entry.url, href) for href in parser.xlsx_hrefs}
    if config.require_spreadsheet and not resolved:
        raise NbsMacroReleaseSpreadsheetMissingError(
            "NBS macro release article lacks its required spreadsheet"
        )
    if len(resolved) != 1:
        raise NbsMacroReleaseIdentityError(
            "NBS macro release article must resolve exactly one spreadsheet"
        )
    attachment_url = next(iter(resolved))
    _attachment_url(attachment_url, article_url=entry.url)
    return _ArticleData(
        title=entry.title,
        published_at=published_at,
        attachment_url=attachment_url,
    )


def _validate_xlsx(body: bytes) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as workbook:
            if any(item.flag_bits & 0x1 for item in workbook.infolist()):
                raise NbsMacroReleaseSpreadsheetError(
                    "NBS macro release spreadsheet contains encrypted entries"
                )
            names = {item.filename: item for item in workbook.infolist()}
            required = ("[Content_Types].xml", "xl/workbook.xml")
            if any(name not in names or names[name].file_size < 1 for name in required):
                raise NbsMacroReleaseSpreadsheetError(
                    "NBS macro release spreadsheet lacks required XLSX contents"
                )
            for name in required:
                if not workbook.read(name):
                    raise NbsMacroReleaseSpreadsheetError(
                        "NBS macro release spreadsheet has empty required XLSX contents"
                    )
    except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
        if isinstance(exc, NbsMacroReleaseSpreadsheetError):
            raise
        raise NbsMacroReleaseSpreadsheetError(
            "NBS macro release spreadsheet is not a valid XLSX archive"
        ) from exc


def _observations_from_capture(
    capture: NbsMacroReleaseCapture,
    *,
    config: NbsMacroReleaseSourceConfig,
    query: DataQuery,
    indicators: tuple[str, ...],
) -> tuple[tuple[SourceObservation, bytes], ...]:
    if capture.feed is None:
        raise NbsMacroReleaseParseError("NBS macro release capture lacks the RSS feed")
    _validate_exact_response(
        capture.feed,
        expected_url=config.feed_url,
        expected_content_type=_FEED_CONTENT_TYPE,
        kind="RSS feed",
    )
    entries = _parse_feed(capture.feed.body, config=config)
    window_start = query.window_start or query.as_of
    _validate_feed_window(entries, config=config, window_start=window_start)
    selected = _latest_entries(
        entries,
        indicators=indicators,
        window_start=window_start,
        not_after=query.as_of,
    )
    by_indicator = {item.indicator: item for item in capture.articles}
    if len(by_indicator) != len(capture.articles) or set(by_indicator) != {
        cast(str, item.indicator) for item in selected
    }:
        raise NbsMacroReleaseIdentityError(
            "NBS macro release captured articles do not match the requested latest releases"
        )
    observations: list[tuple[SourceObservation, bytes]] = []
    for entry in selected:
        assert entry.indicator is not None
        assert entry.reference_period is not None
        item = by_indicator[entry.indicator]
        if item.article.final_url != entry.url:
            raise NbsMacroReleaseIdentityError(
                "NBS macro release captured article URL does not match the RSS"
            )
        _validate_exact_response(
            item.article,
            expected_url=entry.url,
            expected_content_type=_ARTICLE_CONTENT_TYPE,
            kind="article",
        )
        article = _parse_article(item.article.body, entry=entry, config=config)
        _validate_exact_response(
            item.attachment,
            expected_url=article.attachment_url,
            expected_content_type=_XLSX_CONTENT_TYPE,
            kind="spreadsheet",
        )
        _validate_xlsx(item.attachment.body)
        if article.published_at > capture.retrieved_at:
            raise NbsMacroReleaseParseError(
                "NBS macro release publication time is after actual receipt"
            )
        raw_record = _article_raw_record(item)
        article_hash = sha256(item.article.body).hexdigest()
        attachment_hash = sha256(item.attachment.body).hexdigest()
        article_path = urlsplit(entry.url).path
        content_id = article_path.rsplit("/", 1)[-1].removesuffix(".html")
        observation = SourceObservation.build(
            capability=query.capability,
            provider_id=NBS_MACRO_RELEASE_PROVIDER_ID,
            provider_version=NBS_MACRO_RELEASE_PROVIDER_VERSION,
            upstream_source=config.source_id,
            upstream_record_id=content_id,
            source_ref=entry.url,
            lineage_id=f"{config.source_id}:{entry.indicator}:{entry.reference_period}",
            times=ObservationTimes(
                occurred_at=article.published_at,
                published_at=article.published_at,
                available_at=capture.retrieved_at,
                source_updated_at=None,
                aggregator_fetched_at=None,
                retrieved_at=capture.retrieved_at,
                occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
                availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
            ),
            authority_at=capture.retrieved_at,
            authority_kind="actual_receipt",
            raw_content_hash=sha256(raw_record).hexdigest(),
            normalized_payload={
                "record_type": "original_release",
                "indicator": entry.indicator,
                "reference_period": entry.reference_period,
                "release_title": entry.title,
                # RSS descriptions are discovery-only. A null value keeps that metadata from
                # becoming authoritative normalized content or changing release identity.
                "release_summary": None,
                "release_url": entry.url,
                "article_manifest": {
                    "url": entry.url,
                    "content_type": item.article.content_type,
                    "size_bytes": len(item.article.body),
                    "sha256": article_hash,
                },
                "attachments": [
                    {
                        "url": item.attachment.final_url,
                        "filename": urlsplit(item.attachment.final_url).path.rsplit("/", 1)[-1],
                        "content_type": item.attachment.content_type,
                        "size_bytes": len(item.attachment.body),
                        "sha256": attachment_hash,
                    }
                ],
                "publisher": config.publisher,
                "published_at": _timestamp(article.published_at),
                "revision_lineage": [],
                "rights": {
                    "rights_basis_url": config.rights_basis_url,
                    "rights_reviewed_at": _timestamp(config.rights_reviewed_at),
                    "license_scope": config.license_scope,
                    "content_scope": config.content_scope,
                    "redistribution_allowed": config.redistribution_allowed,
                },
            },
            license_scope=config.license_scope,
        )
        observations.append((observation, raw_record))
    return tuple(observations)


def _article_raw_record(item: NbsMacroReleaseArticleCapture) -> bytes:
    header = canonical_json_bytes(
        {
            "schema_version": "market-impact.nbs-macro-release-record.v1",
            "article_size": len(item.article.body),
            "article_sha256": sha256(item.article.body).hexdigest(),
            "attachment_size": len(item.attachment.body),
            "attachment_sha256": sha256(item.attachment.body).hexdigest(),
        }
    )
    return b"".join(
        (
            header,
            b"\n",
            item.article.body,
            b"\n",
            item.attachment.body,
        )
    )


def _capture_bundle(
    capture: NbsMacroReleaseCapture,
    *,
    config: NbsMacroReleaseSourceConfig,
) -> bytes:
    if capture.feed is None:
        raise NbsMacroReleaseParseError("NBS macro release capture lacks the RSS feed")
    components: list[tuple[dict[str, object], bytes]] = [
        (_component_header("feed", capture.feed), capture.feed.body)
    ]
    for item in capture.articles:
        components.append(
            (
                _component_header("article", item.article, indicator=item.indicator),
                item.article.body,
            )
        )
        components.append(
            (
                _component_header("attachment", item.attachment, indicator=item.indicator),
                item.attachment.body,
            )
        )
    parts = [
        f"{NBS_MACRO_RELEASE_CAPTURE_SCHEMA}\n".encode(),
        canonical_json_bytes(
            {
                "source_id": config.source_id,
                "source_config_id": config.source_config_id,
                "component_count": len(components),
            }
        ),
        b"\n",
    ]
    for header, body in components:
        parts.extend((canonical_json_bytes(header), b"\n", body, b"\n"))
    payload = b"".join(parts)
    if len(payload) > config.max_capture_bytes + 16_384:
        raise NbsMacroReleaseParseError("NBS macro release capture bundle exceeds byte limit")
    return payload


def _component_header(
    kind: str,
    response: NbsMacroReleaseHTTPResponse,
    *,
    indicator: str | None = None,
) -> dict[str, object]:
    return {
        "kind": kind,
        "indicator": indicator,
        "final_url": response.final_url,
        "content_type": response.content_type,
        "body_size": len(response.body),
        "body_sha256": sha256(response.body).hexdigest(),
    }


def _bundle_header(
    payload: bytes,
    offset: int,
    name: str,
) -> tuple[Mapping[str, object], int]:
    end = payload.find(b"\n", offset)
    if end < 0:
        raise NbsMacroReleaseParseError(f"NBS macro release {name} is truncated")
    try:
        header = _mapping(json.loads(payload[offset:end]), f"NBS macro release {name}")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NbsMacroReleaseParseError(f"NBS macro release {name} is invalid") from exc
    return header, end + 1


def _validate_exact_response(
    response: NbsMacroReleaseHTTPResponse,
    *,
    expected_url: str,
    expected_content_type: str,
    kind: str,
) -> None:
    if response.final_url != expected_url:
        raise NbsMacroReleaseIdentityError(f"NBS macro release {kind} redirect target drifted")
    if response.content_type.casefold().split(";", 1)[0].strip() != expected_content_type:
        raise NbsMacroReleaseIdentityError(f"NBS macro release {kind} content type drifted")


def _article_url(value: str) -> str:
    split = _official_url(value, "NBS macro release article URL")
    if _ARTICLE_PATH.fullmatch(split.path) is None:
        raise NbsMacroReleaseIdentityError("NBS macro release article path is not allowed")
    return value


def _attachment_url(value: str, *, article_url: str) -> str:
    split = _official_url(value, "NBS macro release attachment URL")
    article = urlsplit(article_url)
    if split.path.rsplit("/", 1)[0] != article.path.rsplit("/", 1)[0]:
        raise NbsMacroReleaseIdentityError(
            "NBS macro release spreadsheet is not in the article directory"
        )
    if _ATTACHMENT_NAME.fullmatch(split.path.rsplit("/", 1)[-1]) is None:
        raise NbsMacroReleaseIdentityError("NBS macro release spreadsheet path is not allowed")
    return value


def _official_url(value: str, name: str) -> SplitResult:
    split = urlsplit(value)
    if (
        split.scheme != "https"
        or split.hostname != "www.stats.gov.cn"
        or split.port is not None
        or not split.path.startswith("/")
        or split.query
        or split.fragment
        or split.username is not None
        or split.password is not None
    ):
        raise NbsMacroReleaseIdentityError(f"{name} must use the exact official HTTPS origin")
    return split


def _title_identity(title: str) -> tuple[str | None, str | None]:
    for indicator, pattern in _TITLE_PATTERNS.items():
        match = pattern.match(title)
        if match is not None:
            return indicator, f"{match.group('year')}-{int(match.group('month')):02d}"
    return None, None


def _feed_published_at(raw: str, *, config: NbsMacroReleaseSourceConfig) -> datetime:
    try:
        local = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=ZoneInfo(config.published_timezone)
        )
    except ValueError as exc:
        raise NbsMacroReleaseParseError(
            "NBS macro release feed publication time is invalid"
        ) from exc
    return local.astimezone(UTC)


def _query_indicators(
    config: NbsMacroReleaseSourceConfig,
    parameters: Mapping[str, object],
) -> tuple[str, ...]:
    unexpected = set(parameters) - {"indicators"}
    if unexpected:
        raise NbsMacroReleaseParseError(
            f"unsupported NBS macro release query parameters: {sorted(unexpected)}"
        )
    raw = parameters.get("indicators")
    if raw is None:
        return config.indicators
    if not isinstance(raw, list):
        raise NbsMacroReleaseParseError("NBS macro release indicators must be an array")
    requested = _indicator_tuple(tuple(cast(list[object], raw)), "query indicators")
    if not set(requested) <= set(config.indicators):
        raise NbsMacroReleaseParseError(
            "NBS macro release query indicators are outside the source config"
        )
    return tuple(item for item in config.indicators if item in requested)


def _indicator_tuple(value: tuple[object, ...], name: str) -> tuple[str, ...]:
    if not value or len(value) != len(set(value)):
        raise ValueError(f"{name} must be non-empty and unique")
    if any(not isinstance(item, str) or item not in _ALLOWED_INDICATORS for item in value):
        raise ValueError(f"{name} must contain only cpi and ppi")
    return cast(tuple[str, ...], value)


def _unique_child_text(parent: ElementTree.Element, tag: str) -> str:
    children = parent.findall(tag)
    if len(children) != 1 or children[0].text is None or not children[0].text.strip():
        raise NbsMacroReleaseParseError(
            f"NBS macro release feed field {tag} must be unique and non-empty"
        )
    return children[0].text.strip()


def _clean_text(value: str) -> str:
    return " ".join(value.replace("\u3000", " ").replace("\u2002", " ").split())


def _identifier(value: str, name: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} is invalid")
    return value


def _trimmed(value: str, name: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")
    return value


def _strict_utc(value: datetime, name: str) -> datetime:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")
    return value


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    return _strict_utc(clock(), "NBS macro release receipt clock")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object with string keys")
    untyped = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in untyped):
        raise ValueError(f"{name} must be an object with string keys")
    return cast(Mapping[str, object], untyped)


def _string(fields: Mapping[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a string array")
    untyped = cast(list[object], value)
    if any(not isinstance(item, str) for item in untyped):
        raise ValueError(f"{name} must be a string array")
    return tuple(cast(list[str], untyped))


def _integer(fields: Mapping[str, object], name: str) -> int:
    value = fields.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _boolean(fields: Mapping[str, object], name: str) -> bool:
    value = fields.get(name)
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a timestamp") from exc
    return _strict_utc(parsed, name)
