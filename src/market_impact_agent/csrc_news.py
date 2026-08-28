from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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

CSRC_NEWS_SOURCE_SCHEMA = "market-impact.csrc-news-source.v1"
CSRC_NEWS_PROVIDER_ID = "csrc-official-news"
CSRC_NEWS_PROVIDER_VERSION = "1"
CSRC_NEWS_CONTENT_SCOPE = "official_publication_private_research"


class CsrcNewsError(RuntimeError):
    error_kind = "csrc_news_error"


class CsrcNewsHTTPError(CsrcNewsError):
    error_kind = "http_error"

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"CSRC news route returned HTTP {status_code}")


class CsrcNewsNetworkError(CsrcNewsError):
    error_kind = "network_error"


class CsrcNewsIdentityError(CsrcNewsError):
    error_kind = "source_identity_mismatch"


class CsrcNewsParseError(CsrcNewsError):
    error_kind = "source_parse_error"


@dataclass(frozen=True, slots=True)
class CsrcNewsSourceConfig:
    source_config_id: str
    source_id: str
    endpoint_url: str
    channel_id: str
    publisher: str
    published_timezone: str
    page_size: int
    maximum_pages: int
    rights_basis_url: str
    rights_reviewed_at: datetime
    license_scope: str
    content_scope: str = CSRC_NEWS_CONTENT_SCOPE
    redistribution_allowed: bool = False
    schema_version: str = CSRC_NEWS_SOURCE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != CSRC_NEWS_SOURCE_SCHEMA:
            raise ValueError("unsupported CSRC news source schema_version")
        _identifier(self.source_id, "CSRC news source_id")
        _trimmed(self.channel_id, "CSRC news channel_id")
        _csrc_https_url(self.endpoint_url, "CSRC news endpoint_url")
        if urlsplit(self.endpoint_url).path.rstrip("/").rsplit("/", 1)[-1] != self.channel_id:
            raise ValueError("CSRC news endpoint_url must bind the declared channel_id")
        _trimmed(self.publisher, "CSRC news publisher")
        try:
            ZoneInfo(self.published_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("CSRC news published_timezone must be an IANA timezone") from exc
        if not 1 <= self.page_size <= 100:
            raise ValueError("CSRC news page_size must be between 1 and 100")
        if not 1 <= self.maximum_pages <= 100:
            raise ValueError("CSRC news maximum_pages must be between 1 and 100")
        _csrc_https_url(self.rights_basis_url, "CSRC news rights_basis_url")
        _strict_utc(self.rights_reviewed_at, "CSRC news rights_reviewed_at")
        _trimmed(self.license_scope, "CSRC news license_scope")
        if self.content_scope != CSRC_NEWS_CONTENT_SCOPE:
            raise ValueError(f"CSRC news content_scope must be {CSRC_NEWS_CONTENT_SCOPE}")
        if self.redistribution_allowed:
            raise ValueError("CSRC news source cannot declare redistribution permission")
        if self.source_config_id != self.expected_source_config_id:
            raise ValueError("CSRC news source_config_id does not match content")

    @property
    def expected_source_config_id(self) -> str:
        return f"csrc-news-source-{canonical_hash(self.core_dict())}"

    @property
    def artifact_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "endpoint_url": self.endpoint_url,
            "channel_id": self.channel_id,
            "publisher": self.publisher,
            "published_timezone": self.published_timezone,
            "page_size": self.page_size,
            "maximum_pages": self.maximum_pages,
            "rights_basis_url": self.rights_basis_url,
            "rights_reviewed_at": _timestamp(self.rights_reviewed_at),
            "license_scope": self.license_scope,
            "content_scope": self.content_scope,
            "redistribution_allowed": self.redistribution_allowed,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "source_config_id": self.source_config_id}

    @classmethod
    def build(
        cls,
        *,
        source_id: str,
        endpoint_url: str,
        channel_id: str,
        publisher: str,
        published_timezone: str,
        page_size: int,
        maximum_pages: int,
        rights_basis_url: str,
        rights_reviewed_at: datetime,
        license_scope: str,
    ) -> CsrcNewsSourceConfig:
        core = {
            "schema_version": CSRC_NEWS_SOURCE_SCHEMA,
            "source_id": source_id,
            "endpoint_url": endpoint_url,
            "channel_id": channel_id,
            "publisher": publisher,
            "published_timezone": published_timezone,
            "page_size": page_size,
            "maximum_pages": maximum_pages,
            "rights_basis_url": rights_basis_url,
            "rights_reviewed_at": _timestamp(rights_reviewed_at),
            "license_scope": license_scope,
            "content_scope": CSRC_NEWS_CONTENT_SCOPE,
            "redistribution_allowed": False,
        }
        return cls(
            source_config_id=f"csrc-news-source-{canonical_hash(core)}",
            source_id=source_id,
            endpoint_url=endpoint_url,
            channel_id=channel_id,
            publisher=publisher,
            published_timezone=published_timezone,
            page_size=page_size,
            maximum_pages=maximum_pages,
            rights_basis_url=rights_basis_url,
            rights_reviewed_at=rights_reviewed_at,
            license_scope=license_scope,
        )


@dataclass(frozen=True, slots=True)
class CsrcNewsHTTPResponse:
    body: bytes
    final_url: str
    content_type: str

    def __post_init__(self) -> None:
        if not self.body:
            raise CsrcNewsParseError("CSRC news response is empty")
        _csrc_https_url(self.final_url, "CSRC news final_url")
        _trimmed(self.content_type, "CSRC news content_type")


class CsrcNewsHTTPClient(Protocol):
    def get(self, url: str, *, max_response_bytes: int) -> CsrcNewsHTTPResponse: ...


class UrllibCsrcNewsHTTPClient:
    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("CSRC news timeout_seconds must be positive")
        self.timeout_seconds = timeout_seconds

    def get(self, url: str, *, max_response_bytes: int) -> CsrcNewsHTTPResponse:
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "market-impact-agent/0.1 (+private-research)",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read(max_response_bytes + 1)
                if len(body) > max_response_bytes:
                    raise CsrcNewsParseError("CSRC news response exceeds byte limit")
                return CsrcNewsHTTPResponse(
                    body=body,
                    final_url=response.geturl(),
                    content_type=response.headers.get_content_type(),
                )
        except HTTPError as exc:
            raise CsrcNewsHTTPError(exc.code) from exc
        except URLError as exc:
            raise CsrcNewsNetworkError(str(exc.reason)) from exc


@dataclass(frozen=True, slots=True)
class CsrcNewsPageCapture:
    page: int
    request_url: str
    response: CsrcNewsHTTPResponse


@dataclass(frozen=True, slots=True)
class CsrcNewsCapture:
    source_id: str
    retrieved_at: datetime
    pages: tuple[CsrcNewsPageCapture, ...]
    coverage_complete: bool
    failure_status: DataFetchStatus | None = None
    error_kind: str | None = None

    def __post_init__(self) -> None:
        _strict_utc(self.retrieved_at, "CSRC news capture retrieved_at")
        if self.coverage_complete:
            if not self.pages:
                raise ValueError("complete CSRC news capture requires at least one page")
            if self.failure_status is not None or self.error_kind is not None:
                raise ValueError("complete CSRC news capture cannot carry a failure")
        else:
            if self.failure_status is None or self.error_kind is None:
                raise ValueError("incomplete CSRC news capture requires a typed failure")


@dataclass(frozen=True, slots=True)
class _PageData:
    page: int
    total: int
    records: tuple[tuple[Mapping[str, object], bytes], ...]


@dataclass(frozen=True, slots=True)
class _QueryParameters:
    keywords: tuple[str, ...]
    max_items: int


class CsrcNewsProvider(DataProvider):
    def __init__(
        self,
        source_configs: tuple[CsrcNewsSourceConfig, ...],
        *,
        http_client: CsrcNewsHTTPClient | None = None,
        clock: Callable[[], datetime] | None = None,
        max_response_bytes: int = 2_000_000,
    ) -> None:
        if not source_configs:
            raise ValueError("CSRC news provider requires at least one source")
        if max_response_bytes < 1:
            raise ValueError("CSRC news max_response_bytes must be positive")
        sources = {item.source_id: item for item in source_configs}
        if len(sources) != len(source_configs):
            raise ValueError("CSRC news source IDs must be unique")
        self._sources = sources
        self._http_client = UrllibCsrcNewsHTTPClient() if http_client is None else http_client
        self._clock = (lambda: datetime.now(UTC)) if clock is None else clock
        self._max_response_bytes = max_response_bytes
        self._manifest = ObservationProviderManifest(
            schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
            provider_id=CSRC_NEWS_PROVIDER_ID,
            provider_version=CSRC_NEWS_PROVIDER_VERSION,
            transport=ProviderTransport.HTTP,
            declared_capabilities=frozenset({ObservationCapability.EVENT_REVELATION}),
            verified_capabilities=frozenset({ObservationCapability.EVENT_REVELATION}),
            upstream_sources=tuple(item.source_id for item in source_configs),
            auth_required=False,
            provides_source_updated_at=False,
            provides_aggregator_fetched_at=False,
            provides_historical_occurrence_at=False,
            provides_revision_history=False,
            enabled=True,
            trust_tier=ObservationTrustTier.CONTRACT_VALIDATED,
            license_note=(
                "Official CSRC publication responses may be retained only in private research "
                "storage under the source-specific legal review; redistribution, historical "
                "availability, and execution rights are not inferred."
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
    ) -> tuple[CsrcNewsCapture, ...]:
        require_aware(window_start, "CSRC news collection window_start")
        parsed_parameters = _query_parameters(parameters)
        captures: list[CsrcNewsCapture] = []
        for config in self._sources.values():
            captures.append(
                await self._collect_one(
                    config,
                    window_start=window_start.astimezone(UTC),
                    parameters=parsed_parameters,
                )
            )
        return tuple(captures)

    def replay(self, captures: tuple[CsrcNewsCapture, ...]) -> _CapturedCsrcNewsProvider:
        return _CapturedCsrcNewsProvider(self, captures)

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
        capture = await self._collect_one(
            config,
            window_start=query.window_start or query.as_of,
            parameters=_query_parameters(query.parameters),
        )
        return self.response_from_capture(query=query, source=source, capture=capture)

    async def _collect_one(
        self,
        config: CsrcNewsSourceConfig,
        *,
        window_start: datetime,
        parameters: _QueryParameters,
    ) -> CsrcNewsCapture:
        del parameters
        pages: list[CsrcNewsPageCapture] = []
        prior_oldest_date: date | None = None
        expected_total: int | None = None
        fetched_count = 0
        window_start_date = window_start.astimezone(ZoneInfo(config.published_timezone)).date()
        try:
            for page_number in range(1, config.maximum_pages + 1):
                request_url = _page_url(config, page_number)
                response = await asyncio.to_thread(
                    self._http_client.get,
                    request_url,
                    max_response_bytes=self._max_response_bytes,
                )
                if response.final_url != request_url:
                    raise CsrcNewsIdentityError(
                        "CSRC news redirect target does not match the requested page"
                    )
                if response.content_type.casefold() != "application/json":
                    raise CsrcNewsParseError(
                        f"unsupported CSRC news content type: {response.content_type}"
                    )
                page = _parse_page(response.body, config=config, expected_page=page_number)
                if expected_total is None:
                    expected_total = page.total
                elif page.total != expected_total:
                    raise CsrcNewsParseError("CSRC news pagination total changed during capture")
                fetched_count += len(page.records)
                if fetched_count > expected_total:
                    raise CsrcNewsParseError("CSRC news pagination exceeds declared total")
                if not page.records and fetched_count != expected_total:
                    raise CsrcNewsParseError(
                        "CSRC news returned an empty page before the declared total was covered"
                    )
                pages.append(
                    CsrcNewsPageCapture(
                        page=page_number,
                        request_url=request_url,
                        response=response,
                    )
                )
                published = tuple(
                    _published_at(record, config=config) for record, _raw in page.records
                )
                publication_dates = tuple(
                    item.astimezone(ZoneInfo(config.published_timezone)).date()
                    for item in published
                )
                if any(left < right for left, right in pairwise(publication_dates)):
                    raise CsrcNewsParseError(
                        "CSRC news results are not ordered by publication date descending"
                    )
                if (
                    prior_oldest_date is not None
                    and publication_dates
                    and publication_dates[0] > prior_oldest_date
                ):
                    raise CsrcNewsParseError(
                        "CSRC news pagination order is not publication-date descending"
                    )
                if publication_dates:
                    prior_oldest_date = publication_dates[-1]
                if fetched_count == expected_total or (
                    publication_dates and publication_dates[-1] < window_start_date
                ):
                    return CsrcNewsCapture(
                        source_id=config.source_id,
                        retrieved_at=_utc_now(self._clock),
                        pages=tuple(pages),
                        coverage_complete=True,
                    )
            return CsrcNewsCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind="pagination_limit_exceeded",
            )
        except CsrcNewsHTTPError as exc:
            return CsrcNewsCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=(
                    DataFetchStatus.RATE_LIMITED
                    if exc.status_code == 429
                    else DataFetchStatus.ERROR
                ),
                error_kind=f"http_{exc.status_code}",
            )
        except CsrcNewsError as exc:
            return CsrcNewsCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        except Exception as exc:
            return CsrcNewsCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind=type(exc).__name__,
            )

    def response_from_capture(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
        capture: CsrcNewsCapture,
    ) -> ProviderDataResponse:
        config = self._sources.get(source.upstream_source)
        if config is None or capture.source_id != source.upstream_source:
            return self._failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_config_missing",
            )
        if query.capability is not ObservationCapability.EVENT_REVELATION:
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
            parameters = _query_parameters(query.parameters)
            parsed = tuple(
                _parse_page(item.response.body, config=config, expected_page=item.page)
                for item in capture.pages
            )
            observations = _parse_observations(
                parsed,
                config=config,
                query=query,
                retrieved_at=capture.retrieved_at,
                parameters=parameters,
            )
            raw_payload = _capture_bundle(capture.pages)
        except CsrcNewsError as exc:
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


class _CapturedCsrcNewsProvider(DataProvider):
    def __init__(
        self,
        provider: CsrcNewsProvider,
        captures: tuple[CsrcNewsCapture, ...],
    ) -> None:
        capture_by_source = {item.source_id: item for item in captures}
        if len(capture_by_source) != len(captures) or set(capture_by_source) != set(
            provider.manifest.upstream_sources
        ):
            raise ValueError("CSRC news captures must cover each registered source exactly once")
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


def load_csrc_news_source(path: Path) -> CsrcNewsSourceConfig:
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    errors = validate_agent_contract(payload, "csrc-news-source.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    fields = _mapping(payload, "CSRC news source")
    config = CsrcNewsSourceConfig(
        schema_version=_string(fields, "schema_version"),
        source_config_id=_string(fields, "source_config_id"),
        source_id=_string(fields, "source_id"),
        endpoint_url=_string(fields, "endpoint_url"),
        channel_id=_string(fields, "channel_id"),
        publisher=_string(fields, "publisher"),
        published_timezone=_string(fields, "published_timezone"),
        page_size=_integer(fields, "page_size"),
        maximum_pages=_integer(fields, "maximum_pages"),
        rights_basis_url=_string(fields, "rights_basis_url"),
        rights_reviewed_at=_datetime(fields.get("rights_reviewed_at"), "rights_reviewed_at"),
        license_scope=_string(fields, "license_scope"),
        content_scope=_string(fields, "content_scope"),
        redistribution_allowed=_boolean(fields, "redistribution_allowed"),
    )
    if config.to_dict() != fields:
        raise ValueError("CSRC news source is not canonical")
    return config


def _page_url(config: CsrcNewsSourceConfig, page: int) -> str:
    query = urlencode(
        (
            ("_isAgg", "true"),
            ("_isJson", "true"),
            ("_pageSize", str(config.page_size)),
            ("_template", "index"),
            ("_rangeTimeGte", ""),
            ("_channelName", ""),
            ("page", str(page)),
        )
    )
    return f"{config.endpoint_url}?{query}"


def _parse_page(
    body: bytes,
    *,
    config: CsrcNewsSourceConfig,
    expected_page: int,
) -> _PageData:
    try:
        text = body.decode("utf-8")
        payload = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CsrcNewsParseError("CSRC news response is not valid UTF-8 JSON") from exc
    root = _mapping(payload, "CSRC news response")
    data = _mapping(root.get("data"), "CSRC news data")
    page = _integer(data, "page")
    rows = _integer(data, "rows")
    channel_id = _string(data, "channelId")
    total = _integer(data, "total")
    results = _sequence(data.get("results"), "CSRC news results")
    if page != expected_page:
        raise CsrcNewsIdentityError("CSRC news response page does not match request")
    if rows != config.page_size:
        raise CsrcNewsIdentityError("CSRC news response page size does not match source config")
    if channel_id != config.channel_id:
        raise CsrcNewsIdentityError("CSRC news response channel does not match source config")
    if total < 0 or len(results) > config.page_size:
        raise CsrcNewsParseError("CSRC news result counts are invalid")
    raw_records = _exact_result_records(text, results)
    records: list[tuple[Mapping[str, object], bytes]] = []
    for item, raw in zip(results, raw_records, strict=True):
        record = _mapping(item, "CSRC news result")
        if _string(record, "channelId") != config.channel_id:
            raise CsrcNewsIdentityError("CSRC news result channel does not match source config")
        records.append((record, raw))
    return _PageData(page=page, total=total, records=tuple(records))


def _exact_result_records(text: str, results: Sequence[object]) -> tuple[bytes, ...]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r'"results"\s*:\s*\[', text):
        position = match.end()
        decoded: list[object] = []
        records: list[bytes] = []
        while True:
            while position < len(text) and text[position].isspace():
                position += 1
            if position < len(text) and text[position] == "]":
                break
            try:
                value, end = decoder.raw_decode(text, position)
            except json.JSONDecodeError:
                break
            decoded.append(value)
            records.append(text[position:end].encode())
            position = end
            while position < len(text) and text[position].isspace():
                position += 1
            if position < len(text) and text[position] == ",":
                position += 1
                continue
            if position < len(text) and text[position] == "]":
                break
            decoded = []
            break
        if decoded == list(results):
            return tuple(records)
    raise CsrcNewsParseError("CSRC news exact result records cannot be recovered")


def _parse_observations(
    pages: tuple[_PageData, ...],
    *,
    config: CsrcNewsSourceConfig,
    query: DataQuery,
    retrieved_at: datetime,
    parameters: _QueryParameters,
) -> tuple[tuple[SourceObservation, bytes], ...]:
    observations: list[tuple[SourceObservation, bytes]] = []
    seen_refs: set[str] = set()
    for page in pages:
        for record, raw_record in page.records:
            title = _string(record, "title")
            summary = _optional_string(record, "memo") or ""
            published_at = _published_at(record, config=config)
            if published_at > retrieved_at:
                raise CsrcNewsParseError("CSRC news publication time is after actual receipt")
            if query.window_start is not None and published_at < query.window_start:
                continue
            if published_at > query.as_of:
                continue
            if parameters.keywords and not _keyword_match(
                parameters.keywords,
                title=title,
                summary=summary,
            ):
                continue
            source_ref = _source_ref(_string(record, "url"))
            if source_ref in seen_refs:
                raise CsrcNewsParseError("CSRC news response contains duplicate publication URLs")
            seen_refs.add(source_ref)
            upstream_record_id = _content_id(source_ref)
            observation = SourceObservation.build(
                capability=ObservationCapability.EVENT_REVELATION,
                provider_id=CSRC_NEWS_PROVIDER_ID,
                provider_version=CSRC_NEWS_PROVIDER_VERSION,
                upstream_source=config.source_id,
                upstream_record_id=upstream_record_id,
                source_ref=source_ref,
                lineage_id=f"{config.source_id}:{upstream_record_id}",
                times=ObservationTimes(
                    occurred_at=retrieved_at,
                    published_at=published_at,
                    available_at=retrieved_at,
                    source_updated_at=None,
                    aggregator_fetched_at=None,
                    retrieved_at=retrieved_at,
                    occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
                    availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
                ),
                authority_at=retrieved_at,
                authority_kind="actual_receipt",
                raw_content_hash=sha256(raw_record).hexdigest(),
                normalized_payload={
                    "publisher": config.publisher,
                    "headline": title,
                    "summary": summary,
                    "url": source_ref,
                    "published_at": _timestamp(published_at),
                    "channel_id": config.channel_id,
                    "content_scope": config.content_scope,
                    "rights_basis_url": config.rights_basis_url,
                },
                license_scope=config.license_scope,
            )
            observations.append((observation, raw_record))
            if len(observations) >= parameters.max_items:
                return tuple(observations)
    return tuple(observations)


def _capture_bundle(pages: tuple[CsrcNewsPageCapture, ...]) -> bytes:
    parts = [b"market-impact.csrc-news-capture.v1\n"]
    for item in pages:
        header = canonical_json_bytes(
            {
                "page": item.page,
                "request_url": item.request_url,
                "final_url": item.response.final_url,
                "content_type": item.response.content_type,
                "body_size": len(item.response.body),
                "body_sha256": sha256(item.response.body).hexdigest(),
            }
        )
        parts.extend((header, b"\n", item.response.body, b"\n"))
    return b"".join(parts)


def load_csrc_news_capture_bundle(
    payload: bytes,
    *,
    config: CsrcNewsSourceConfig,
    retrieved_at: datetime,
) -> CsrcNewsCapture:
    """Reconstruct one complete capture from the exact stored response bundle."""

    _strict_utc(retrieved_at, "CSRC news capture bundle retrieved_at")
    prefix = b"market-impact.csrc-news-capture.v1\n"
    if not payload.startswith(prefix):
        raise CsrcNewsParseError("CSRC news capture bundle has an invalid header")
    offset = len(prefix)
    pages: list[CsrcNewsPageCapture] = []
    while offset < len(payload):
        header_end = payload.find(b"\n", offset)
        if header_end < 0:
            raise CsrcNewsParseError("CSRC news capture bundle header is truncated")
        try:
            header = _mapping(
                json.loads(payload[offset:header_end]),
                "CSRC news capture bundle page header",
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CsrcNewsParseError("CSRC news capture bundle header is invalid") from exc
        page = _integer(header, "page")
        if page != len(pages) + 1:
            raise CsrcNewsIdentityError("CSRC news capture bundle pages are not contiguous")
        expected_url = _page_url(config, page)
        if _string(header, "request_url") != expected_url:
            raise CsrcNewsIdentityError("CSRC news capture bundle request URL mismatch")
        if _string(header, "final_url") != expected_url:
            raise CsrcNewsIdentityError("CSRC news capture bundle final URL mismatch")
        body_size = _integer(header, "body_size")
        if body_size < 1:
            raise CsrcNewsParseError("CSRC news capture bundle body size is invalid")
        body_start = header_end + 1
        body_end = body_start + body_size
        if body_end >= len(payload) or payload[body_end : body_end + 1] != b"\n":
            raise CsrcNewsParseError("CSRC news capture bundle body is truncated")
        body = payload[body_start:body_end]
        if sha256(body).hexdigest() != _string(header, "body_sha256"):
            raise CsrcNewsParseError("CSRC news capture bundle body hash mismatch")
        response = CsrcNewsHTTPResponse(
            body=body,
            final_url=expected_url,
            content_type=_string(header, "content_type"),
        )
        _parse_page(body, config=config, expected_page=page)
        pages.append(
            CsrcNewsPageCapture(
                page=page,
                request_url=expected_url,
                response=response,
            )
        )
        offset = body_end + 1
    if not pages:
        raise CsrcNewsParseError("CSRC news capture bundle contains no pages")
    return CsrcNewsCapture(
        source_id=config.source_id,
        retrieved_at=retrieved_at,
        pages=tuple(pages),
        coverage_complete=True,
    )


def _published_at(record: Mapping[str, object], *, config: CsrcNewsSourceConfig) -> datetime:
    raw = _string(record, "publishedTimeStr")
    try:
        local = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=ZoneInfo(config.published_timezone)
        )
    except ValueError as exc:
        raise CsrcNewsParseError("CSRC news publication time is invalid") from exc
    return local.astimezone(UTC)


def _source_ref(raw: str) -> str:
    resolved = urljoin("https://www.csrc.gov.cn/", raw)
    _csrc_https_url(resolved, "CSRC news publication URL")
    return resolved


def _content_id(source_ref: str) -> str:
    match = re.search(r"/([^/]+)/content\.shtml$", urlsplit(source_ref).path)
    if match is None:
        raise CsrcNewsParseError("CSRC news publication URL lacks a stable content ID")
    return match.group(1)


def _query_parameters(value: Mapping[str, object]) -> _QueryParameters:
    unexpected = set(value) - {"keywords", "max_items"}
    if unexpected:
        raise CsrcNewsParseError(f"unsupported CSRC news query parameters: {sorted(unexpected)}")
    raw_keywords = value.get("keywords", [])
    if not isinstance(raw_keywords, list):
        raise CsrcNewsParseError("CSRC news keywords must be an array")
    keywords: list[str] = []
    for item in cast(list[object], raw_keywords):
        if not isinstance(item, str) or not item.strip():
            raise CsrcNewsParseError("CSRC news keywords must be non-empty strings")
        keywords.append(item.strip().casefold())
    raw_max_items = value.get("max_items", 50)
    if isinstance(raw_max_items, bool) or not isinstance(raw_max_items, int):
        raise CsrcNewsParseError("CSRC news max_items must be an integer")
    if not 1 <= raw_max_items <= 100:
        raise CsrcNewsParseError("CSRC news max_items must be between 1 and 100")
    return _QueryParameters(keywords=tuple(keywords), max_items=raw_max_items)


def _keyword_match(keywords: tuple[str, ...], *, title: str, summary: str) -> bool:
    haystack = f"{title}\n{summary}".casefold()
    return any(item in haystack for item in keywords)


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    require_aware(value, "CSRC news clock")
    return value.astimezone(UTC)


def _csrc_https_url(value: str, name: str) -> None:
    _trimmed(value, name)
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.hostname != "www.csrc.gov.cn":
        raise ValueError(f"{name} must use https://www.csrc.gov.cn")
    if parsed.username is not None or parsed.password is not None or not parsed.path:
        raise ValueError(f"{name} must be a public path without credentials")


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} must use lowercase letters, digits, dot, dash, or underscore")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise CsrcNewsParseError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise CsrcNewsParseError(f"{name} must be an object")
    return cast(Mapping[str, object], raw)


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise CsrcNewsParseError(f"{name} must be an array")
    return cast(list[object], value)


def _string(value: Mapping[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise CsrcNewsParseError(f"{key} must be a non-empty string")
    return result.strip()


def _optional_string(value: Mapping[str, object], key: str) -> str | None:
    result = value.get(key)
    if result is None:
        return None
    if not isinstance(result, str):
        raise CsrcNewsParseError(f"{key} must be a string or null")
    stripped = result.strip()
    return stripped or None


def _integer(value: Mapping[str, object], key: str) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int):
        raise CsrcNewsParseError(f"{key} must be an integer")
    return result


def _boolean(value: Mapping[str, object], key: str) -> bool:
    result = value.get(key)
    if not isinstance(result, bool):
        raise ValueError(f"{key} must be a boolean")
    return result


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO 8601 string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    _strict_utc(result, name)
    return result
