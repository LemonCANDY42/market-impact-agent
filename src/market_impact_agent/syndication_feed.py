from __future__ import annotations

import asyncio
import importlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from xml.parsers import expat

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataProvider,
    DataQuery,
    DataSourceBinding,
    ProviderDataResponse,
    SourceObservation,
)
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

SYNDICATION_FEED_SOURCE_SCHEMA = "market-impact.syndication-feed-source.v1"
SYNDICATION_FEED_PROVIDER_ID = "http-syndication-feed"
SYNDICATION_FEED_PROVIDER_VERSION = "1"

_ALLOWED_CONTENT_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/rss+xml",
        "application/xml",
        "text/xml",
    }
)


class SyndicationFeedError(RuntimeError):
    error_kind = "syndication_feed_error"


class SyndicationFeedHTTPError(SyndicationFeedError):
    error_kind = "http_error"

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"syndication feed returned HTTP {status_code}")


class SyndicationFeedNetworkError(SyndicationFeedError):
    error_kind = "network_error"


class SyndicationFeedIdentityError(SyndicationFeedError):
    error_kind = "source_identity_mismatch"


class SyndicationFeedParseError(SyndicationFeedError):
    error_kind = "feed_parse_error"


@dataclass(frozen=True, slots=True)
class SyndicationFeedSourceConfig:
    source_config_id: str
    source_id: str
    request_url: str
    expected_final_url: str
    publisher: str
    license_scope: str
    content_scope: str = "metadata_and_feed_excerpt"
    schema_version: str = SYNDICATION_FEED_SOURCE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != SYNDICATION_FEED_SOURCE_SCHEMA:
            raise ValueError("unsupported syndication feed source schema_version")
        _identifier(self.source_id, "syndication feed source_id")
        _https_url(self.request_url, "syndication feed request_url")
        _https_url(self.expected_final_url, "syndication feed expected_final_url")
        _trimmed(self.publisher, "syndication feed publisher")
        _trimmed(self.license_scope, "syndication feed license_scope")
        if self.content_scope != "metadata_and_feed_excerpt":
            raise ValueError("syndication feed content_scope must be metadata_and_feed_excerpt")
        if self.source_config_id != self.expected_source_config_id:
            raise ValueError("syndication feed source_config_id does not match content")

    @property
    def expected_source_config_id(self) -> str:
        return f"syndication-feed-source-{canonical_hash(self.core_dict())}"

    @property
    def artifact_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "request_url": self.request_url,
            "expected_final_url": self.expected_final_url,
            "publisher": self.publisher,
            "license_scope": self.license_scope,
            "content_scope": self.content_scope,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "source_config_id": self.source_config_id}

    @classmethod
    def build(
        cls,
        *,
        source_id: str,
        request_url: str,
        expected_final_url: str,
        publisher: str,
        license_scope: str,
    ) -> SyndicationFeedSourceConfig:
        core = {
            "schema_version": SYNDICATION_FEED_SOURCE_SCHEMA,
            "source_id": source_id,
            "request_url": request_url,
            "expected_final_url": expected_final_url,
            "publisher": publisher,
            "license_scope": license_scope,
            "content_scope": "metadata_and_feed_excerpt",
        }
        return cls(
            source_config_id=f"syndication-feed-source-{canonical_hash(core)}",
            source_id=source_id,
            request_url=request_url,
            expected_final_url=expected_final_url,
            publisher=publisher,
            license_scope=license_scope,
        )


@dataclass(frozen=True, slots=True)
class SyndicationHTTPResponse:
    body: bytes
    final_url: str
    content_type: str

    def __post_init__(self) -> None:
        _trimmed(self.final_url, "syndication HTTP response final_url")
        _trimmed(self.content_type, "syndication HTTP response content_type")


@dataclass(frozen=True, slots=True)
class SyndicationFeedCapture:
    source_id: str
    retrieved_at: datetime
    response: SyndicationHTTPResponse | None
    failure_status: DataFetchStatus | None = None
    error_kind: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.source_id, "syndication feed capture source_id")
        _utc_now(lambda: self.retrieved_at)
        if self.response is None:
            if self.failure_status is None or self.failure_status.completed:
                raise ValueError("failed syndication feed capture requires a failed status")
            _trimmed(self.error_kind or "", "failed syndication feed capture error_kind")
        elif self.failure_status is not None or self.error_kind is not None:
            raise ValueError("successful syndication feed capture cannot carry failure details")


class SyndicationHTTPClient(Protocol):
    def get(self, url: str, *, max_response_bytes: int) -> SyndicationHTTPResponse: ...


@dataclass(frozen=True, slots=True)
class UrllibSyndicationHTTPClient:
    timeout_seconds: float = 20.0
    user_agent: str = "market-impact-agent-syndication/0.1"

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("syndication HTTP timeout must be positive")
        _trimmed(self.user_agent, "syndication HTTP user_agent")

    def get(self, url: str, *, max_response_bytes: int) -> SyndicationHTTPResponse:
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        request = Request(url, headers={"User-Agent": self.user_agent})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read(max_response_bytes + 1)
                if len(body) > max_response_bytes:
                    raise SyndicationFeedParseError("syndication feed exceeds byte limit")
                return SyndicationHTTPResponse(
                    body=body,
                    final_url=response.geturl(),
                    content_type=response.headers.get_content_type(),
                )
        except HTTPError as exc:
            raise SyndicationFeedHTTPError(exc.code) from exc
        except URLError as exc:
            raise SyndicationFeedNetworkError(str(exc.reason)) from exc


class SyndicationFeedProvider(DataProvider):
    def __init__(
        self,
        source_configs: tuple[SyndicationFeedSourceConfig, ...],
        *,
        http_client: SyndicationHTTPClient | None = None,
        clock: Callable[[], datetime] | None = None,
        max_response_bytes: int = 2_000_000,
    ) -> None:
        if not source_configs:
            raise ValueError("syndication feed provider requires at least one source")
        if max_response_bytes < 1:
            raise ValueError("syndication feed max_response_bytes must be positive")
        sources = {item.source_id: item for item in source_configs}
        if len(sources) != len(source_configs):
            raise ValueError("syndication feed source IDs must be unique")
        self._sources = sources
        self._http_client = UrllibSyndicationHTTPClient() if http_client is None else http_client
        self._clock = (lambda: datetime.now(UTC)) if clock is None else clock
        self._max_response_bytes = max_response_bytes
        self._manifest = ObservationProviderManifest(
            schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
            provider_id=SYNDICATION_FEED_PROVIDER_ID,
            provider_version=SYNDICATION_FEED_PROVIDER_VERSION,
            transport=ProviderTransport.HTTP,
            declared_capabilities=frozenset({ObservationCapability.EVENT_REVELATION}),
            verified_capabilities=frozenset({ObservationCapability.EVENT_REVELATION}),
            upstream_sources=tuple(item.source_id for item in source_configs),
            auth_required=False,
            provides_source_updated_at=True,
            provides_aggregator_fetched_at=False,
            provides_historical_occurrence_at=False,
            provides_revision_history=False,
            enabled=True,
            trust_tier=ObservationTrustTier.CONTRACT_VALIDATED,
            license_note=(
                "Source-specific metadata/excerpt scope is frozen in each public source config; "
                "no article-body, redistribution, historical-availability, or trading-use right "
                "is inferred."
            ),
        )
        self._manifest.assert_valid()

    @property
    def manifest(self) -> ObservationProviderManifest:
        return self._manifest

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]:
        return self._sources[upstream_source].to_dict()

    async def collect(self) -> tuple[SyndicationFeedCapture, ...]:
        captures: list[SyndicationFeedCapture] = []
        for config in self._sources.values():
            captures.append(await self._collect_one(config))
        return tuple(captures)

    def replay(
        self,
        captures: tuple[SyndicationFeedCapture, ...],
    ) -> _CapturedSyndicationFeedProvider:
        return _CapturedSyndicationFeedProvider(self, captures)

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
        capture = await self._collect_one(config)
        return self.response_from_capture(query=query, source=source, capture=capture)

    async def _collect_one(
        self,
        config: SyndicationFeedSourceConfig,
    ) -> SyndicationFeedCapture:
        try:
            response = await asyncio.to_thread(
                self._http_client.get,
                config.request_url,
                max_response_bytes=self._max_response_bytes,
            )
            retrieved_at = _utc_now(self._clock)
        except SyndicationFeedHTTPError as exc:
            retrieved_at = _utc_now(self._clock)
            return SyndicationFeedCapture(
                source_id=config.source_id,
                retrieved_at=retrieved_at,
                response=None,
                failure_status=(
                    DataFetchStatus.RATE_LIMITED
                    if exc.status_code == 429
                    else DataFetchStatus.ERROR
                ),
                error_kind=f"http_{exc.status_code}",
            )
        except SyndicationFeedError as exc:
            return SyndicationFeedCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                response=None,
                failure_status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        except Exception as exc:
            return SyndicationFeedCapture(
                source_id=config.source_id,
                retrieved_at=_utc_now(self._clock),
                response=None,
                failure_status=DataFetchStatus.ERROR,
                error_kind=type(exc).__name__,
            )
        return SyndicationFeedCapture(
            source_id=config.source_id,
            retrieved_at=retrieved_at,
            response=response,
        )

    def response_from_capture(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
        capture: SyndicationFeedCapture,
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
        if capture.response is None:
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
            if not capture.response.body:
                raise SyndicationFeedParseError("syndication feed response is empty")
            if capture.response.final_url != config.expected_final_url:
                raise SyndicationFeedIdentityError(
                    "syndication feed redirect target does not match source config"
                )
            if capture.response.content_type.casefold() not in _ALLOWED_CONTENT_TYPES:
                raise SyndicationFeedParseError(
                    f"unsupported syndication content type: {capture.response.content_type}"
                )
            parsed_feed = _parse_observations(
                capture.response.body,
                config=config,
                query=query,
                retrieved_at=capture.retrieved_at,
                keywords=parameters.keywords,
                max_items=parameters.max_items,
                resolved_feed_url=capture.response.final_url,
            )
        except SyndicationFeedError as exc:
            return self._failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        observations = tuple(item.observation for item in parsed_feed)
        raw_records = tuple(
            (item.observation.observation_id, item.raw_record) for item in parsed_feed
        )
        return ProviderDataResponse(
            status=DataFetchStatus.DATA if observations else DataFetchStatus.NO_DATA,
            provider_id=self.manifest.provider_id,
            provider_version=self.manifest.provider_version,
            upstream_source=source.upstream_source,
            retrieved_at=capture.retrieved_at,
            raw_payload=capture.response.body,
            observations=observations,
            raw_records=raw_records,
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


class _CapturedSyndicationFeedProvider(DataProvider):
    def __init__(
        self,
        provider: SyndicationFeedProvider,
        captures: tuple[SyndicationFeedCapture, ...],
    ) -> None:
        capture_by_source = {item.source_id: item for item in captures}
        if len(capture_by_source) != len(captures) or set(capture_by_source) != set(
            provider.manifest.upstream_sources
        ):
            raise ValueError(
                "syndication feed captures must cover each registered source exactly once"
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
        capture = self._captures[source.upstream_source]
        return self._provider.response_from_capture(
            query=query,
            source=source,
            capture=capture,
        )


@dataclass(frozen=True, slots=True)
class _QueryParameters:
    keywords: tuple[str, ...]
    max_items: int


@dataclass(frozen=True, slots=True)
class _ParsedObservation:
    observation: SourceObservation
    raw_record: bytes


def load_syndication_feed_source(path: Path) -> SyndicationFeedSourceConfig:
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    errors = validate_agent_contract(payload, "syndication-feed-source.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    fields = _mapping(payload, "syndication feed source")
    config = SyndicationFeedSourceConfig(
        schema_version=_string(fields, "schema_version"),
        source_config_id=_string(fields, "source_config_id"),
        source_id=_string(fields, "source_id"),
        request_url=_string(fields, "request_url"),
        expected_final_url=_string(fields, "expected_final_url"),
        publisher=_string(fields, "publisher"),
        license_scope=_string(fields, "license_scope"),
        content_scope=_string(fields, "content_scope"),
    )
    if config.to_dict() != fields:
        raise ValueError("syndication feed source is not canonical")
    return config


def _parse_observations(
    body: bytes,
    *,
    config: SyndicationFeedSourceConfig,
    query: DataQuery,
    retrieved_at: datetime,
    keywords: tuple[str, ...],
    max_items: int,
    resolved_feed_url: str,
) -> tuple[_ParsedObservation, ...]:
    try:
        module = importlib.import_module("feedparser")
    except ModuleNotFoundError as exc:
        raise SyndicationFeedParseError(
            "feedparser is required; install the project data extra"
        ) from exc
    parse = cast(Callable[[bytes], object], module.parse)
    parsed = _mapping(parse(body), "parsed syndication feed")
    if bool(parsed.get("bozo", False)):
        raise SyndicationFeedParseError("syndication feed is not well-formed")
    entries = _sequence(parsed.get("entries"), "syndication feed entries")
    raw_entry_records = _exact_entry_records(body)
    if len(entries) != len(raw_entry_records):
        raise SyndicationFeedParseError(
            "syndication parser entry count does not match exact XML records"
        )
    feed = _mapping(parsed.get("feed", {}), "syndication feed metadata")
    feed_title = _optional_string(feed, "title")
    observations: list[_ParsedObservation] = []
    for raw_entry, raw_record in zip(entries, raw_entry_records, strict=True):
        entry = _mapping(raw_entry, "syndication feed entry")
        title = _string(entry, "title")
        link = _string(entry, "link")
        _https_url(link, "syndication feed entry link")
        record_id = _optional_string(entry, "id") or link
        published_at = _entry_time(entry, "published_parsed")
        if published_at is None:
            raise SyndicationFeedParseError("syndication feed entry lacks publication time")
        if published_at > retrieved_at:
            raise SyndicationFeedParseError(
                "syndication feed publication time is after actual receipt"
            )
        if query.window_start is not None and published_at < query.window_start:
            continue
        if published_at > query.as_of:
            continue
        summary = _optional_string(entry, "summary") or ""
        if keywords and not _keyword_match(keywords, title=title, summary=summary):
            continue
        updated_at = _explicit_updated_at(entry)
        if updated_at is not None and not published_at <= updated_at <= retrieved_at:
            raise SyndicationFeedParseError(
                "syndication feed update time is outside publication and receipt"
            )
        creator = _optional_string(entry, "author")
        observation = SourceObservation.build(
            capability=ObservationCapability.EVENT_REVELATION,
            provider_id=SYNDICATION_FEED_PROVIDER_ID,
            provider_version=SYNDICATION_FEED_PROVIDER_VERSION,
            upstream_source=config.source_id,
            upstream_record_id=record_id,
            source_ref=link,
            lineage_id=f"{config.source_id}:{record_id}",
            times=ObservationTimes(
                occurred_at=retrieved_at,
                published_at=published_at,
                available_at=retrieved_at,
                source_updated_at=updated_at,
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
                "feed_title": feed_title,
                "feed_url": config.request_url,
                "resolved_feed_url": resolved_feed_url,
                "headline": title,
                "summary": summary,
                "url": link,
                "creator": creator,
                "published_at": _timestamp(published_at),
                "content_scope": config.content_scope,
            },
            license_scope=config.license_scope,
        )
        observations.append(_ParsedObservation(observation=observation, raw_record=raw_record))
        if len(observations) >= max_items:
            break
    return tuple(observations)


def _exact_entry_records(body: bytes) -> tuple[bytes, ...]:
    if re.search(rb"<!\s*(?:DOCTYPE|ENTITY)\b", body, flags=re.IGNORECASE):
        raise SyndicationFeedParseError(
            "syndication feed cannot contain DTD or entity declarations"
        )
    parser = expat.ParserCreate()
    depth = 0
    active_name: str | None = None
    active_depth: int | None = None
    active_start: int | None = None
    records: list[bytes] = []

    def start_element(name: str, attributes: Mapping[str, str]) -> None:
        nonlocal active_depth, active_name, active_start, depth
        del attributes
        depth += 1
        local_name = name.rsplit(":", 1)[-1]
        if local_name.casefold() in {"content", "encoded"}:
            raise SyndicationFeedParseError(
                "syndication feed contains a full-content element outside the allowed excerpt scope"
            )
        if active_name is None and local_name in {"entry", "item"}:
            active_name = local_name
            active_depth = depth
            active_start = parser.CurrentByteIndex

    def end_element(name: str) -> None:
        nonlocal active_depth, active_name, active_start, depth
        local_name = name.rsplit(":", 1)[-1]
        if active_name == local_name and active_depth == depth and active_start is not None:
            closing_end = body.find(b">", parser.CurrentByteIndex)
            if closing_end < 0:
                raise SyndicationFeedParseError("syndication entry closing tag is incomplete")
            records.append(body[active_start : closing_end + 1])
            active_name = None
            active_depth = None
            active_start = None
        depth -= 1

    def reject_external_entity(
        context: str,
        base: str | None,
        system_id: str | None,
        public_id: str | None,
    ) -> int:
        del context, base, system_id, public_id
        return 0

    parser.StartElementHandler = start_element
    parser.EndElementHandler = end_element
    parser.ExternalEntityRefHandler = reject_external_entity
    try:
        parser.Parse(body, True)
    except expat.ExpatError as exc:
        raise SyndicationFeedParseError("syndication feed XML parsing failed") from exc
    if active_name is not None:
        raise SyndicationFeedParseError("syndication feed entry did not close")
    return tuple(records)


def _query_parameters(value: Mapping[str, object]) -> _QueryParameters:
    unexpected = set(value) - {"keywords", "max_items"}
    if unexpected:
        raise SyndicationFeedParseError(
            f"unsupported syndication query parameters: {sorted(unexpected)}"
        )
    raw_keywords = value.get("keywords", [])
    if not isinstance(raw_keywords, list):
        raise SyndicationFeedParseError("syndication keywords must be an array")
    keywords: list[str] = []
    for item in cast(list[object], raw_keywords):
        if not isinstance(item, str) or not item.strip():
            raise SyndicationFeedParseError("syndication keywords must be non-empty strings")
        keywords.append(item.strip().casefold())
    raw_max_items = value.get("max_items", 50)
    if isinstance(raw_max_items, bool) or not isinstance(raw_max_items, int):
        raise SyndicationFeedParseError("syndication max_items must be an integer")
    if not 1 <= raw_max_items <= 100:
        raise SyndicationFeedParseError("syndication max_items must be between 1 and 100")
    return _QueryParameters(keywords=tuple(dict.fromkeys(keywords)), max_items=raw_max_items)


def _explicit_updated_at(entry: Mapping[str, object]) -> datetime | None:
    if "updated" not in entry:
        return None
    return _entry_time(entry, "updated_parsed")


def _entry_time(entry: Mapping[str, object], key: str) -> datetime | None:
    value = entry.get(key)
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise SyndicationFeedParseError(f"invalid syndication timestamp field: {key}")
    sequence = cast(Sequence[object], value)
    if len(sequence) < 6:
        raise SyndicationFeedParseError(f"invalid syndication timestamp field: {key}")
    parts = tuple(sequence[index] for index in range(6))
    if any(isinstance(item, bool) or not isinstance(item, int) for item in parts):
        raise SyndicationFeedParseError(f"invalid syndication timestamp field: {key}")
    year, month, day, hour, minute, second = cast(tuple[int, int, int, int, int, int], parts)
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    except ValueError as exc:
        raise SyndicationFeedParseError(f"invalid syndication timestamp field: {key}") from exc


def _keyword_match(keywords: tuple[str, ...], *, title: str, summary: str) -> bool:
    haystack = f"{title}\n{summary}".casefold()
    return any(keyword in haystack for keyword in keywords)


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("syndication feed clock must return UTC")
    return value


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise SyndicationFeedParseError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise SyndicationFeedParseError(f"{name} keys must be strings")
    return dict(cast(Mapping[str, object], value))


def _sequence(value: object, name: str) -> tuple[object, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise SyndicationFeedParseError(f"{name} must be an array")
    return tuple(cast(Sequence[object], value))


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise SyndicationFeedParseError(f"{key} must be a non-empty string")
    return item.strip()


def _optional_string(value: Mapping[str, object], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str) or not item.strip():
        raise SyndicationFeedParseError(f"{key} must be a non-empty string when present")
    return item.strip()


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} has invalid characters")


def _https_url(value: str, name: str) -> None:
    parts = urlsplit(value)
    if parts.scheme != "https" or not parts.netloc or parts.username or parts.password:
        raise ValueError(f"{name} must be an HTTPS URL without embedded credentials")
