from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
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

TUSHARE_OBSERVATION_SOURCE_SCHEMA = "market-impact.tushare-observation-source.v1"
TUSHARE_OBSERVATION_CAPTURE_SCHEMA = "market-impact.tushare-observation-capture.v1"
TUSHARE_OBSERVATION_ENDPOINT = "https://api.tushare.pro"
TUSHARE_OBSERVATION_PROVIDER_ID = "tushare-observation"
TUSHARE_OBSERVATION_PROVIDER_VERSION = "1"
TUSHARE_AGGREGATOR = "Tushare Pro"

_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_SENSITIVE_PARAMETER_PARTS = frozenset({"token", "credential", "secret", "password"})
_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
_MAX_CAPTURE_BYTES = 256 * 1024 * 1024


class TushareObservationError(RuntimeError):
    error_kind = "tushare_observation_error"


class TushareObservationSourceError(TushareObservationError):
    error_kind = "source_config_error"


class TushareObservationTransportError(TushareObservationError):
    error_kind = "transport_error"


class TushareObservationResponseTooLargeError(TushareObservationTransportError):
    error_kind = "response_too_large"


class TushareObservationCaptureTooLargeError(TushareObservationError):
    error_kind = "capture_size_exceeded"


class TushareObservationPermissionError(TushareObservationError):
    error_kind = "permission_denied"


class TushareObservationAPIError(TushareObservationError):
    error_kind = "tushare_api_error"


class TushareObservationParseError(TushareObservationError):
    error_kind = "source_parse_error"


class TushareObservationFieldError(TushareObservationError):
    error_kind = "response_field_mismatch"


class TushareObservationDuplicateError(TushareObservationError):
    error_kind = "duplicate_primary_key"


class TushareObservationPaginationError(TushareObservationError):
    error_kind = "pagination_limit_exceeded"


class TushareObservationOverflowError(TushareObservationError):
    error_kind = "pagination_overflow"


class TushareObservationSecretLeakError(TushareObservationError):
    error_kind = "response_secret_leak"


class TushareObservationTransport(Protocol):
    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes: ...


@dataclass(frozen=True, slots=True)
class TushareObservationCaptureUsage:
    request_count: int
    response_bytes: int
    capture_bytes: int

    def __post_init__(self) -> None:
        if self.request_count < 1 or self.response_bytes < 1 or self.capture_bytes < 1:
            raise ValueError("Tushare capture usage values must be positive")


@dataclass(frozen=True, slots=True)
class TushareObservationSourceConfig:
    source_config_id: str
    source_id: str
    api_name: str
    capability: ObservationCapability
    upstream_publisher: str
    documentation_url: str
    rights_url: str
    license_scope: str
    content_scope: str
    semantic_scope: str
    fields: tuple[str, ...]
    primary_key_fields: tuple[str, ...]
    allowed_parameters: tuple[str, ...]
    fixed_parameters_json: str = field(repr=False)
    date_fields: tuple[str, ...]
    datetime_fields: tuple[str, ...]
    publisher_time_field: str | None
    aggregator_timestamp_field: str | None
    pagination_page_size: int
    pagination_max_pages: int
    source_timezone: str = "Asia/Shanghai"
    schema_version: str = TUSHARE_OBSERVATION_SOURCE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TUSHARE_OBSERVATION_SOURCE_SCHEMA:
            raise ValueError("unsupported Tushare observation source schema_version")
        _identifier(self.source_id, "Tushare observation source_id")
        _identifier(self.api_name, "Tushare observation api_name")
        _trimmed(self.upstream_publisher, "Tushare observation upstream_publisher")
        _https_url(self.documentation_url, "Tushare observation documentation_url")
        _https_url(self.rights_url, "Tushare observation rights_url")
        _trimmed(self.license_scope, "Tushare observation license_scope")
        _trimmed(self.content_scope, "Tushare observation content_scope")
        _trimmed(self.semantic_scope, "Tushare observation semantic_scope")
        _unique_identifiers(self.fields, "Tushare observation fields")
        _unique_identifiers(self.primary_key_fields, "Tushare observation primary_key_fields")
        if not self.primary_key_fields:
            raise ValueError("Tushare observation primary_key_fields must not be empty")
        if not set(self.primary_key_fields) <= set(self.fields):
            raise ValueError("Tushare observation primary_key_fields must be response fields")
        _unique_identifiers(self.allowed_parameters, "Tushare observation allowed_parameters")
        if any(item in {"offset", "limit"} for item in self.allowed_parameters):
            raise ValueError("Tushare observation source controls offset and limit pagination")
        fixed_parameters = _canonical_object(
            self.fixed_parameters_json,
            "Tushare observation fixed_parameters_json",
        )
        _validate_public_parameters(fixed_parameters, "Tushare observation fixed_parameters")
        if any(key in {"offset", "limit"} for key in fixed_parameters):
            raise ValueError("Tushare observation fixed parameters cannot override pagination")
        _optional_unique_identifiers(self.date_fields, "Tushare observation date_fields")
        _optional_unique_identifiers(self.datetime_fields, "Tushare observation datetime_fields")
        if set(self.date_fields) & set(self.datetime_fields):
            raise ValueError("Tushare observation temporal fields must not overlap")
        if not set(self.date_fields) | set(self.datetime_fields) <= set(self.fields):
            raise ValueError("Tushare observation temporal fields must be response fields")
        for name, field_name in (
            ("publisher_time_field", self.publisher_time_field),
            ("aggregator_timestamp_field", self.aggregator_timestamp_field),
        ):
            if field_name is not None and field_name not in self.datetime_fields:
                raise ValueError(f"Tushare observation {name} must be a datetime field")
        if self.publisher_time_field is not None and self.aggregator_timestamp_field is not None:
            raise ValueError(
                "a route cannot use one timestamp as both publisher and aggregator time"
            )
        if not 1 <= self.pagination_page_size <= 10_000:
            raise ValueError("Tushare observation pagination_page_size must be between 1 and 10000")
        if not 1 <= self.pagination_max_pages <= 1_000:
            raise ValueError("Tushare observation pagination_max_pages must be between 1 and 1000")
        try:
            ZoneInfo(self.source_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(
                "Tushare observation source_timezone must be an IANA timezone"
            ) from exc
        if self.source_config_id != self.expected_source_config_id:
            raise ValueError("Tushare observation source_config_id does not match content")

    @property
    def fixed_parameters(self) -> dict[str, object]:
        return _canonical_object(
            self.fixed_parameters_json,
            "Tushare observation fixed_parameters_json",
        )

    @property
    def expected_source_config_id(self) -> str:
        return f"tushare-observation-source-{canonical_hash(self.core_dict())}"

    @property
    def artifact_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "api_name": self.api_name,
            "capability": self.capability.value,
            "upstream_publisher": self.upstream_publisher,
            "documentation_url": self.documentation_url,
            "rights_url": self.rights_url,
            "license_scope": self.license_scope,
            "content_scope": self.content_scope,
            "semantic_scope": self.semantic_scope,
            "fields": list(self.fields),
            "primary_key_fields": list(self.primary_key_fields),
            "allowed_parameters": list(self.allowed_parameters),
            "fixed_parameters": self.fixed_parameters,
            "date_fields": list(self.date_fields),
            "datetime_fields": list(self.datetime_fields),
            "publisher_time_field": self.publisher_time_field,
            "aggregator_timestamp_field": self.aggregator_timestamp_field,
            "pagination_page_size": self.pagination_page_size,
            "pagination_max_pages": self.pagination_max_pages,
            "source_timezone": self.source_timezone,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "source_config_id": self.source_config_id}

    @classmethod
    def build(
        cls,
        *,
        source_id: str,
        api_name: str,
        capability: ObservationCapability,
        upstream_publisher: str,
        documentation_url: str,
        rights_url: str,
        license_scope: str,
        content_scope: str,
        semantic_scope: str,
        fields: tuple[str, ...],
        primary_key_fields: tuple[str, ...],
        allowed_parameters: tuple[str, ...],
        fixed_parameters: Mapping[str, object],
        date_fields: tuple[str, ...],
        datetime_fields: tuple[str, ...],
        publisher_time_field: str | None,
        aggregator_timestamp_field: str | None,
        pagination_page_size: int,
        pagination_max_pages: int,
        source_timezone: str = "Asia/Shanghai",
    ) -> TushareObservationSourceConfig:
        fixed_parameters_json = canonical_json_bytes(fixed_parameters).decode()
        core = {
            "schema_version": TUSHARE_OBSERVATION_SOURCE_SCHEMA,
            "source_id": source_id,
            "api_name": api_name,
            "capability": capability.value,
            "upstream_publisher": upstream_publisher,
            "documentation_url": documentation_url,
            "rights_url": rights_url,
            "license_scope": license_scope,
            "content_scope": content_scope,
            "semantic_scope": semantic_scope,
            "fields": list(fields),
            "primary_key_fields": list(primary_key_fields),
            "allowed_parameters": list(allowed_parameters),
            "fixed_parameters": _canonical_object(
                fixed_parameters_json,
                "Tushare observation fixed_parameters",
            ),
            "date_fields": list(date_fields),
            "datetime_fields": list(datetime_fields),
            "publisher_time_field": publisher_time_field,
            "aggregator_timestamp_field": aggregator_timestamp_field,
            "pagination_page_size": pagination_page_size,
            "pagination_max_pages": pagination_max_pages,
            "source_timezone": source_timezone,
        }
        return cls(
            source_config_id=f"tushare-observation-source-{canonical_hash(core)}",
            source_id=source_id,
            api_name=api_name,
            capability=capability,
            upstream_publisher=upstream_publisher,
            documentation_url=documentation_url,
            rights_url=rights_url,
            license_scope=license_scope,
            content_scope=content_scope,
            semantic_scope=semantic_scope,
            fields=fields,
            primary_key_fields=primary_key_fields,
            allowed_parameters=allowed_parameters,
            fixed_parameters_json=fixed_parameters_json,
            date_fields=date_fields,
            datetime_fields=datetime_fields,
            publisher_time_field=publisher_time_field,
            aggregator_timestamp_field=aggregator_timestamp_field,
            pagination_page_size=pagination_page_size,
            pagination_max_pages=pagination_max_pages,
            source_timezone=source_timezone,
        )


@dataclass(frozen=True, slots=True)
class TushareObservationPageCapture:
    page: int
    request_parameters_json: str = field(repr=False)
    response_body: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if self.page < 1:
            raise ValueError("Tushare observation capture page must be positive")
        _canonical_object(
            self.request_parameters_json,
            "Tushare observation page request_parameters_json",
        )
        if not self.response_body:
            raise ValueError("Tushare observation capture response body must not be empty")

    @property
    def request_parameters(self) -> dict[str, object]:
        return _canonical_object(
            self.request_parameters_json,
            "Tushare observation page request_parameters_json",
        )


@dataclass(frozen=True, slots=True)
class TushareObservationCapture:
    source_id: str
    request_parameters_json: str = field(repr=False)
    retrieved_at: datetime
    pages: tuple[TushareObservationPageCapture, ...]
    coverage_complete: bool
    failure_status: DataFetchStatus | None = None
    error_kind: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.source_id, "Tushare observation capture source_id")
        _canonical_object(
            self.request_parameters_json,
            "Tushare observation capture request_parameters_json",
        )
        _strict_utc(self.retrieved_at, "Tushare observation capture retrieved_at")
        if any(item.page != index for index, item in enumerate(self.pages, start=1)):
            raise ValueError("Tushare observation capture pages must be contiguous")
        if self.coverage_complete:
            if not self.pages:
                raise ValueError("complete Tushare observation capture requires a response page")
            if self.failure_status is not None or self.error_kind is not None:
                raise ValueError("complete Tushare observation capture cannot carry a failure")
        elif self.failure_status is None or self.error_kind is None:
            raise ValueError("incomplete Tushare observation capture requires a typed failure")

    @property
    def request_parameters(self) -> dict[str, object]:
        return _canonical_object(
            self.request_parameters_json,
            "Tushare observation capture request_parameters_json",
        )


@dataclass(frozen=True, slots=True)
class _ParsedPage:
    fields: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]


class TushareObservationProvider(DataProvider):
    def __init__(
        self,
        token: str,
        source_configs: tuple[TushareObservationSourceConfig, ...],
        *,
        endpoint: str = TUSHARE_OBSERVATION_ENDPOINT,
        timeout_seconds: float = 30.0,
        transport: TushareObservationTransport | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not token or token != token.strip():
            raise ValueError(
                "Tushare observation token must be a non-empty trimmed constructor value"
            )
        if endpoint != TUSHARE_OBSERVATION_ENDPOINT:
            raise ValueError("Tushare observation endpoint must be https://api.tushare.pro")
        if timeout_seconds <= 0:
            raise ValueError("Tushare observation timeout_seconds must be positive")
        if not source_configs:
            raise ValueError("Tushare observation provider requires at least one source config")
        sources = {item.source_id: item for item in source_configs}
        if len(sources) != len(source_configs):
            raise ValueError("Tushare observation source IDs must be unique")
        self._token = token
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._sources = sources
        self._transport = _post_json if transport is None else transport
        self._clock = (lambda: datetime.now(UTC)) if clock is None else clock
        capabilities = frozenset(item.capability for item in source_configs)
        self._manifest = ObservationProviderManifest(
            schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
            provider_id=TUSHARE_OBSERVATION_PROVIDER_ID,
            provider_version=TUSHARE_OBSERVATION_PROVIDER_VERSION,
            transport=ProviderTransport.HTTP,
            declared_capabilities=capabilities,
            verified_capabilities=capabilities,
            upstream_sources=tuple(item.source_id for item in source_configs),
            auth_required=True,
            provides_source_updated_at=False,
            provides_aggregator_fetched_at=any(
                item.aggregator_timestamp_field is not None for item in source_configs
            ),
            provides_historical_occurrence_at=False,
            provides_revision_history=False,
            enabled=True,
            trust_tier=ObservationTrustTier.CONTRACT_VALIDATED,
            license_note=(
                "Tushare route contracts are enabled for private research only. Each configured "
                "source remains externally subject to route acceptance; actual receipt proves "
                "neither publisher history nor retrospective availability."
            ),
        )
        self._manifest.assert_valid()

    def __repr__(self) -> str:
        return (
            "TushareObservationProvider("
            f"source_ids={tuple(self._sources)!r}, endpoint={self._endpoint!r}, token='[REDACTED]')"
        )

    @property
    def manifest(self) -> ObservationProviderManifest:
        return self._manifest

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]:
        return self._sources[upstream_source].to_dict()

    async def collect(
        self,
        *,
        source_id: str,
        parameters: Mapping[str, object],
    ) -> TushareObservationCapture:
        config = self._sources.get(source_id)
        retrieved_at = _utc_now(self._clock)
        if config is None:
            return _failed_capture(
                source_id=source_id,
                request_parameters={},
                retrieved_at=retrieved_at,
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_config_missing",
            )
        try:
            request_parameters = _request_parameters(config, parameters)
        except TushareObservationError as exc:
            return _failed_capture(
                source_id=config.source_id,
                request_parameters={},
                retrieved_at=retrieved_at,
                status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        pages: list[TushareObservationPageCapture] = []
        captured_bytes = 0
        try:
            for page_number in range(1, config.pagination_max_pages + 1):
                page_parameters = _page_parameters(
                    request_parameters,
                    offset=(page_number - 1) * config.pagination_page_size,
                    limit=config.pagination_page_size,
                )
                body = _request_body(
                    api_name=config.api_name,
                    token=self._token,
                    parameters=page_parameters,
                    fields=config.fields,
                )
                response_body = await asyncio.to_thread(
                    self._transport,
                    self._endpoint,
                    body,
                    self._timeout_seconds,
                )
                captured_bytes += len(response_body)
                if captured_bytes > _MAX_CAPTURE_BYTES:
                    raise TushareObservationCaptureTooLargeError(
                        "Tushare capture exceeded its aggregate byte limit"
                    )
                if self._token.encode() in response_body:
                    # Preserve the typed API failure without retaining its secret-bearing body.
                    _parse_response_page(response_body, config=config)
                    raise TushareObservationSecretLeakError(
                        "Tushare response contains the configured token and cannot be captured"
                    )
                # Preserve the bounded, secret-free received page before interpretation.
                # Parse/key/API errors must remain auditable without another network read.
                pages.append(
                    TushareObservationPageCapture(
                        page=page_number,
                        request_parameters_json=canonical_json_bytes(page_parameters).decode(),
                        response_body=response_body,
                    )
                )
                page = _parse_response_page(response_body, config=config)
                _validate_page_primary_keys(page, config=config)
                if len(page.rows) > config.pagination_page_size:
                    raise TushareObservationOverflowError(
                        "Tushare response exceeded the configured page size"
                    )
                if len(page.rows) < config.pagination_page_size:
                    return TushareObservationCapture(
                        source_id=config.source_id,
                        request_parameters_json=canonical_json_bytes(request_parameters).decode(),
                        retrieved_at=_utc_now(self._clock),
                        pages=tuple(pages),
                        coverage_complete=True,
                    )
            return TushareObservationCapture(
                source_id=config.source_id,
                request_parameters_json=canonical_json_bytes(request_parameters).decode(),
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind=TushareObservationPaginationError.error_kind,
            )
        except TushareObservationError as exc:
            return TushareObservationCapture(
                source_id=config.source_id,
                request_parameters_json=canonical_json_bytes(request_parameters).decode(),
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
            )
        except Exception:
            return TushareObservationCapture(
                source_id=config.source_id,
                request_parameters_json=canonical_json_bytes(request_parameters).decode(),
                retrieved_at=_utc_now(self._clock),
                pages=tuple(pages),
                coverage_complete=False,
                failure_status=DataFetchStatus.ERROR,
                error_kind="unexpected_provider_error",
            )

    def replay(
        self,
        captures: tuple[TushareObservationCapture, ...],
    ) -> _CapturedTushareObservationProvider:
        return _CapturedTushareObservationProvider(self, captures)

    async def fetch(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
    ) -> ProviderDataResponse:
        config = self._sources.get(source.upstream_source)
        if config is None:
            return self.failed_response(
                source=source,
                retrieved_at=_utc_now(self._clock),
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_config_missing",
            )
        capture = await self.collect(source_id=config.source_id, parameters=query.parameters)
        return self.response_from_capture(query=query, source=source, capture=capture)

    def response_from_capture(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
        capture: TushareObservationCapture,
    ) -> ProviderDataResponse:
        config = self._sources.get(source.upstream_source)
        if config is None or capture.source_id != source.upstream_source:
            return self.failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_config_missing",
            )
        if query.capability is not config.capability:
            return self.failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="unsupported_capability",
            )
        if not capture.coverage_complete:
            assert capture.failure_status is not None
            assert capture.error_kind is not None
            return self.failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=capture.failure_status,
                error_kind=capture.error_kind,
                raw_payload=_capture_bundle(
                    capture, config=config, request_parameters=capture.request_parameters
                )
                if capture.pages
                else None,
            )
        try:
            request_parameters = _request_parameters(config, query.parameters)
            if capture.request_parameters != request_parameters:
                raise TushareObservationSourceError("capture parameters do not match the query")
            parsed_pages = tuple(
                _parse_capture_page(
                    item,
                    config=config,
                    request_parameters=request_parameters,
                )
                for item in capture.pages
            )
            _validate_capture_page_completeness(parsed_pages, config=config)
            observations = _observations_from_pages(
                parsed_pages,
                config=config,
                retrieved_at=capture.retrieved_at,
            )
            raw_payload = _capture_bundle(
                capture,
                config=config,
                request_parameters=request_parameters,
            )
        except TushareObservationError as exc:
            return self.failed_response(
                source=source,
                retrieved_at=capture.retrieved_at,
                status=DataFetchStatus.ERROR,
                error_kind=exc.error_kind,
                raw_payload=_capture_bundle(
                    capture, config=config, request_parameters=capture.request_parameters
                ),
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

    def failed_response(
        self,
        *,
        source: DataSourceBinding,
        retrieved_at: datetime,
        status: DataFetchStatus,
        error_kind: str,
        raw_payload: bytes | None = None,
    ) -> ProviderDataResponse:
        return ProviderDataResponse(
            status=status,
            provider_id=self.manifest.provider_id,
            provider_version=self.manifest.provider_version,
            upstream_source=source.upstream_source,
            retrieved_at=retrieved_at,
            raw_payload=raw_payload,
            observations=(),
            raw_records=(),
            error_kind=error_kind,
        )

    def receipt_now(self) -> datetime:
        return _utc_now(self._clock)


class _CapturedTushareObservationProvider(DataProvider):
    def __init__(
        self,
        provider: TushareObservationProvider,
        captures: tuple[TushareObservationCapture, ...],
    ) -> None:
        capture_by_source = {item.source_id: item for item in captures}
        if len(capture_by_source) != len(captures):
            raise ValueError("Tushare observation replay captures must have unique source IDs")
        if not set(capture_by_source) <= set(provider.manifest.upstream_sources):
            raise ValueError("Tushare observation replay contains an unregistered source")
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
        capture = self._captures.get(source.upstream_source)
        if capture is None:
            return self._provider.failed_response(
                source=source,
                retrieved_at=self._provider.receipt_now(),
                status=DataFetchStatus.NOT_CONFIGURED,
                error_kind="source_capture_missing",
            )
        return self._provider.response_from_capture(query=query, source=source, capture=capture)


def load_tushare_observation_source(path: Path) -> TushareObservationSourceConfig:
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    return tushare_observation_source_from_dict(payload)


def tushare_observation_source_from_dict(payload: object) -> TushareObservationSourceConfig:
    errors = _source_schema_errors(payload)
    if errors:
        raise ValueError("; ".join(errors))
    fields = _mapping(payload, "Tushare observation source")
    fixed_parameters = _mapping(fields.get("fixed_parameters"), "fixed_parameters")
    config = TushareObservationSourceConfig(
        source_config_id=_string(fields, "source_config_id"),
        source_id=_string(fields, "source_id"),
        api_name=_string(fields, "api_name"),
        capability=ObservationCapability(_string(fields, "capability")),
        upstream_publisher=_string(fields, "upstream_publisher"),
        documentation_url=_string(fields, "documentation_url"),
        rights_url=_string(fields, "rights_url"),
        license_scope=_string(fields, "license_scope"),
        content_scope=_string(fields, "content_scope"),
        semantic_scope=_string(fields, "semantic_scope"),
        fields=_string_tuple(fields.get("fields"), "fields"),
        primary_key_fields=_string_tuple(fields.get("primary_key_fields"), "primary_key_fields"),
        allowed_parameters=_string_tuple(fields.get("allowed_parameters"), "allowed_parameters"),
        fixed_parameters_json=canonical_json_bytes(fixed_parameters).decode(),
        date_fields=_string_tuple(fields.get("date_fields"), "date_fields"),
        datetime_fields=_string_tuple(fields.get("datetime_fields"), "datetime_fields"),
        publisher_time_field=_nullable_string(
            fields.get("publisher_time_field"), "publisher_time_field"
        ),
        aggregator_timestamp_field=_nullable_string(
            fields.get("aggregator_timestamp_field"),
            "aggregator_timestamp_field",
        ),
        pagination_page_size=_integer(fields.get("pagination_page_size"), "pagination_page_size"),
        pagination_max_pages=_integer(fields.get("pagination_max_pages"), "pagination_max_pages"),
        source_timezone=_string(fields, "source_timezone"),
        schema_version=_string(fields, "schema_version"),
    )
    if config.to_dict() != fields:
        raise ValueError("Tushare observation source config is not canonical")
    return config


def load_tushare_observation_capture_bundle(
    payload: bytes,
    *,
    config: TushareObservationSourceConfig,
    parameters: Mapping[str, object],
    retrieved_at: datetime,
) -> TushareObservationCapture:
    _strict_utc(retrieved_at, "Tushare observation capture bundle retrieved_at")
    prefix = f"{TUSHARE_OBSERVATION_CAPTURE_SCHEMA}\n".encode()
    if not payload.startswith(prefix):
        raise TushareObservationParseError(
            "Tushare observation capture bundle has an invalid header"
        )
    offset = len(prefix)
    top_end = payload.find(b"\n", offset)
    if top_end < 0:
        raise TushareObservationParseError("Tushare observation capture bundle header is truncated")
    try:
        top = _mapping(json.loads(payload[offset:top_end]), "Tushare observation capture header")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TushareObservationParseError(
            "Tushare observation capture bundle header is invalid"
        ) from exc
    request_parameters = _request_parameters(config, parameters)
    if _string(top, "source_id") != config.source_id:
        raise TushareObservationSourceError("Tushare observation capture source ID mismatch")
    if _string(top, "source_config_id") != config.source_config_id:
        raise TushareObservationSourceError("Tushare observation capture config ID mismatch")
    if (
        _mapping(top.get("request_parameters"), "Tushare observation capture parameters")
        != request_parameters
    ):
        raise TushareObservationSourceError("Tushare observation capture parameter mismatch")
    offset = top_end + 1
    pages: list[TushareObservationPageCapture] = []
    while offset < len(payload):
        header_end = payload.find(b"\n", offset)
        if header_end < 0:
            raise TushareObservationParseError(
                "Tushare observation capture page header is truncated"
            )
        try:
            header = _mapping(
                json.loads(payload[offset:header_end]),
                "Tushare observation capture page header",
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TushareObservationParseError(
                "Tushare observation capture page header is invalid"
            ) from exc
        page = _integer(header.get("page"), "page")
        if page != len(pages) + 1:
            raise TushareObservationParseError(
                "Tushare observation capture pages are not contiguous"
            )
        if _string(header, "api_name") != config.api_name:
            raise TushareObservationSourceError("Tushare observation capture API name mismatch")
        expected_parameters = _page_parameters(
            request_parameters,
            offset=(page - 1) * config.pagination_page_size,
            limit=config.pagination_page_size,
        )
        if (
            _mapping(header.get("request_parameters"), "capture page request parameters")
            != expected_parameters
        ):
            raise TushareObservationSourceError(
                "Tushare observation capture page parameter mismatch"
            )
        if _string_tuple(header.get("fields"), "capture page fields") != config.fields:
            raise TushareObservationFieldError("Tushare observation capture fields mismatch")
        body_size = _integer(header.get("body_size"), "body_size")
        if body_size < 1:
            raise TushareObservationParseError("Tushare observation capture body size is invalid")
        body_start = header_end + 1
        body_end = body_start + body_size
        if body_end >= len(payload) or payload[body_end : body_end + 1] != b"\n":
            raise TushareObservationParseError("Tushare observation capture body is truncated")
        body = payload[body_start:body_end]
        if sha256(body).hexdigest() != _string(header, "body_sha256"):
            raise TushareObservationParseError("Tushare observation capture body hash mismatch")
        _parse_response_page(body, config=config)
        pages.append(
            TushareObservationPageCapture(
                page=page,
                request_parameters_json=canonical_json_bytes(expected_parameters).decode(),
                response_body=body,
            )
        )
        offset = body_end + 1
    if not pages:
        raise TushareObservationParseError(
            "Tushare observation capture bundle has no response pages"
        )
    if len(pages) > config.pagination_max_pages:
        raise TushareObservationOverflowError(
            "Tushare observation capture exceeds configured page count"
        )
    if (
        len(_parse_response_page(pages[-1].response_body, config=config).rows)
        >= config.pagination_page_size
    ):
        raise TushareObservationPaginationError(
            "Tushare observation capture does not prove pagination completion"
        )
    return TushareObservationCapture(
        source_id=config.source_id,
        request_parameters_json=canonical_json_bytes(request_parameters).decode(),
        retrieved_at=retrieved_at,
        pages=tuple(pages),
        coverage_complete=True,
    )


def _observations_from_pages(
    pages: tuple[_ParsedPage, ...],
    *,
    config: TushareObservationSourceConfig,
    retrieved_at: datetime,
) -> tuple[tuple[SourceObservation, bytes], ...]:
    seen_primary_keys: set[str] = set()
    observations: list[tuple[SourceObservation, bytes]] = []
    for page in pages:
        for row in page.rows:
            values = dict(zip(config.fields, row, strict=True))
            _validate_row_temporal_fields(values, config=config)
            primary_values = {field: values[field] for field in config.primary_key_fields}
            primary_key_json = canonical_json_bytes(primary_values).decode()
            if primary_key_json in seen_primary_keys:
                if config.primary_key_fields == config.fields:
                    # Full-row source identity: identical repeated disclosures are
                    # retained in raw pages but normalize to one observation.
                    continue
                raise TushareObservationDuplicateError(
                    "Tushare response contains duplicate primary keys"
                )
            seen_primary_keys.add(primary_key_json)
            raw_record = canonical_json_bytes({"fields": list(config.fields), "values": list(row)})
            upstream_record_id = f"tushare-record-{canonical_hash(primary_values)}"
            publisher_time = _optional_configured_datetime(
                values,
                config.publisher_time_field,
                config=config,
            )
            aggregator_time = _optional_configured_datetime(
                values,
                config.aggregator_timestamp_field,
                config=config,
            )
            if publisher_time is not None and publisher_time > retrieved_at:
                raise TushareObservationParseError(
                    "Tushare publisher-reported time is after actual receipt"
                )
            if aggregator_time is not None and aggregator_time > retrieved_at:
                raise TushareObservationParseError(
                    "Tushare aggregator timestamp is after actual receipt"
                )
            occurrence_basis = (
                OccurrenceBasis.AGGREGATOR_SNAPSHOT
                if aggregator_time is not None
                else OccurrenceBasis.RETRIEVAL_OBSERVED
            )
            occurred_at = aggregator_time if aggregator_time is not None else retrieved_at
            observation = SourceObservation.build(
                capability=config.capability,
                provider_id=TUSHARE_OBSERVATION_PROVIDER_ID,
                provider_version=TUSHARE_OBSERVATION_PROVIDER_VERSION,
                upstream_source=config.source_id,
                upstream_record_id=upstream_record_id,
                source_ref=(f"tushare://api.tushare.pro/{config.api_name}/{upstream_record_id}"),
                lineage_id=f"{config.source_id}:{upstream_record_id}",
                times=ObservationTimes(
                    occurred_at=occurred_at,
                    published_at=publisher_time,
                    available_at=retrieved_at,
                    source_updated_at=None,
                    aggregator_fetched_at=aggregator_time,
                    retrieved_at=retrieved_at,
                    occurrence_basis=occurrence_basis,
                    availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
                ),
                authority_at=retrieved_at,
                authority_kind="actual_receipt",
                raw_content_hash=sha256(raw_record).hexdigest(),
                normalized_payload={
                    "aggregator": TUSHARE_AGGREGATOR,
                    "api_name": config.api_name,
                    "upstream_publisher": config.upstream_publisher,
                    "documentation_url": config.documentation_url,
                    "rights_url": config.rights_url,
                    "content_scope": config.content_scope,
                    "semantic_scope": config.semantic_scope,
                    "time_semantics": _time_semantics(config),
                    "record": values,
                },
                license_scope=config.license_scope,
            )
            observations.append((observation, raw_record))
    return tuple(observations)


def _validate_page_primary_keys(
    page: _ParsedPage,
    *,
    config: TushareObservationSourceConfig,
) -> None:
    seen_primary_keys: set[str] = set()
    for row in page.rows:
        values = dict(zip(config.fields, row, strict=True))
        primary_values = {field: values[field] for field in config.primary_key_fields}
        primary_key_json = canonical_json_bytes(primary_values).decode()
        if primary_key_json in seen_primary_keys and config.primary_key_fields != config.fields:
            raise TushareObservationDuplicateError(
                "Tushare response contains duplicate primary keys"
            )
        seen_primary_keys.add(primary_key_json)


def _validate_capture_page_completeness(
    pages: tuple[_ParsedPage, ...],
    *,
    config: TushareObservationSourceConfig,
) -> None:
    if not pages or len(pages) > config.pagination_max_pages:
        raise TushareObservationPaginationError(
            "Tushare capture does not fit the configured pagination boundary"
        )
    if any(len(page.rows) != config.pagination_page_size for page in pages[:-1]):
        raise TushareObservationPaginationError(
            "Tushare capture has a short page before its final response"
        )
    if len(pages[-1].rows) >= config.pagination_page_size:
        raise TushareObservationPaginationError(
            "Tushare capture does not prove pagination completion"
        )


def _parse_capture_page(
    capture: TushareObservationPageCapture,
    *,
    config: TushareObservationSourceConfig,
    request_parameters: Mapping[str, object],
) -> _ParsedPage:
    expected = _page_parameters(
        request_parameters,
        offset=(capture.page - 1) * config.pagination_page_size,
        limit=config.pagination_page_size,
    )
    if capture.request_parameters != expected:
        raise TushareObservationSourceError(
            "Tushare capture page parameters do not match its position"
        )
    page = _parse_response_page(capture.response_body, config=config)
    if len(page.rows) > config.pagination_page_size:
        raise TushareObservationOverflowError("Tushare capture page exceeds configured page size")
    return page


def _parse_response_page(
    response_body: bytes, *, config: TushareObservationSourceConfig
) -> _ParsedPage:
    try:
        payload = json.loads(response_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TushareObservationParseError("Tushare response is not valid UTF-8 JSON") from exc
    response = _mapping(payload, "Tushare response")
    code = _integer(response.get("code"), "Tushare response code")
    if code != 0:
        message = response.get("msg")
        if _permission_denied(code, message):
            raise TushareObservationPermissionError("Tushare permission was denied")
        raise TushareObservationAPIError(f"Tushare API returned status code {code}")
    data = _mapping(response.get("data"), "Tushare response data")
    fields = _string_tuple(data.get("fields"), "Tushare response fields")
    if fields != config.fields:
        raise TushareObservationFieldError("Tushare response fields do not match source config")
    raw_items = data.get("items")
    if not isinstance(raw_items, list):
        raise TushareObservationParseError("Tushare response items must be an array")
    rows: list[tuple[object, ...]] = []
    for raw_row in cast(list[object], raw_items):
        if not isinstance(raw_row, list):
            raise TushareObservationParseError("Tushare response rows must be rectangular")
        row = cast(list[object], raw_row)
        if len(row) != len(fields):
            raise TushareObservationParseError("Tushare response rows must be rectangular")
        if any(not _is_scalar(value) for value in row):
            raise TushareObservationParseError("Tushare response rows must contain scalar values")
        rows.append(tuple(row))
    return _ParsedPage(fields=fields, rows=tuple(rows))


def _capture_bundle(
    capture: TushareObservationCapture,
    *,
    config: TushareObservationSourceConfig,
    request_parameters: Mapping[str, object],
) -> bytes:
    parts = [f"{TUSHARE_OBSERVATION_CAPTURE_SCHEMA}\n".encode()]
    parts.extend(
        (
            canonical_json_bytes(
                {
                    "source_id": config.source_id,
                    "source_config_id": config.source_config_id,
                    "request_parameters": dict(request_parameters),
                }
            ),
            b"\n",
        )
    )
    for item in capture.pages:
        header = canonical_json_bytes(
            {
                "page": item.page,
                "api_name": config.api_name,
                "request_parameters": item.request_parameters,
                "fields": list(config.fields),
                "body_size": len(item.response_body),
                "body_sha256": sha256(item.response_body).hexdigest(),
            }
        )
        parts.extend((header, b"\n", item.response_body, b"\n"))
    return b"".join(parts)


def summarize_tushare_observation_capture_usage(
    payload: bytes,
) -> TushareObservationCaptureUsage:
    """Read exact page and byte counts without decoding licensed response content."""

    prefix = f"{TUSHARE_OBSERVATION_CAPTURE_SCHEMA}\n".encode()
    if not payload.startswith(prefix):
        raise TushareObservationParseError("Tushare capture bundle prefix is invalid")
    cursor = len(prefix)

    def read_line() -> bytes:
        nonlocal cursor
        newline = payload.find(b"\n", cursor)
        if newline < 0:
            raise TushareObservationParseError("Tushare capture bundle line is incomplete")
        line = payload[cursor:newline]
        cursor = newline + 1
        return line

    try:
        metadata = json.loads(read_line())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TushareObservationParseError("Tushare capture metadata is invalid") from error
    if not isinstance(metadata, dict):
        raise TushareObservationParseError("Tushare capture metadata is invalid")

    request_count = 0
    response_bytes = 0
    while cursor < len(payload):
        try:
            header = json.loads(read_line())
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise TushareObservationParseError("Tushare capture page header is invalid") from error
        typed_header = _mapping(header, "Tushare capture page header")
        body_size = typed_header.get("body_size")
        body_hash = typed_header.get("body_sha256")
        if (
            isinstance(body_size, bool)
            or not isinstance(body_size, int)
            or body_size < 1
            or not isinstance(body_hash, str)
        ):
            raise TushareObservationParseError("Tushare capture page size is invalid")
        body = payload[cursor : cursor + body_size]
        if len(body) != body_size or sha256(body).hexdigest() != body_hash:
            raise TushareObservationParseError("Tushare capture page body is invalid")
        cursor += body_size
        if cursor >= len(payload) or payload[cursor : cursor + 1] != b"\n":
            raise TushareObservationParseError("Tushare capture page delimiter is invalid")
        cursor += 1
        request_count += 1
        response_bytes += body_size
    return TushareObservationCaptureUsage(
        request_count=request_count,
        response_bytes=response_bytes,
        capture_bytes=len(payload),
    )


def _request_parameters(
    config: TushareObservationSourceConfig,
    parameters: Mapping[str, object],
) -> dict[str, object]:
    unexpected = sorted(set(parameters) - set(config.allowed_parameters))
    if unexpected:
        raise TushareObservationSourceError(
            f"unsupported Tushare query parameters: {', '.join(unexpected)}"
        )
    _validate_public_parameters(parameters, "Tushare query parameters")
    result = config.fixed_parameters
    for key, value in parameters.items():
        if key in result and result[key] != value:
            raise TushareObservationSourceError("query parameters cannot override source bindings")
        result[key] = value
    _validate_parameter_dates(config, result)
    return _canonical_object(canonical_json_bytes(result).decode(), "Tushare request parameters")


def _page_parameters(
    request_parameters: Mapping[str, object],
    *,
    offset: int,
    limit: int,
) -> dict[str, object]:
    if offset < 0 or limit < 1:
        raise ValueError("Tushare pagination parameters are invalid")
    return _canonical_object(
        canonical_json_bytes({**request_parameters, "offset": offset, "limit": limit}).decode(),
        "Tushare page parameters",
    )


def _request_body(
    *,
    api_name: str,
    token: str,
    parameters: Mapping[str, object],
    fields: tuple[str, ...],
) -> bytes:
    return json.dumps(
        {
            "api_name": api_name,
            "token": token,
            "params": dict(parameters),
            "fields": ",".join(fields),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _failed_capture(
    *,
    source_id: str,
    request_parameters: Mapping[str, object],
    retrieved_at: datetime,
    status: DataFetchStatus,
    error_kind: str,
) -> TushareObservationCapture:
    return TushareObservationCapture(
        source_id=source_id,
        request_parameters_json=canonical_json_bytes(request_parameters).decode(),
        retrieved_at=retrieved_at,
        pages=(),
        coverage_complete=False,
        failure_status=status,
        error_kind=error_kind,
    )


def _validate_row_temporal_fields(
    values: Mapping[str, object],
    *,
    config: TushareObservationSourceConfig,
) -> None:
    # Full-row content identity includes typed absence; optional dates stay optional.
    primary_keys: set[str] = (
        set() if config.primary_key_fields == config.fields else set(config.primary_key_fields)
    )
    for field_name in config.date_fields:
        value = values[field_name]
        if _missing_temporal_value(value):
            if field_name in primary_keys:
                raise TushareObservationParseError(
                    f"Tushare primary-key date field {field_name} must not be empty"
                )
            continue
        _parse_tushare_date(value, field_name)
    for field_name in config.datetime_fields:
        value = values[field_name]
        if _missing_temporal_value(value):
            if field_name in primary_keys:
                raise TushareObservationParseError(
                    f"Tushare primary-key datetime field {field_name} must not be empty"
                )
            continue
        _parse_tushare_datetime(value, field_name, config=config)


def _optional_configured_datetime(
    values: Mapping[str, object],
    field_name: str | None,
    *,
    config: TushareObservationSourceConfig,
) -> datetime | None:
    if field_name is None or _missing_temporal_value(values[field_name]):
        return None
    return _parse_tushare_datetime(values[field_name], field_name, config=config)


def _validate_parameter_dates(
    config: TushareObservationSourceConfig,
    parameters: Mapping[str, object],
) -> None:
    parsed: dict[str, date | datetime] = {}
    for name in ("start_date", "end_date", "trade_date", "report_date", "list_date"):
        value = parameters.get(name)
        if value is None:
            continue
        parsed[name] = (
            _parse_tushare_datetime(value, name, config=config)
            if config.api_name in {"news", "major_news"} and name in {"start_date", "end_date"}
            else _parse_tushare_date(value, name)
        )
    monthly = parameters.get("m")
    if monthly is not None:
        _parse_tushare_date(monthly, "m")
    start = parsed.get("start_date")
    end = parsed.get("end_date")
    if start is not None and end is not None and start > end:
        raise TushareObservationSourceError("Tushare start_date must not be after end_date")


def _parse_tushare_date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise TushareObservationParseError(f"Tushare {name} must be a date string")
    formats = ("%Y%m%d", "%Y%m", "%Y-%m-%d")
    for pattern in formats:
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    raise TushareObservationParseError(f"Tushare {name} is not a supported date")


def _parse_tushare_datetime(
    value: object,
    name: str,
    *,
    config: TushareObservationSourceConfig,
) -> datetime:
    if not isinstance(value, str):
        raise TushareObservationParseError(f"Tushare {name} must be a datetime string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise TushareObservationParseError(
                f"Tushare {name} is not a supported datetime"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(config.source_timezone))
    require_aware(parsed, f"Tushare {name}")
    return parsed.astimezone(UTC)


def _time_semantics(config: TushareObservationSourceConfig) -> str:
    if config.api_name == "cn_schedule":
        return (
            "Schedule observation only; it does not establish an original release, revision, "
            "or historical availability. Availability and authority are actual local receipt only."
        )
    if config.aggregator_timestamp_field is not None:
        return (
            "Tushare update timestamps are aggregator metadata, not publisher publication or "
            "authority history; availability and authority are actual local receipt only."
        )
    if config.publisher_time_field is not None:
        return (
            "The publisher-reported time is retained as reported; Tushare receipt is not "
            "publisher publication or authority history. Availability and authority are actual "
            "local receipt only."
        )
    return (
        "Tushare is an explicit aggregator. This receipt does not establish publisher publication "
        "or authority history; availability and authority are actual local receipt only."
    )


def _permission_denied(code: int, message: object) -> bool:
    if code in {-2001, -2002, -2003, 401, 403}:
        return True
    if not isinstance(message, str):
        return False
    lowered = message.casefold()
    return "permission" in lowered or "权限" in message or "积分" in message


def _post_json(endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
    request = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(payload) > _MAX_RESPONSE_BYTES:
                raise TushareObservationResponseTooLargeError(
                    "Tushare HTTPS response exceeded its byte limit"
                )
            return payload
    except HTTPError as exc:
        raise TushareObservationTransportError(
            "Tushare HTTPS request returned an HTTP error"
        ) from exc
    except (URLError, TimeoutError) as exc:
        raise TushareObservationTransportError("Tushare HTTPS request failed") from exc


def _source_schema_errors(payload: object) -> tuple[str, ...]:
    schema_path = (
        Path(__file__).resolve().parent / "schemas" / "tushare-observation-source.schema.json"
    )
    if not schema_path.is_file():
        schema_path = (
            Path(__file__).resolve().parents[2]
            / "schemas"
            / "tushare-observation-source.schema.json"
        )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = cast(
        _ContractValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
    errors = sorted(validator.iter_errors(payload), key=lambda item: (item.json_path, item.message))
    return tuple(f"{item.json_path}: {item.message}" for item in errors)


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TushareObservationParseError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TushareObservationParseError(f"{name} must be an object")
    return dict(cast(Mapping[str, object], raw))


class _ContractValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


def _canonical_object(value: str, name: str) -> dict[str, object]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be valid JSON") from exc
    result = _mapping(parsed, name)
    if canonical_json_bytes(result).decode() != value:
        raise ValueError(f"{name} must use canonical JSON")
    return result


def _string(value: Mapping[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise TushareObservationParseError(f"{key} must be a non-empty string")
    return result


def _nullable_string(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise TushareObservationParseError(f"{name} must be a non-empty string or null")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TushareObservationParseError(f"{name} must be an array")
    result: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str) or not item:
            raise TushareObservationParseError(f"{name} must contain non-empty strings")
        result.append(item)
    return tuple(result)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TushareObservationParseError(f"{name} must be an integer")
    return value


def _is_scalar(value: object) -> bool:
    if isinstance(value, (dict, list)):
        return False
    if isinstance(value, float) and not math.isfinite(value):
        return False
    return value is None or isinstance(value, (str, int, float, bool))


def _missing_temporal_value(value: object) -> bool:
    return value is None or value == ""


def _validate_public_parameters(parameters: Mapping[str, object], name: str) -> None:
    for key, value in parameters.items():
        if (
            not key
            or key != key.strip()
            or any(part in key.casefold() for part in _SENSITIVE_PARAMETER_PARTS)
        ):
            raise TushareObservationSourceError(f"{name} cannot include credential-like fields")
        if not _is_scalar(value):
            raise TushareObservationSourceError(f"{name} values must be scalar")


def _identifier(value: str, name: str) -> None:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must use lowercase letters, digits, dot, dash, or underscore")


def _unique_identifiers(values: tuple[str, ...], name: str) -> None:
    if not values:
        raise ValueError(f"{name} must not be empty")
    for value in values:
        _identifier(value, name)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _optional_unique_identifiers(values: tuple[str, ...], name: str) -> None:
    for value in values:
        _identifier(value, name)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _https_url(value: str, name: str) -> None:
    _trimmed(value, name)
    if not value.startswith("https://"):
        raise ValueError(f"{name} must use HTTPS")
    if "@" in value.split("//", 1)[1].split("/", 1)[0]:
        raise ValueError(f"{name} must not contain credentials")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    require_aware(value, "Tushare observation clock")
    return value.astimezone(UTC)
