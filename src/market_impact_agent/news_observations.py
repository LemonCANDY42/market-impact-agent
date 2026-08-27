from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware

NEWS_OBSERVATION_BATCH_SCHEMA = "market-impact.news-observation-batch.v1"


class NewsQueryMode(StrEnum):
    HISTORICAL = "historical"
    MASKED_REPLAY = "masked_replay"


class NewsFetchStatus(StrEnum):
    DATA = "data"
    NO_DATA = "no_data"
    NOT_CONFIGURED = "not_configured"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"


class NewsRejectionReason(StrEnum):
    MISSING_PUBLISHED_AT = "missing_published_at"
    BEFORE_WINDOW = "before_window"
    AT_OR_AFTER_WINDOW_END = "at_or_after_window_end"
    MISSING_AVAILABLE_AT = "missing_available_at"
    AVAILABLE_AT_OR_AFTER_WINDOW_END = "available_at_or_after_window_end"
    SOURCE_UPDATED_AT_OR_AFTER_WINDOW_END = "source_updated_at_or_after_window_end"
    DUPLICATE_LINEAGE = "duplicate_lineage"
    SOURCE_LIMIT = "source_limit"


@dataclass(frozen=True, slots=True)
class NewsSourceRegistration:
    provider_id: str
    provider_version: str
    source_id: str
    source_version: str

    def __post_init__(self) -> None:
        for name in ("provider_id", "provider_version", "source_id", "source_version"):
            _nonempty(cast(str, getattr(self, name)), name)

    @property
    def source_key(self) -> str:
        return f"{self.provider_id}:{self.source_id}"

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "source_id": self.source_id,
            "source_version": self.source_version,
        }


@dataclass(frozen=True, slots=True)
class NewsQuery:
    query_id: str
    mode: NewsQueryMode
    start_at: datetime
    end_at: datetime
    terms: tuple[str, ...]
    limit_per_source: int
    sources: tuple[NewsSourceRegistration, ...]

    def __post_init__(self) -> None:
        _strict_utc(self.start_at, "news query start_at")
        _strict_utc(self.end_at, "news query end_at")
        if self.end_at <= self.start_at:
            raise ValueError("news query end_at must be after start_at")
        _unique_nonempty(self.terms, "news query terms")
        if self.limit_per_source < 1:
            raise ValueError("news query limit_per_source must be positive")
        if not self.sources:
            raise ValueError("news query requires at least one registered source")
        source_keys = tuple(item.source_key for item in self.sources)
        if len(source_keys) != len(set(source_keys)):
            raise ValueError("news query registered source keys must be unique")
        if self.query_id != self.expected_query_id:
            raise ValueError("news query_id does not match content")

    @property
    def query_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_query_id(self) -> str:
        return f"news-query-{self.query_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "start_at": _timestamp(self.start_at),
            "end_at": _timestamp(self.end_at),
            "terms": list(self.terms),
            "limit_per_source": self.limit_per_source,
            "sources": [item.to_dict() for item in self.sources],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "query_id": self.query_id}

    @classmethod
    def build(
        cls,
        *,
        mode: NewsQueryMode,
        start_at: datetime,
        end_at: datetime,
        terms: tuple[str, ...],
        limit_per_source: int,
        sources: tuple[NewsSourceRegistration, ...],
    ) -> NewsQuery:
        core = {
            "mode": mode.value,
            "start_at": _timestamp(start_at),
            "end_at": _timestamp(end_at),
            "terms": list(terms),
            "limit_per_source": limit_per_source,
            "sources": [item.to_dict() for item in sources],
        }
        return cls(
            query_id=f"news-query-{canonical_hash(core)}",
            mode=mode,
            start_at=start_at,
            end_at=end_at,
            terms=terms,
            limit_per_source=limit_per_source,
            sources=sources,
        )


@dataclass(frozen=True, slots=True)
class FetchedNewsRecord:
    provider_id: str
    source_id: str
    upstream_record_id: str
    lineage_id: str
    title: str
    raw_content_hash: str
    published_at: datetime | None
    source_updated_at: datetime | None
    available_at: datetime | None

    def __post_init__(self) -> None:
        for name in (
            "provider_id",
            "source_id",
            "upstream_record_id",
            "lineage_id",
            "title",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        _sha256(self.raw_content_hash, "raw_content_hash")
        for name in ("published_at", "source_updated_at", "available_at"):
            value = cast(datetime | None, getattr(self, name))
            if value is not None:
                _strict_utc(value, name)
        if (
            self.published_at is not None
            and self.available_at is not None
            and self.available_at < self.published_at
        ):
            raise ValueError("news available_at cannot precede published_at")
        if (
            self.source_updated_at is not None
            and self.available_at is not None
            and self.available_at < self.source_updated_at
        ):
            raise ValueError("news available_at cannot precede source_updated_at")


@dataclass(frozen=True, slots=True)
class NewsSourceFetch:
    source_key: str
    status: NewsFetchStatus
    checked_at: datetime
    completed_at: datetime
    raw_content_hash: str | None
    records: tuple[FetchedNewsRecord, ...]
    error_class: str | None = None
    error_summary: str | None = None

    def __post_init__(self) -> None:
        _nonempty(self.source_key, "news source fetch source_key")
        _strict_utc(self.checked_at, "news source fetch checked_at")
        _strict_utc(self.completed_at, "news source fetch completed_at")
        if self.completed_at < self.checked_at:
            raise ValueError("news source fetch completion cannot precede check")
        if self.status is NewsFetchStatus.DATA:
            if not self.records or self.raw_content_hash is None:
                raise ValueError("data news fetch requires records and raw_content_hash")
        elif self.status is NewsFetchStatus.NO_DATA:
            if self.records or self.raw_content_hash is None:
                raise ValueError("no_data news fetch requires an empty hashed response")
        elif self.records or self.raw_content_hash is not None:
            raise ValueError("unavailable news fetch cannot contain records or content hash")
        if self.raw_content_hash is not None:
            _sha256(self.raw_content_hash, "news source fetch raw_content_hash")
        failed = self.status in {NewsFetchStatus.RATE_LIMITED, NewsFetchStatus.ERROR}
        if failed and (self.error_class is None or self.error_summary is None):
            raise ValueError("failed news fetch requires error_class and error_summary")
        if not failed and (self.error_class is not None or self.error_summary is not None):
            raise ValueError("non-failed news fetch cannot contain error fields")
        if self.error_class is not None:
            _nonempty(self.error_class, "news source fetch error_class")
        if self.error_summary is not None:
            _nonempty(self.error_summary, "news source fetch error_summary")


@dataclass(frozen=True, slots=True)
class NewsObservation:
    observation_id: str
    provider_id: str
    provider_version: str
    source_id: str
    source_version: str
    upstream_record_id: str
    lineage_id: str
    title: str
    raw_content_hash: str
    published_at: datetime
    source_updated_at: datetime | None
    available_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "provider_id",
            "provider_version",
            "source_id",
            "source_version",
            "upstream_record_id",
            "lineage_id",
            "title",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        _sha256(self.raw_content_hash, "news observation raw_content_hash")
        _strict_utc(self.published_at, "news observation published_at")
        _strict_utc(self.available_at, "news observation available_at")
        if self.source_updated_at is not None:
            _strict_utc(self.source_updated_at, "news observation source_updated_at")
        if self.available_at < self.published_at:
            raise ValueError("news observation available_at cannot precede published_at")
        if self.source_updated_at is not None and self.available_at < self.source_updated_at:
            raise ValueError("news observation available_at cannot precede source_updated_at")
        if self.observation_id != self.expected_observation_id:
            raise ValueError("news observation_id does not match content")

    @property
    def expected_observation_id(self) -> str:
        return f"news-observation-{canonical_hash(self.core_dict())}"

    @property
    def source_key(self) -> str:
        return f"{self.provider_id}:{self.source_id}"

    def core_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "source_id": self.source_id,
            "source_version": self.source_version,
            "upstream_record_id": self.upstream_record_id,
            "lineage_id": self.lineage_id,
            "title": self.title,
            "raw_content_hash": self.raw_content_hash,
            "published_at": _timestamp(self.published_at),
            "source_updated_at": _optional_timestamp(self.source_updated_at),
            "available_at": _timestamp(self.available_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "observation_id": self.observation_id}


@dataclass(frozen=True, slots=True)
class NewsFetchAttempt:
    source_key: str
    status: NewsFetchStatus
    checked_at: datetime
    completed_at: datetime
    raw_content_hash: str | None
    raw_record_count: int | None
    accepted_record_count: int | None
    rejection_counts: tuple[tuple[NewsRejectionReason, int], ...]
    error_class: str | None
    error_summary: str | None

    def __post_init__(self) -> None:
        _nonempty(self.source_key, "news fetch attempt source_key")
        _strict_utc(self.checked_at, "news fetch attempt checked_at")
        _strict_utc(self.completed_at, "news fetch attempt completed_at")
        if self.completed_at < self.checked_at:
            raise ValueError("news fetch attempt completion cannot precede check")
        reasons = tuple(reason for reason, _count in self.rejection_counts)
        if len(reasons) != len(set(reasons)):
            raise ValueError("news fetch attempt rejection reasons must be unique")
        if any(count < 1 for _reason, count in self.rejection_counts):
            raise ValueError("news fetch attempt rejection counts must be positive")
        if self.status is NewsFetchStatus.DATA:
            if (
                self.raw_content_hash is None
                or self.raw_record_count is None
                or self.raw_record_count < 1
                or self.accepted_record_count is None
            ):
                raise ValueError("data news fetch attempt fields are incomplete")
        elif self.status is NewsFetchStatus.NO_DATA:
            if (
                self.raw_content_hash is None
                or self.raw_record_count != 0
                or self.accepted_record_count != 0
                or self.rejection_counts
            ):
                raise ValueError("no_data news fetch attempt fields are invalid")
        elif (
            self.raw_content_hash is not None
            or self.raw_record_count is not None
            or self.accepted_record_count is not None
            or self.rejection_counts
        ):
            raise ValueError("unavailable news fetch attempt cannot contain record fields")
        if self.raw_content_hash is not None:
            _sha256(self.raw_content_hash, "news fetch attempt raw_content_hash")
        if self.status is NewsFetchStatus.DATA:
            if self.accepted_record_count is None or self.raw_record_count is None:
                raise AssertionError("validated data attempt lacks counts")
            rejected = sum(count for _reason, count in self.rejection_counts)
            if (
                self.accepted_record_count < 0
                or self.accepted_record_count + rejected != self.raw_record_count
            ):
                raise ValueError("news fetch attempt counts do not reconcile")
        failed = self.status in {NewsFetchStatus.RATE_LIMITED, NewsFetchStatus.ERROR}
        if failed and (self.error_class is None or self.error_summary is None):
            raise ValueError("failed news fetch attempt requires error fields")
        if not failed and (self.error_class is not None or self.error_summary is not None):
            raise ValueError("non-failed news fetch attempt cannot contain error fields")
        if self.error_class is not None:
            _nonempty(self.error_class, "news fetch attempt error_class")
        if self.error_summary is not None:
            _nonempty(self.error_summary, "news fetch attempt error_summary")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_key": self.source_key,
            "status": self.status.value,
            "checked_at": _timestamp(self.checked_at),
            "completed_at": _timestamp(self.completed_at),
            "raw_content_hash": self.raw_content_hash,
            "raw_record_count": self.raw_record_count,
            "accepted_record_count": self.accepted_record_count,
            "rejection_counts": {reason.value: count for reason, count in self.rejection_counts},
            "error_class": self.error_class,
            "error_summary": self.error_summary,
        }


@dataclass(frozen=True, slots=True)
class NewsObservationBatch:
    batch_id: str
    query: NewsQuery
    attempts: tuple[NewsFetchAttempt, ...]
    observations: tuple[NewsObservation, ...]

    def __post_init__(self) -> None:
        expected_keys = tuple(item.source_key for item in self.query.sources)
        if tuple(item.source_key for item in self.attempts) != expected_keys:
            raise ValueError("news batch attempts do not match registered source order")
        observation_ids = tuple(item.observation_id for item in self.observations)
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("news batch observation_id values must be unique")
        lineage_ids = tuple(item.lineage_id for item in self.observations)
        if len(lineage_ids) != len(set(lineage_ids)):
            raise ValueError("news batch lineage_id values must be unique")
        if any(
            not self.query.start_at <= item.published_at < self.query.end_at
            or (item.source_updated_at is not None and item.source_updated_at >= self.query.end_at)
            or item.available_at >= self.query.end_at
            for item in self.observations
        ):
            raise ValueError("news batch contains an observation outside its point-in-time window")
        registrations = {item.source_key: item for item in self.query.sources}
        accepted_counts: Counter[str] = Counter()
        for observation in self.observations:
            registration = registrations.get(observation.source_key)
            if registration is None:
                raise ValueError("news batch observation source is not registered")
            if (
                observation.provider_version != registration.provider_version
                or observation.source_version != registration.source_version
            ):
                raise ValueError("news batch observation source version is not registered")
            accepted_counts[observation.source_key] += 1
        for attempt in self.attempts:
            if attempt.accepted_record_count is not None and (
                accepted_counts[attempt.source_key] != attempt.accepted_record_count
            ):
                raise ValueError("news batch accepted counts do not match observations")
        if self.batch_id != self.expected_batch_id:
            raise ValueError("news batch_id does not match content")

    @property
    def batch_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_batch_id(self) -> str:
        return f"news-batch-{self.batch_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": NEWS_OBSERVATION_BATCH_SCHEMA,
            "query": self.query.to_dict(),
            "attempts": [item.to_dict() for item in self.attempts],
            "observations": [item.to_dict() for item in self.observations],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "batch_id": self.batch_id}


def build_news_observation_batch(
    *, query: NewsQuery, fetches: tuple[NewsSourceFetch, ...]
) -> NewsObservationBatch:
    expected_keys = tuple(item.source_key for item in query.sources)
    if tuple(item.source_key for item in fetches) != expected_keys:
        raise ValueError("news fetches do not match registered source order")

    observations: list[NewsObservation] = []
    attempts: list[NewsFetchAttempt] = []
    seen_lineage: set[str] = set()
    registrations = {item.source_key: item for item in query.sources}

    for fetch in fetches:
        registration = registrations[fetch.source_key]
        rejection_counts: Counter[NewsRejectionReason] = Counter()
        accepted_for_source = 0
        for record in fetch.records:
            if (
                record.provider_id != registration.provider_id
                or record.source_id != registration.source_id
            ):
                raise ValueError("news record does not match its registered Provider and source")
            reason = _temporal_rejection(query, record)
            if reason is not None:
                rejection_counts[reason] += 1
                continue
            if record.lineage_id in seen_lineage:
                rejection_counts[NewsRejectionReason.DUPLICATE_LINEAGE] += 1
                continue
            if accepted_for_source >= query.limit_per_source:
                rejection_counts[NewsRejectionReason.SOURCE_LIMIT] += 1
                continue
            if record.published_at is None or record.available_at is None:
                raise AssertionError("eligible news record lacks required times")
            core = {
                "provider_id": registration.provider_id,
                "provider_version": registration.provider_version,
                "source_id": registration.source_id,
                "source_version": registration.source_version,
                "upstream_record_id": record.upstream_record_id,
                "lineage_id": record.lineage_id,
                "title": record.title,
                "raw_content_hash": record.raw_content_hash,
                "published_at": _timestamp(record.published_at),
                "source_updated_at": _optional_timestamp(record.source_updated_at),
                "available_at": _timestamp(record.available_at),
            }
            observations.append(
                NewsObservation(
                    observation_id=f"news-observation-{canonical_hash(core)}",
                    provider_id=registration.provider_id,
                    provider_version=registration.provider_version,
                    source_id=registration.source_id,
                    source_version=registration.source_version,
                    upstream_record_id=record.upstream_record_id,
                    lineage_id=record.lineage_id,
                    title=record.title,
                    raw_content_hash=record.raw_content_hash,
                    published_at=record.published_at,
                    source_updated_at=record.source_updated_at,
                    available_at=record.available_at,
                )
            )
            accepted_for_source += 1
            seen_lineage.add(record.lineage_id)
        attempt = NewsFetchAttempt(
            source_key=fetch.source_key,
            status=fetch.status,
            checked_at=fetch.checked_at,
            completed_at=fetch.completed_at,
            raw_content_hash=fetch.raw_content_hash,
            raw_record_count=(
                None
                if fetch.status not in {NewsFetchStatus.DATA, NewsFetchStatus.NO_DATA}
                else len(fetch.records)
            ),
            accepted_record_count=(
                None
                if fetch.status not in {NewsFetchStatus.DATA, NewsFetchStatus.NO_DATA}
                else accepted_for_source
            ),
            rejection_counts=tuple(
                sorted(rejection_counts.items(), key=lambda item: item[0].value)
            ),
            error_class=fetch.error_class,
            error_summary=fetch.error_summary,
        )
        attempts.append(attempt)

    core = {
        "schema_version": NEWS_OBSERVATION_BATCH_SCHEMA,
        "query": query.to_dict(),
        "attempts": [item.to_dict() for item in attempts],
        "observations": [item.to_dict() for item in observations],
    }
    return NewsObservationBatch(
        batch_id=f"news-batch-{canonical_hash(core)}",
        query=query,
        attempts=tuple(attempts),
        observations=tuple(observations),
    )


def news_observation_batch_from_dict(value: object) -> NewsObservationBatch:
    payload = _object(value, "News Observation Batch")
    _closed(
        payload,
        {"schema_version", "batch_id", "query", "attempts", "observations"},
        "News Observation Batch",
    )
    if _string(payload, "schema_version") != NEWS_OBSERVATION_BATCH_SCHEMA:
        raise ValueError("unsupported News Observation Batch schema_version")
    query = _query(_object(payload.get("query"), "news query"))
    batch = NewsObservationBatch(
        batch_id=_string(payload, "batch_id"),
        query=query,
        attempts=tuple(_attempt(item) for item in _object_list(payload, "attempts")),
        observations=tuple(_observation(item) for item in _object_list(payload, "observations")),
    )
    if batch.to_dict() != payload:
        raise ValueError("News Observation Batch does not match canonical contract")
    return batch


def _query(payload: dict[str, object]) -> NewsQuery:
    _closed(
        payload,
        {"query_id", "mode", "start_at", "end_at", "terms", "limit_per_source", "sources"},
        "news query",
    )
    return NewsQuery(
        query_id=_string(payload, "query_id"),
        mode=NewsQueryMode(_string(payload, "mode")),
        start_at=_datetime(payload, "start_at"),
        end_at=_datetime(payload, "end_at"),
        terms=_string_tuple(payload, "terms"),
        limit_per_source=_integer(payload, "limit_per_source"),
        sources=tuple(_source(item) for item in _object_list(payload, "sources")),
    )


def _source(payload: dict[str, object]) -> NewsSourceRegistration:
    _closed(
        payload,
        {"provider_id", "provider_version", "source_id", "source_version"},
        "news source registration",
    )
    return NewsSourceRegistration(
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        source_id=_string(payload, "source_id"),
        source_version=_string(payload, "source_version"),
    )


def _attempt(payload: dict[str, object]) -> NewsFetchAttempt:
    _closed(
        payload,
        {
            "source_key",
            "status",
            "checked_at",
            "completed_at",
            "raw_content_hash",
            "raw_record_count",
            "accepted_record_count",
            "rejection_counts",
            "error_class",
            "error_summary",
        },
        "news fetch attempt",
    )
    raw_rejections = _object(payload.get("rejection_counts"), "rejection_counts")
    rejection_counts: list[tuple[NewsRejectionReason, int]] = []
    for reason, count in raw_rejections.items():
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("news rejection count must be an integer")
        rejection_counts.append((NewsRejectionReason(reason), count))
    return NewsFetchAttempt(
        source_key=_string(payload, "source_key"),
        status=NewsFetchStatus(_string(payload, "status")),
        checked_at=_datetime(payload, "checked_at"),
        completed_at=_datetime(payload, "completed_at"),
        raw_content_hash=_nullable_string(payload, "raw_content_hash"),
        raw_record_count=_nullable_integer(payload, "raw_record_count"),
        accepted_record_count=_nullable_integer(payload, "accepted_record_count"),
        rejection_counts=tuple(sorted(rejection_counts, key=lambda item: item[0].value)),
        error_class=_nullable_string(payload, "error_class"),
        error_summary=_nullable_string(payload, "error_summary"),
    )


def _observation(payload: dict[str, object]) -> NewsObservation:
    _closed(
        payload,
        {
            "observation_id",
            "provider_id",
            "provider_version",
            "source_id",
            "source_version",
            "upstream_record_id",
            "lineage_id",
            "title",
            "raw_content_hash",
            "published_at",
            "source_updated_at",
            "available_at",
        },
        "news observation",
    )
    return NewsObservation(
        observation_id=_string(payload, "observation_id"),
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        source_id=_string(payload, "source_id"),
        source_version=_string(payload, "source_version"),
        upstream_record_id=_string(payload, "upstream_record_id"),
        lineage_id=_string(payload, "lineage_id"),
        title=_string(payload, "title"),
        raw_content_hash=_string(payload, "raw_content_hash"),
        published_at=_datetime(payload, "published_at"),
        source_updated_at=_nullable_datetime(payload, "source_updated_at"),
        available_at=_datetime(payload, "available_at"),
    )


def _temporal_rejection(query: NewsQuery, record: FetchedNewsRecord) -> NewsRejectionReason | None:
    if record.published_at is None:
        return NewsRejectionReason.MISSING_PUBLISHED_AT
    if record.published_at < query.start_at:
        return NewsRejectionReason.BEFORE_WINDOW
    if record.published_at >= query.end_at:
        return NewsRejectionReason.AT_OR_AFTER_WINDOW_END
    if record.source_updated_at is not None and record.source_updated_at >= query.end_at:
        return NewsRejectionReason.SOURCE_UPDATED_AT_OR_AFTER_WINDOW_END
    if record.available_at is None:
        return NewsRejectionReason.MISSING_AVAILABLE_AT
    if record.available_at >= query.end_at:
        return NewsRejectionReason.AVAILABLE_AT_OR_AFTER_WINDOW_END
    return None


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if not values or len(values) != len(set(values)):
        raise ValueError(f"{name} must contain unique values")
    for value in values:
        _nonempty(value, name)


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 hex digest")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], value)


def _object_list(payload: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, name) for item in cast(list[object], value))


def _closed(payload: dict[str, object], expected: set[str], name: str) -> None:
    if set(payload) != expected:
        raise ValueError(
            f"{name} fields do not match contract: "
            f"missing={sorted(expected - set(payload))}, extra={sorted(set(payload) - expected)}"
        )


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _nullable_string(payload: dict[str, object], name: str) -> str | None:
    return None if payload.get(name) is None else _string(payload, name)


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _nullable_integer(payload: dict[str, object], name: str) -> int | None:
    return None if payload.get(name) is None else _integer(payload, name)


def _string_tuple(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], value))


def _datetime(payload: dict[str, object], name: str) -> datetime:
    value = _string(payload, name)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _strict_utc(parsed, name)
    return parsed


def _nullable_datetime(payload: dict[str, object], name: str) -> datetime | None:
    return None if payload.get(name) is None else _datetime(payload, name)
