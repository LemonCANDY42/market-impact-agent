import json
from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from typing import cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.news_observations import (
    FetchedNewsRecord,
    NewsFetchStatus,
    NewsQuery,
    NewsQueryMode,
    NewsRejectionReason,
    NewsSourceFetch,
    NewsSourceRegistration,
    build_news_observation_batch,
    news_observation_batch_from_dict,
)


def _source(provider_id: str, source_id: str) -> NewsSourceRegistration:
    return NewsSourceRegistration(
        provider_id=provider_id,
        provider_version="adapter-v1",
        source_id=source_id,
        source_version="source-v3",
    )


def _query(
    *, limit: int = 2, sources: tuple[NewsSourceRegistration, ...] | None = None
) -> NewsQuery:
    return NewsQuery.build(
        mode=NewsQueryMode.MASKED_REPLAY,
        start_at=datetime(2031, 1, 1, tzinfo=UTC),
        end_at=datetime(2031, 1, 2, tzinfo=UTC),
        terms=("physical supply", "recovery"),
        limit_per_source=limit,
        sources=sources or (_source("provider-a", "wire-a"), _source("provider-b", "wire-b")),
    )


def _record(
    *,
    provider_id: str = "provider-a",
    source_id: str = "wire-a",
    upstream_record_id: str = "record-1",
    lineage_id: str = "lineage-1",
    title: str = "Supply update",
    published_at: datetime | None = datetime(2031, 1, 1, 12, tzinfo=UTC),
    source_updated_at: datetime | None = None,
    available_at: datetime | None = datetime(2031, 1, 1, 12, 5, tzinfo=UTC),
) -> FetchedNewsRecord:
    return FetchedNewsRecord(
        provider_id=provider_id,
        source_id=source_id,
        upstream_record_id=upstream_record_id,
        lineage_id=lineage_id,
        title=title,
        raw_content_hash=sha256(upstream_record_id.encode()).hexdigest(),
        published_at=published_at,
        source_updated_at=published_at if source_updated_at is None else source_updated_at,
        available_at=available_at,
    )


def _fetch(
    source_key: str,
    *,
    status: NewsFetchStatus,
    records: tuple[FetchedNewsRecord, ...] = (),
) -> NewsSourceFetch:
    return NewsSourceFetch(
        source_key=source_key,
        status=status,
        checked_at=datetime(2031, 1, 2, 1, tzinfo=UTC),
        completed_at=datetime(2031, 1, 2, 1, 0, 1, tzinfo=UTC),
        raw_content_hash=(
            sha256(f"raw-{source_key}".encode()).hexdigest()
            if status in {NewsFetchStatus.DATA, NewsFetchStatus.NO_DATA}
            else None
        ),
        records=records,
        error_class="RateLimit" if status is NewsFetchStatus.RATE_LIMITED else None,
        error_summary="retry later" if status is NewsFetchStatus.RATE_LIMITED else None,
    )


def test_batch_is_content_identified_schema_valid_and_roundtrips() -> None:
    query = _query()
    batch = build_news_observation_batch(
        query=query,
        fetches=(
            _fetch("provider-a:wire-a", status=NewsFetchStatus.DATA, records=(_record(),)),
            _fetch("provider-b:wire-b", status=NewsFetchStatus.NO_DATA),
        ),
    )
    payload = batch.to_dict()

    assert batch.batch_id == f"news-batch-{batch.batch_hash}"
    assert batch.query.query_id == f"news-query-{batch.query.query_hash}"
    assert validate_agent_contract(payload, "news-observation-batch.schema.json") == ()
    assert news_observation_batch_from_dict(json.loads(json.dumps(payload))) == batch
    assert [attempt.status for attempt in batch.attempts] == [
        NewsFetchStatus.DATA,
        NewsFetchStatus.NO_DATA,
    ]


def test_future_shifted_replay_rejects_undated_and_out_of_window_before_limit() -> None:
    query = _query(limit=1)
    records = (
        _record(upstream_record_id="undated", lineage_id="undated", published_at=None),
        _record(
            upstream_record_id="at-end",
            lineage_id="at-end",
            published_at=query.end_at,
            available_at=query.end_at,
        ),
        _record(upstream_record_id="valid-1", lineage_id="valid-1"),
        _record(upstream_record_id="valid-2", lineage_id="valid-2"),
    )
    batch = build_news_observation_batch(
        query=query,
        fetches=(
            _fetch("provider-a:wire-a", status=NewsFetchStatus.DATA, records=records),
            _fetch("provider-b:wire-b", status=NewsFetchStatus.NO_DATA),
        ),
    )

    assert [item.upstream_record_id for item in batch.observations] == ["valid-1"]
    assert dict(batch.attempts[0].rejection_counts) == {
        NewsRejectionReason.AT_OR_AFTER_WINDOW_END: 1,
        NewsRejectionReason.MISSING_PUBLISHED_AT: 1,
        NewsRejectionReason.SOURCE_LIMIT: 1,
    }
    assert batch.attempts[0].raw_record_count == 4
    assert batch.attempts[0].accepted_record_count == 1


def test_batch_rejects_revision_at_exact_cutoff_with_typed_reason() -> None:
    query = _query()
    records = (
        _record(
            upstream_record_id="revised-at-cutoff",
            lineage_id="revised-at-cutoff",
            source_updated_at=query.end_at,
            available_at=query.end_at,
        ),
        _record(
            upstream_record_id="revised-before-cutoff",
            lineage_id="revised-before-cutoff",
            source_updated_at=query.end_at - timedelta(seconds=1),
            available_at=query.end_at - timedelta(seconds=1),
        ),
    )

    batch = build_news_observation_batch(
        query=query,
        fetches=(
            _fetch("provider-a:wire-a", status=NewsFetchStatus.DATA, records=records),
            _fetch("provider-b:wire-b", status=NewsFetchStatus.NO_DATA),
        ),
    )

    assert [item.upstream_record_id for item in batch.observations] == ["revised-before-cutoff"]
    assert dict(batch.attempts[0].rejection_counts) == {
        NewsRejectionReason.SOURCE_UPDATED_AT_OR_AFTER_WINDOW_END: 1
    }
    assert validate_agent_contract(batch.to_dict(), "news-observation-batch.schema.json") == ()


def test_record_and_parser_reject_availability_before_source_revision() -> None:
    with pytest.raises(ValueError, match="available_at cannot precede source_updated_at"):
        _record(
            source_updated_at=datetime(2031, 1, 1, 12, 6, tzinfo=UTC),
            available_at=datetime(2031, 1, 1, 12, 5, tzinfo=UTC),
        )

    query = _query()
    batch = build_news_observation_batch(
        query=query,
        fetches=(
            _fetch("provider-a:wire-a", status=NewsFetchStatus.DATA, records=(_record(),)),
            _fetch("provider-b:wire-b", status=NewsFetchStatus.NO_DATA),
        ),
    )
    payload = batch.to_dict()
    observations = cast(list[object], payload["observations"])
    observation = observations[0]
    assert isinstance(observation, dict)
    observation["source_updated_at"] = "2031-01-01T12:06:00Z"

    with pytest.raises(ValueError, match="available_at cannot precede source_updated_at"):
        news_observation_batch_from_dict(payload)


def test_lineage_dedupes_across_providers_but_equal_titles_do_not() -> None:
    query = _query(limit=3)
    first = _record(lineage_id="shared-lineage", title="Same title")
    duplicate = _record(
        provider_id="provider-b",
        source_id="wire-b",
        upstream_record_id="provider-copy",
        lineage_id="shared-lineage",
        title="Different title",
    )
    same_title_new_lineage = _record(
        provider_id="provider-b",
        source_id="wire-b",
        upstream_record_id="independent-record",
        lineage_id="independent-lineage",
        title="Same title",
    )
    batch = build_news_observation_batch(
        query=query,
        fetches=(
            _fetch("provider-a:wire-a", status=NewsFetchStatus.DATA, records=(first,)),
            _fetch(
                "provider-b:wire-b",
                status=NewsFetchStatus.DATA,
                records=(duplicate, same_title_new_lineage),
            ),
        ),
    )

    assert [item.lineage_id for item in batch.observations] == [
        "shared-lineage",
        "independent-lineage",
    ]
    assert dict(batch.attempts[1].rejection_counts) == {NewsRejectionReason.DUPLICATE_LINEAGE: 1}


@pytest.mark.parametrize(
    ("status", "error_class", "error_summary"),
    [
        (NewsFetchStatus.NOT_CONFIGURED, None, None),
        (NewsFetchStatus.RATE_LIMITED, "RateLimit", "retry later"),
        (NewsFetchStatus.ERROR, "Transport", "connection failed"),
    ],
)
def test_unavailable_fetch_statuses_are_typed_without_fake_records(
    status: NewsFetchStatus, error_class: str | None, error_summary: str | None
) -> None:
    fetch = NewsSourceFetch(
        source_key="provider-a:wire-a",
        status=status,
        checked_at=datetime(2031, 1, 2, tzinfo=UTC),
        completed_at=datetime(2031, 1, 2, tzinfo=UTC),
        raw_content_hash=None,
        records=(),
        error_class=error_class,
        error_summary=error_summary,
    )
    query = _query(sources=(_source("provider-a", "wire-a"),))
    batch = build_news_observation_batch(query=query, fetches=(fetch,))

    assert batch.observations == ()
    assert batch.attempts[0].status is status
    assert batch.attempts[0].raw_record_count is None


def test_query_requires_exact_utc_and_fetches_require_registered_order() -> None:
    with pytest.raises(ValueError, match="must use UTC"):
        NewsQuery.build(
            mode=NewsQueryMode.HISTORICAL,
            start_at=datetime(2031, 1, 1, tzinfo=timezone(timedelta(hours=8))),
            end_at=datetime(2031, 1, 2, tzinfo=timezone(timedelta(hours=8))),
            terms=("supply",),
            limit_per_source=1,
            sources=(_source("provider-a", "wire-a"),),
        )

    query = _query()
    with pytest.raises(ValueError, match="registered source order"):
        build_news_observation_batch(
            query=query,
            fetches=(
                _fetch("provider-b:wire-b", status=NewsFetchStatus.NO_DATA),
                _fetch("provider-a:wire-a", status=NewsFetchStatus.NO_DATA),
            ),
        )


def test_schema_and_parser_reject_tampered_identity_and_status_fields() -> None:
    query = _query(sources=(_source("provider-a", "wire-a"),))
    batch = build_news_observation_batch(
        query=query,
        fetches=(_fetch("provider-a:wire-a", status=NewsFetchStatus.DATA, records=(_record(),)),),
    )
    payload = batch.to_dict()
    payload["batch_id"] = f"news-batch-{'0' * 64}"
    with pytest.raises(ValueError, match="batch_id does not match content"):
        news_observation_batch_from_dict(payload)

    invalid = batch.to_dict()
    attempts = cast(list[object], invalid["attempts"])
    attempt = attempts[0]
    assert isinstance(attempt, dict)
    attempt["status"] = "not_configured"
    assert validate_agent_contract(invalid, "news-observation-batch.schema.json") == ()
    with pytest.raises(ValueError, match="unavailable news fetch attempt"):
        news_observation_batch_from_dict(invalid)
