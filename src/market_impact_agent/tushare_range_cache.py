"""Opt-in complete interval reuse for raw Tushare daily and fund_daily rows.

Coverage means the configured pagination completed, never tradability or PIT
at an earlier date. The enclosing DataInputHarness applies its original cutoff.
"""

from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
import time
import uuid
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_acquisition import AcquisitionPending, AcquisitionUncertain
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    ProviderDataResponse,
    SourceObservation,
    source_observation_from_dict,
)
from market_impact_agent.observations import ObservationProviderManifest
from market_impact_agent.tushare_observation import (
    TushareObservationError,
    TushareObservationProvider,
    load_tushare_observation_capture_bundle,
    tushare_observation_source_from_dict,
)


class TushareDailyRangeCache:
    """Harness-owned interval acquisition and saved physical response projection."""

    def __init__(self, provider: TushareObservationProvider, store: LocalDataSnapshotStore) -> None:
        self.provider = provider
        self.store = store
        with store.authority_transaction() as connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS tushare_range_owners (
                scope_id TEXT PRIMARY KEY, token TEXT, expires_at REAL, state TEXT NOT NULL)""")
            connection.execute("""CREATE TABLE IF NOT EXISTS tushare_range_responses (
                scope_id TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
                artifact_hash TEXT NOT NULL, complete INTEGER NOT NULL,
                PRIMARY KEY(scope_id, start_date, end_date))""")

    @property
    def manifest(self) -> ObservationProviderManifest:
        return self.provider.manifest

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]:
        return self.provider.public_source_config(upstream_source)

    def _scope(self, query: DataQuery, source: DataSourceBinding) -> tuple[str, date, date]:
        config = self.public_source_config(source.upstream_source)
        params = query.parameters
        if config.get("api_name") not in {"daily", "fund_daily"}:
            raise ValueError("range cache supports only raw daily and fund_daily routes")
        if set(params) != {"ts_code", "start_date", "end_date"}:
            raise ValueError("range cache requires exact ts_code/start_date/end_date parameters")
        if not isinstance(params["ts_code"], str) or not params["ts_code"]:
            raise ValueError("range cache requires one instrument")
        start = _date(params["start_date"])
        end = _date(params["end_date"])
        if start > end:
            raise ValueError("range cache start_date exceeds end_date")
        scope_id = canonical_hash(
            {
                "contract": "tushare-raw-daily-range-v1",
                "source": source.to_dict(),
                "config": dict(config),
                "capability": query.capability.value,
                "pit_lane": query.pit_lane.value,
                "source_policy_id": query.source_policy_id,
                "ts_code": params["ts_code"],
            }
        )
        if query.sources != (source,):
            raise ValueError("range projection requires its exact single source binding")
        if (
            source.provider_id != self.manifest.provider_id
            or source.provider_version != self.manifest.provider_version
            or source.manifest_hash != canonical_hash(self.manifest.to_dict())
            or source.source_config_hash != canonical_hash(config)
            or not self.manifest.enabled
            or query.capability not in self.manifest.verified_capabilities
        ):
            raise ValueError("range projection source binding mismatch")
        return scope_id, start, end

    async def acquire(
        self, *, query: DataQuery, source: DataSourceBinding, saved_only: bool = False
    ) -> tuple[tuple[str, str, str, bool], ...]:
        scope_id, start, end = self._scope(query, source)
        token = uuid.uuid4().hex
        if saved_only:
            rows = await asyncio.to_thread(self._saved_rows, scope_id)
        else:
            rows = await asyncio.to_thread(self._claim, scope_id, token)
        try:
            selected: list[tuple[str, str, str, bool]] = []
            missing = [(start, end)]
            for row in rows:
                left, right, _, complete = row
                a, b = _date(left), _date(right)
                if a > end or b < start:
                    continue
                self._verify(query, source, row)
                if not complete:
                    if saved_only:
                        raise AcquisitionUncertain(scope_id)
                    return (row,)
                selected.append(row)
                missing = _subtract(missing, a, b)
            if saved_only and missing:
                raise AcquisitionUncertain(scope_id)
            for a, b in missing:
                segment = self.segment_query(query, _format(a), _format(b))
                response = await self.provider.fetch(query=segment, source=source)
                digest = await asyncio.to_thread(self._persist, scope_id, token, a, b, response)
                row = (_format(a), _format(b), digest, response.status.completed)
                self._verify(query, source, row)
                if not response.status.completed:
                    return (row,)
                selected.append(row)
            return tuple(selected)
        except BaseException as exc:
            if saved_only and isinstance(exc, (ValueError, LookupError, OSError, TypeError)):
                raise AcquisitionUncertain(scope_id) from exc
            if not saved_only:
                await asyncio.to_thread(self._release, scope_id, token, "uncertain")
            raise
        finally:
            if not saved_only:
                await asyncio.to_thread(self._release, scope_id, token, "idle")

    def _saved_rows(self, scope_id: str) -> list[tuple[str, str, str, bool]]:
        # Read-only recovery never clears an uncertain scope or steals an active owner.
        with self.store.authority_transaction() as connection:
            owner = connection.execute(
                "SELECT * FROM tushare_range_owners WHERE scope_id = ?", (scope_id,)
            ).fetchone()
            if (
                owner is not None
                and owner["state"] == "running"
                and owner["expires_at"] > time.time()
            ):
                raise AcquisitionPending(scope_id)
            rows = connection.execute(
                "SELECT * FROM tushare_range_responses WHERE scope_id = ? "
                "ORDER BY start_date, end_date",
                (scope_id,),
            ).fetchall()
        return [
            (r["start_date"], r["end_date"], r["artifact_hash"], bool(r["complete"])) for r in rows
        ]

    @staticmethod
    def segment_query(query: DataQuery, start: str, end: str) -> DataQuery:
        return DataQuery.build(
            capability=query.capability,
            pit_lane=query.pit_lane,
            as_of=query.as_of,
            window_start=query.window_start,
            source_policy_id=query.source_policy_id,
            parameters={**query.parameters, "start_date": start, "end_date": end},
            sources=query.sources,
            minimum_data_sources=query.minimum_data_sources,
        )

    def _verify(
        self, query: DataQuery, source: DataSourceBinding, row: tuple[str, str, str, bool]
    ) -> ProviderDataResponse:
        return _verify_segment(self.store, self.provider, query, source, row)

    async def project(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
        segments: tuple[tuple[str, str, str, bool], ...],
    ) -> DataSnapshot:
        scope_id, start, end = self._scope(query, source)
        responses = tuple(self._verify(query, source, row) for row in segments)
        harness = DataInputHarness(self.store)
        harness.register(self.provider)
        if len(responses) == 1 and not responses[0].status.completed:
            snapshot = await harness.snapshot_from_response(query, responses[0])
            await asyncio.to_thread(self.store.put, snapshot)
            return snapshot
        missing = [(start, end)]
        observations: set[str] = set()
        for (left, right, _, _), response in zip(segments, responses, strict=True):
            if not response.status.completed or _date(left) > end or _date(right) < start:
                raise ValueError("saved projection requires overlapping complete intervals")
            missing = _subtract(missing, _date(left), _date(right))
            for item in response.observations:
                record = cast(dict[str, object], item.normalized_payload["record"])
                if start <= _date(record["trade_date"]) <= end:
                    observations.add(item.observation_id)
        if missing:
            raise AcquisitionUncertain(scope_id)
        manifest = {
            "schema_version": "market-impact.tushare-range-projection.v2",
            "scope_id": scope_id,
            "source": source.to_dict(),
            "parameters": query.parameters,
            "segments": [
                {
                    "start_date": left,
                    "end_date": right,
                    "response_artifact_hash": digest,
                    "raw_response_hash": response.raw_response_hash,
                }
                for (left, right, digest, _), response in zip(segments, responses, strict=True)
            ],
        }
        snapshot = await harness.project_saved_responses(
            query, responses=responses, observation_ids=frozenset(observations), manifest=manifest
        )
        await asyncio.to_thread(self.store.put, snapshot)
        return snapshot

    def latest_receipt(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
        segments: tuple[tuple[str, str, str, bool], ...],
    ) -> datetime:
        self._scope(query, source)
        return max(self._verify(query, source, row).retrieved_at for row in segments)

    def _claim(self, scope_id: str, token: str) -> list[tuple[str, str, str, bool]]:
        with self.store.authority_transaction() as connection:
            owner = connection.execute(
                "SELECT * FROM tushare_range_owners WHERE scope_id = ?", (scope_id,)
            ).fetchone()
            if owner is not None and owner["state"] != "idle":
                if owner["state"] == "running" and owner["expires_at"] > time.time():
                    raise AcquisitionPending(scope_id)
                raise AcquisitionUncertain(scope_id)
            connection.execute(
                """INSERT INTO tushare_range_owners VALUES (?, ?, ?, 'running')
                ON CONFLICT(scope_id) DO UPDATE SET token=excluded.token,
                expires_at=excluded.expires_at, state='running' """,
                (scope_id, token, time.time() + 3600),
            )
            rows = connection.execute(
                """SELECT * FROM tushare_range_responses
                WHERE scope_id = ? ORDER BY start_date, end_date""",
                (scope_id,),
            ).fetchall()
        return [
            (r["start_date"], r["end_date"], r["artifact_hash"], bool(r["complete"])) for r in rows
        ]

    def _persist(
        self, scope_id: str, token: str, start: date, end: date, response: ProviderDataResponse
    ) -> str:
        raw_payload = response.raw_payload
        if raw_payload is not None:
            self.store.put_raw(raw_payload)
        artifact = self.store.artifacts.put_json(
            {
                "status": response.status.value,
                "provider_id": response.provider_id,
                "provider_version": response.provider_version,
                "upstream_source": response.upstream_source,
                "retrieved_at": response.retrieved_at.isoformat(),
                "error_kind": response.error_kind,
                "raw_payload": None
                if raw_payload is None
                else base64.b64encode(raw_payload).decode(),
                "observations": [item.to_dict() for item in response.observations],
                "raw_records": [
                    [key, base64.b64encode(raw).decode()] for key, raw in response.raw_records
                ],
            }
        )
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                """SELECT token, state FROM tushare_range_owners
                WHERE scope_id = ?""",
                (scope_id,),
            ).fetchone()
            if row is None or row["token"] != token or row["state"] != "running":
                raise AcquisitionUncertain(scope_id)
            connection.execute(
                "INSERT INTO tushare_range_responses VALUES (?, ?, ?, ?, ?)",
                (
                    scope_id,
                    _format(start),
                    _format(end),
                    artifact.content_hash,
                    int(response.status.completed),
                ),
            )

        return artifact.content_hash

    def _release(self, scope_id: str, token: str, state: str) -> None:
        with self.store.authority_transaction() as connection:
            connection.execute(
                """UPDATE tushare_range_owners SET state = ?
                WHERE scope_id = ? AND token = ? AND state = 'running' """,
                (state, scope_id, token),
            )


def _date(value: object) -> date:
    if not isinstance(value, str) or len(value) != 8 or not value.isascii() or not value.isdigit():
        raise ValueError("daily range date must use YYYYMMDD")
    return datetime.strptime(value, "%Y%m%d").date()


def _format(value: date) -> str:
    return value.strftime("%Y%m%d")


def _subtract(ranges: list[tuple[date, date]], start: date, end: date) -> list[tuple[date, date]]:
    result: list[tuple[date, date]] = []
    for a, b in ranges:
        if end < a or start > b:
            result.append((a, b))
        else:
            if a < start:
                result.append((a, start - timedelta(days=1)))
            if b > end:
                result.append((end + timedelta(days=1), b))
    return result


def load_saved_range_response(
    store: LocalDataSnapshotStore, artifact_hash: str
) -> ProviderDataResponse:
    value = cast(dict[str, object], store.artifacts.read_json(artifact_hash))
    raw = value["raw_payload"]
    return ProviderDataResponse(
        status=DataFetchStatus(cast(str, value["status"])),
        provider_id=cast(str, value["provider_id"]),
        provider_version=cast(str, value["provider_version"]),
        upstream_source=cast(str, value["upstream_source"]),
        retrieved_at=datetime.fromisoformat(cast(str, value["retrieved_at"])),
        error_kind=cast(str | None, value["error_kind"]),
        raw_payload=None if raw is None else base64.b64decode(cast(str, raw)),
        observations=tuple(
            source_observation_from_dict(item) for item in cast(list[object], value["observations"])
        ),
        raw_records=tuple(
            (key, base64.b64decode(raw)) for key, raw in cast(list[list[str]], value["raw_records"])
        ),
    )


def _verify_segment(
    store: LocalDataSnapshotStore,
    provider: TushareObservationProvider,
    query: DataQuery,
    source: DataSourceBinding,
    row: tuple[str, str, str, bool],
) -> ProviderDataResponse:
    left, right, digest, complete = row
    if _date(left) > _date(right):
        raise ValueError("invalid saved range interval")
    response = load_saved_range_response(store, digest)
    if response.status.completed != complete:
        raise ValueError("saved range completion mismatch")
    if (response.provider_id, response.provider_version, response.upstream_source) != (
        source.provider_id,
        source.provider_version,
        source.upstream_source,
    ):
        raise ValueError("saved range response source mismatch")
    if response.raw_payload is None:
        if complete:
            raise ValueError("saved range missing raw bundle")
        return response
    raw = store.artifacts.get(
        cast(str, response.raw_response_hash), media_type="application/octet-stream"
    ).path.read_bytes()
    if raw != response.raw_payload:
        raise ValueError("saved range raw bundle mismatch")
    if not complete:
        return response
    config = tushare_observation_source_from_dict(
        dict(provider.public_source_config(source.upstream_source))
    )
    segment = TushareDailyRangeCache.segment_query(query, left, right)
    try:
        capture = load_tushare_observation_capture_bundle(
            raw, config=config, parameters=segment.parameters, retrieved_at=response.retrieved_at
        )
    except TushareObservationError as exc:
        raise ValueError("invalid saved range capture") from exc
    rebuilt = provider.response_from_capture(query=segment, source=source, capture=capture)
    if rebuilt != response:
        raise ValueError("saved range physical records differ from raw capture")
    for item in response.observations:
        record_value = item.normalized_payload.get("record")
        if not isinstance(record_value, dict):
            raise ValueError("saved range record missing")
        record = cast(dict[str, object], record_value)
        if record.get("ts_code") != query.parameters["ts_code"]:
            raise ValueError("saved range record instrument mismatch")
        if not _date(left) <= _date(record.get("trade_date")) <= _date(right):
            raise ValueError("saved range record lies outside physical interval")
    return response


def verify_range_projection(store: LocalDataSnapshotStore, snapshot: DataSnapshot) -> set[str]:
    """Reopen constituent proof without scope claims, transport, or database writes."""
    hashes: set[str] = set()
    for source, attempt in zip(snapshot.query.sources, snapshot.attempts, strict=True):
        if not attempt.status.completed or attempt.raw_response_hash is None:
            continue
        payload = store.artifacts.get(
            attempt.raw_response_hash, media_type="application/octet-stream"
        ).path.read_bytes()
        if not payload.startswith(b"{"):
            continue
        value: object = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("invalid saved range projection")
        manifest = cast(dict[str, object], value)
        version = manifest.get("schema_version")
        if version not in {
            "market-impact.tushare-range-projection.v1",
            "market-impact.tushare-range-projection.v2",
        }:
            raise ValueError("unsupported saved range projection")
        legacy = version == "market-impact.tushare-range-projection.v1"
        if (not legacy and manifest.get("source") != source.to_dict()) or manifest.get(
            "parameters"
        ) != snapshot.query.parameters:
            raise ValueError("range projection binding mismatch")
        if source.source_config_hash is None:
            raise ValueError("range projection source configuration missing")
        config_dict = store.artifacts.read_json(source.source_config_hash)
        config = tushare_observation_source_from_dict(config_dict)
        scope = canonical_hash(
            {
                "contract": "tushare-raw-daily-range-v1",
                "source": source.to_dict(),
                "config": config_dict,
                "capability": snapshot.query.capability.value,
                "pit_lane": snapshot.query.pit_lane.value,
                "source_policy_id": snapshot.query.source_policy_id,
                "ts_code": snapshot.query.parameters["ts_code"],
            }
        )
        if manifest.get("scope_id") != scope or config.api_name not in {"daily", "fund_daily"}:
            raise ValueError("range projection scope mismatch")
        provider = TushareObservationProvider("saved-response-replay", (config,))
        start, end = (
            _date(snapshot.query.parameters["start_date"]),
            _date(snapshot.query.parameters["end_date"]),
        )
        missing = [(start, end)]
        observations: dict[str, SourceObservation] = {}
        receipts: list[datetime] = []
        segments = (
            _legacy_projection_segments(store, provider, snapshot.query, source, scope, manifest)
            if legacy
            else cast(list[dict[str, str]], manifest["segments"])
        )
        for segment in segments:
            left, right, digest = (
                segment["start_date"],
                segment["end_date"],
                segment["response_artifact_hash"],
            )
            if _date(left) > end or _date(right) < start:
                raise ValueError("range projection segment does not overlap query")
            response = _verify_segment(
                store, provider, snapshot.query, source, (left, right, digest, True)
            )
            if response.raw_response_hash != segment["raw_response_hash"]:
                raise ValueError("range projection raw response binding mismatch")
            missing = _subtract(missing, _date(left), _date(right))
            receipts.append(response.retrieved_at)
            hashes.update((digest, cast(str, response.raw_response_hash)))
            for item in response.observations:
                record = cast(dict[str, object], item.normalized_payload["record"])
                if start <= _date(record["trade_date"]) <= end:
                    observations[item.observation_id] = item
        if (
            missing
            or not receipts
            or max(receipts) != attempt.retrieved_at
            or max(receipts) > snapshot.query.as_of
        ):
            raise ValueError("range projection coverage receipt mismatch")
        if attempt.received_count != len(observations):
            raise ValueError("range projection record count mismatch")
        if legacy and any(
            item.times.retrieved_at != attempt.retrieved_at for item in observations.values()
        ):
            raise ValueError("legacy range projection violates physical receipt authority")
        for item in snapshot.observations:
            if (
                item.upstream_source == source.upstream_source
                and observations.get(item.observation_id) != item
            ):
                raise ValueError("range projection observation identity mismatch")
    return hashes


def _legacy_projection_segments(
    store: LocalDataSnapshotStore,
    provider: TushareObservationProvider,
    query: DataQuery,
    source: DataSourceBinding,
    scope: str,
    manifest: Mapping[str, object],
) -> list[dict[str, str]]:
    """Resolve v1's raw hashes through independently verified physical evidence.

    The mutable index is only a discovery hint. No index flag, interval, or receipt
    supplies authority until the immutable capture and response reconcile with it.
    """
    declared = manifest.get("raw_response_hashes")
    if not isinstance(declared, list) or not declared:
        raise ValueError("legacy range projection lacks constituent hashes")
    raw_hashes = cast(list[object], declared)
    if any(not isinstance(value, str) for value in raw_hashes):
        raise ValueError("legacy range projection has invalid constituent hashes")
    try:
        with store.authority_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM tushare_range_responses WHERE scope_id = ? "
                "ORDER BY start_date, end_date",
                (scope,),
            ).fetchall()
    except sqlite3.OperationalError as exc:
        raise ValueError("legacy range projection physical response index missing") from exc
    candidates: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        try:
            response = load_saved_range_response(store, row["artifact_hash"])
            digest = response.raw_response_hash
            if digest not in raw_hashes:
                continue
            # Reopens both CAS objects and proves source, original query, interval,
            # completeness, normalized identity, raw records and original receipt.
            _verify_segment(
                store,
                provider,
                query,
                source,
                (row["start_date"], row["end_date"], row["artifact_hash"], True),
            )
        except (ValueError, LookupError, OSError, TypeError):
            continue
        assert digest is not None
        candidates.setdefault(digest, []).append(
            {
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "response_artifact_hash": row["artifact_hash"],
                "raw_response_hash": digest,
            }
        )
    result: list[dict[str, str]] = []
    for raw_hash in cast(list[str], raw_hashes):
        matches = candidates.get(raw_hash, [])
        if len(matches) != 1:
            raise ValueError("legacy range projection physical proof missing or ambiguous")
        result.append(matches[0])
    return result
