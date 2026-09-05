"""Opt-in complete interval reuse for raw Tushare daily and fund_daily rows.

Coverage means the configured pagination completed, never tradability or PIT
at an earlier date. The enclosing DataInputHarness applies its original cutoff.
"""

from __future__ import annotations

import asyncio
import base64
import time
import uuid
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.data_acquisition import AcquisitionPending, AcquisitionUncertain
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataQuery,
    DataSourceBinding,
    LocalDataSnapshotStore,
    ProviderDataResponse,
    SourceObservation,
    source_observation_from_dict,
)
from market_impact_agent.observations import ObservationProviderManifest
from market_impact_agent.tushare_observation import TushareObservationProvider


class TushareDailyRangeCache:
    """Provider decorator; registration remains bound to the original source config."""

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

    async def fetch(self, *, query: DataQuery, source: DataSourceBinding) -> ProviderDataResponse:
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
        token = uuid.uuid4().hex
        rows = await asyncio.to_thread(self._claim, scope_id, token)
        try:
            responses: list[ProviderDataResponse] = []
            missing = [(start, end)]
            for left, right, artifact_hash, complete in rows:
                a, b = _date(left), _date(right)
                if a > end or b < start:
                    continue
                # Failures block repetition of that exact interval; successful ranges
                # are the only entries allowed to subtract coverage from a new request.
                if not complete:
                    return self._load(artifact_hash)
                responses.append(self._load(artifact_hash))
                missing = _subtract(missing, a, b)
            for a, b in missing:
                segment = DataQuery.build(
                    capability=query.capability,
                    pit_lane=query.pit_lane,
                    as_of=query.as_of,
                    window_start=query.window_start,
                    source_policy_id=query.source_policy_id,
                    parameters={**params, "start_date": _format(a), "end_date": _format(b)},
                    sources=query.sources,
                    minimum_data_sources=query.minimum_data_sources,
                )
                # Existing source collector owns pagination, normalization and completeness.
                response = await self.provider.fetch(query=segment, source=source)
                await asyncio.to_thread(self._persist, scope_id, token, a, b, response)
                if not response.status.completed:
                    return response
                responses.append(response)
            observations: dict[str, SourceObservation] = {}
            records: dict[str, bytes] = {}
            for response in responses:
                raw = dict(response.raw_records)
                for item in response.observations:
                    record = item.normalized_payload.get("record")
                    if not isinstance(record, dict):
                        raise ValueError("daily range row requires record")
                    record = cast(dict[str, object], record)
                    row_date = _date(record.get("trade_date"))
                    if record.get("ts_code") != params["ts_code"]:
                        raise ValueError("daily range row has mismatched instrument")
                    if start <= row_date <= end:
                        observations[item.observation_id] = item
                        records[item.observation_id] = raw[item.observation_id]
            latest = max(response.retrieved_at for response in responses)
            raw_payload = canonical_json_bytes(
                {
                    "schema_version": "market-impact.tushare-range-projection.v1",
                    "scope_id": scope_id,
                    "parameters": params,
                    "raw_response_hashes": [response.raw_response_hash for response in responses],
                }
            )
            return ProviderDataResponse(
                status=DataFetchStatus.DATA if observations else DataFetchStatus.NO_DATA,
                provider_id=source.provider_id,
                provider_version=source.provider_version,
                upstream_source=source.upstream_source,
                retrieved_at=latest,
                raw_payload=raw_payload,
                observations=tuple(observations.values()),
                raw_records=tuple(records.items()),
            )
        except BaseException:
            await asyncio.to_thread(self._release, scope_id, token, "uncertain")
            raise
        finally:
            await asyncio.to_thread(self._release, scope_id, token, "idle")

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
    ) -> None:
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

    def _load(self, artifact_hash: str) -> ProviderDataResponse:
        value = cast(dict[str, object], self.store.artifacts.read_json(artifact_hash))
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
                source_observation_from_dict(item)
                for item in cast(list[object], value["observations"])
            ),
            raw_records=tuple(
                (key, base64.b64decode(raw))
                for key, raw in cast(list[list[str]], value["raw_records"])
            ),
        )

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
