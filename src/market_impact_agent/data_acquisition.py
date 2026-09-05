"""Durable exact-query acquisition in the existing Harness authority store.

A lost owner is uncertain, never permission to repeat external I/O. Terminal
snapshots (including typed failures and absence) remain replayable by query.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from market_impact_agent.data_inputs import DataQuery, DataSnapshot, LocalDataSnapshotStore


class AcquisitionPending(LookupError):
    """Another process owns this exact query; the caller may wait within its budget."""


class AcquisitionUncertain(RuntimeError):
    """The owner disappeared or failed after dispatch; no blind retry is allowed."""


class DurableDataAcquisition:
    def __init__(self, store: LocalDataSnapshotStore) -> None:
        self.store = store
        with store.authority_transaction() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS data_acquisitions (
                query_id TEXT PRIMARY KEY, owner_token TEXT NOT NULL,
                expires_at REAL NOT NULL, state TEXT NOT NULL,
                snapshot_id TEXT, error_kind TEXT
                )"""
            )

    def claim(self, query: DataQuery, *, lease_seconds: float) -> tuple[str, str | None]:
        token = uuid.uuid4().hex
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM data_acquisitions WHERE query_id = ?", (query.query_id,)
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO data_acquisitions VALUES (?, ?, ?, 'running', NULL, NULL)",
                    (query.query_id, token, time.time() + lease_seconds),
                )
                return token, None
            if row["state"] == "complete":
                return token, cast(str, row["snapshot_id"])
            if row["state"] == "running" and row["expires_at"] > time.time():
                raise AcquisitionPending(query.query_id)
            connection.execute(
                "UPDATE data_acquisitions SET state = 'uncertain' WHERE query_id = ?",
                (query.query_id,),
            )
        raise AcquisitionUncertain(query.query_id)

    def finish(self, query: DataQuery, token: str, snapshot: DataSnapshot) -> None:
        if snapshot.query != query:
            raise ValueError("acquisition result must preserve exact query")
        artifact = self.store.artifacts.put_json(snapshot.to_dict())
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM data_acquisitions WHERE query_id = ?", (query.query_id,)
            ).fetchone()
            if row is None or row["owner_token"] != token or row["state"] != "running":
                raise AcquisitionUncertain(query.query_id)
            connection.execute(
                """INSERT OR IGNORE INTO data_snapshots
                (snapshot_id, query_id, artifact_hash, coverage_complete, completed_at)
                VALUES (?, ?, ?, ?, ?)""",
                (
                    snapshot.snapshot_id,
                    query.query_id,
                    artifact.content_hash,
                    int(snapshot.coverage_complete),
                    snapshot.completed_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.execute(
                "UPDATE data_acquisitions SET state = 'complete', snapshot_id = ? "
                "WHERE query_id = ?",
                (snapshot.snapshot_id, query.query_id),
            )

    def mark_uncertain(self, query: DataQuery, token: str, error_kind: str) -> None:
        with self.store.authority_transaction() as connection:
            connection.execute(
                """UPDATE data_acquisitions SET state = 'uncertain', error_kind = ?
                WHERE query_id = ? AND owner_token = ? AND state = 'running'""",
                (error_kind, query.query_id, token),
            )

    async def execute(
        self,
        query: DataQuery,
        *,
        fetch: Callable[[DataQuery], Awaitable[DataSnapshot]],
        lease_seconds: float,
    ) -> DataSnapshot:
        token, snapshot_id = await asyncio.to_thread(self.claim, query, lease_seconds=lease_seconds)
        if snapshot_id is not None:
            return await asyncio.to_thread(self.store.get, snapshot_id)
        try:
            snapshot = await fetch(query)
            await asyncio.to_thread(self.finish, query, token, snapshot)
            return snapshot
        except BaseException as exc:
            await asyncio.to_thread(self.mark_uncertain, query, token, type(exc).__name__)
            raise
