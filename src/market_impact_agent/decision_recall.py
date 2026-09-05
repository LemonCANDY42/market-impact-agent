"""Bounded, rebuildable navigation over authoritative decision artifacts.

The SQLite rows in this module are a disposable projection.  A search hit is
never evidence: callers must reopen the content-addressed source artifact
through :meth:`read_prior_decisions` before using it in a new decision.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import ArtifactStore, RunJournal

MAX_SEARCH_RESULTS = 8
MAX_REOPENED_TOKENS = 12_000
_ALLOWED_SOURCE_SCHEMAS = frozenset(
    {
        "market-impact.research-thesis.v1",
    }
)
_FORBIDDEN_KEYS = frozenset(
    {
        "account_id",
        "account_reference_hash",
        "api_key",
        "credential",
        "paid_news_body",
        "raw_paid_news",
        "secret",
        "token",
        "hidden_outcome",
        "realized_return",
    }
)


@dataclass(frozen=True, slots=True)
class RecallProjectionEntry:
    root_event_id: str
    thesis_epoch: str
    source_kind: str
    source_run_id: str
    source_artifact_hash: str
    source_as_of: datetime
    instrument_ids: tuple[str, ...]
    industry_tags: tuple[str, ...]
    summary: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.root_event_id, "root_event_id"),
            (self.thesis_epoch, "thesis_epoch"),
            (self.source_kind, "source_kind"),
            (self.source_run_id, "source_run_id"),
            (self.summary, "summary"),
        ):
            _text(value, name)
        _sha256(self.source_artifact_hash)
        require_aware(self.source_as_of, "recall source_as_of")
        _unique(self.instrument_ids, "instrument_ids")
        _unique(self.industry_tags, "industry_tags")
        if len(self.summary.encode("utf-8")) > 4096:
            raise ValueError("recall summary is too large")

    @property
    def recall_id(self) -> str:
        return "decision-recall-v1-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.decision-recall-entry.v1",
            "root_event_id": self.root_event_id,
            "thesis_epoch": self.thesis_epoch,
            "source_kind": self.source_kind,
            "source_run_id": self.source_run_id,
            "source_artifact_hash": self.source_artifact_hash,
            "source_as_of": _timestamp(self.source_as_of),
            "instrument_ids": list(self.instrument_ids),
            "industry_tags": list(self.industry_tags),
            "summary": self.summary,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "recall_id": self.recall_id, "evidence": False}


@dataclass(frozen=True, slots=True)
class ReopenedDecision:
    recall_id: str
    source_artifact_hash: str
    source: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "recall_id": self.recall_id,
            "source_artifact_hash": self.source_artifact_hash,
            "source": self.source,
            "evidence": True,
        }


class DecisionRecallProjection:
    """Disposable search projection over a caller-owned ArtifactStore."""

    def __init__(
        self,
        path: Path,
        *,
        artifact_store: ArtifactStore,
        journal: RunJournal,
    ) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = path.resolve()
        self.artifact_store = artifact_store
        if not journal.promotion_eligible:
            raise ValueError("Decision Recall requires an authoritative signed Run Journal")
        self.journal = journal
        with self._connect() as connection:
            schema = """
                CREATE TABLE IF NOT EXISTS decision_recall_entries (
                    recall_id TEXT PRIMARY KEY,
                    root_event_id TEXT NOT NULL,
                    thesis_epoch TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_run_id TEXT NOT NULL,
                    source_artifact_hash TEXT NOT NULL,
                    source_as_of TEXT NOT NULL,
                    instrument_ids_json TEXT NOT NULL,
                    industry_tags_json TEXT NOT NULL,
                    summary TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS decision_recall_root_as_of
                    ON decision_recall_entries(root_event_id, source_as_of, recall_id);
                CREATE INDEX IF NOT EXISTS decision_recall_epoch_as_of
                    ON decision_recall_entries(thesis_epoch, source_as_of, recall_id);
                """
            connection.executescript(schema)
            columns = {
                cast(str, row[1])
                for row in connection.execute("PRAGMA table_info(decision_recall_entries)")
            }
            if "source_run_id" not in columns:
                # This database is only a rebuildable navigation projection.
                connection.execute("DROP TABLE decision_recall_entries")
                connection.executescript(schema)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def rebuild(self, entries: Iterable[RecallProjectionEntry]) -> None:
        materialized = tuple(entries)
        if len({item.recall_id for item in materialized}) != len(materialized):
            raise ValueError("recall rebuild contains duplicate entries")
        for item in materialized:
            self._verify_source(item)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM decision_recall_entries")
            for item in materialized:
                self._insert(connection, item)

    def add(self, entry: RecallProjectionEntry) -> None:
        self._verify_source(entry)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM decision_recall_entries WHERE recall_id = ?",
                (entry.recall_id,),
            ).fetchone()
            if existing is not None:
                if _entry(existing) != entry:
                    raise ValueError("recall identity already maps to different content")
                return
            self._insert(connection, entry)

    def read_current_thesis(
        self, *, root_event_id: str, as_of: datetime
    ) -> RecallProjectionEntry | None:
        _text(root_event_id, "root_event_id")
        require_aware(as_of, "recall as_of")
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM decision_recall_entries
                WHERE root_event_id = ? AND source_kind = 'research_thesis'
                  AND source_as_of <= ?
                ORDER BY source_as_of DESC, recall_id DESC LIMIT 1
                """,
                (root_event_id, _timestamp(as_of)),
            ).fetchone()
        if row is None:
            return None
        entry = _entry(row)
        self._verify_source(entry)
        if entry.source_as_of > as_of:
            raise PermissionError("recall source is after the decision cutoff")
        return entry

    def search_prior_decisions(
        self,
        *,
        as_of: datetime,
        root_event_id: str | None = None,
        instrument_id: str | None = None,
        industry_tag: str | None = None,
        thesis_epoch: str | None = None,
        query: str | None = None,
        limit: int = MAX_SEARCH_RESULTS,
    ) -> tuple[RecallProjectionEntry, ...]:
        require_aware(as_of, "recall as_of")
        if isinstance(limit, bool) or not 1 <= limit <= MAX_SEARCH_RESULTS:
            raise ValueError(f"recall search limit must be in [1, {MAX_SEARCH_RESULTS}]")
        clauses = ["source_as_of <= ?"]
        parameters: list[object] = [_timestamp(as_of)]
        for column, value in (
            ("root_event_id", root_event_id),
            ("thesis_epoch", thesis_epoch),
        ):
            if value is not None:
                _text(value, column)
                clauses.append(f"{column} = ?")
                parameters.append(value)
        for column, value in (
            ("instrument_ids_json", instrument_id),
            ("industry_tags_json", industry_tag),
        ):
            if value is not None:
                _text(value, column)
                clauses.append(f"{column} LIKE ? ESCAPE '\\'")
                parameters.append(f'%"{_escape_like(value)}"%')
        if query is not None:
            _text(query, "query")
            clauses.append("summary LIKE ? ESCAPE '\\'")
            parameters.append(f"%{_escape_like(query)}%")
        parameters.append(limit)
        statement = (
            "SELECT * FROM decision_recall_entries WHERE "
            + " AND ".join(clauses)
            + " ORDER BY source_as_of DESC, recall_id DESC LIMIT ?"
        )
        with self._connect() as connection:
            rows = connection.execute(statement, tuple(parameters)).fetchall()
        entries = tuple(_entry(row) for row in rows)
        for entry in entries:
            self._verify_source(entry)
            if entry.source_as_of > as_of:
                raise PermissionError("recall source is after the decision cutoff")
        return entries

    def read_prior_decisions(
        self,
        recall_ids: tuple[str, ...],
        *,
        as_of: datetime,
        max_tokens: int = MAX_REOPENED_TOKENS,
    ) -> tuple[ReopenedDecision, ...]:
        require_aware(as_of, "recall as_of")
        if not recall_ids or len(recall_ids) > MAX_SEARCH_RESULTS:
            raise ValueError("recall read requires one to eight IDs")
        if len(set(recall_ids)) != len(recall_ids):
            raise ValueError("recall read IDs must be unique")
        if isinstance(max_tokens, bool) or not 1 <= max_tokens <= MAX_REOPENED_TOKENS:
            raise ValueError("recall token limit is outside the registered bound")
        placeholders = ",".join("?" for _ in recall_ids)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM decision_recall_entries WHERE recall_id IN ({placeholders})",
                recall_ids,
            ).fetchall()
        indexed = {cast(str, row["recall_id"]): _entry(row) for row in rows}
        if set(indexed) != set(recall_ids):
            raise KeyError("one or more recall IDs are unknown")
        reopened: list[ReopenedDecision] = []
        for recall_id in recall_ids:
            entry = indexed[recall_id]
            # SQLite is a disposable navigation projection, never an authority.
            # Re-bind every row to its content-addressed source before applying PIT.
            self._verify_source(entry)
            if entry.source_as_of > as_of:
                raise PermissionError("recall source is after the decision cutoff")
            source = _source_object(self.artifact_store.read_json(entry.source_artifact_hash))
            self._assert_safe_source(source)
            reopened.append(ReopenedDecision(recall_id, entry.source_artifact_hash, source))
        payload = [item.to_dict() for item in reopened]
        # One UTF-8 byte per token is a conservative, provider-independent upper bound.
        if len(canonical_json_bytes(payload)) > max_tokens:
            raise ValueError("reopened recall exceeds the context token bound")
        return tuple(reopened)

    def _verify_source(self, entry: RecallProjectionEntry) -> None:
        source = _source_object(self.artifact_store.read_json(entry.source_artifact_hash))
        self._assert_safe_source(source)
        from market_impact_agent.research_thesis_runtime import (
            reopen_completed_research_thesis,
        )

        try:
            thesis, proof = reopen_completed_research_thesis(
                journal=self.journal,
                artifact_store=self.artifact_store,
                run_id=entry.source_run_id,
            )
        except (KeyError, PermissionError, TypeError, ValueError) as exc:
            raise ValueError("recall source has no signed completed Run") from exc
        terminal = _source_object(self.artifact_store.read_json(cast(str, proof["terminal_hash"])))
        if (
            proof.get("run_id") != entry.source_run_id
            or terminal.get("thesis_artifact_hash") != entry.source_artifact_hash
            or thesis.to_dict() != source
        ):
            raise ValueError("recall source is not the signed terminal thesis")
        expected_kind = {
            "market-impact.research-thesis.v1": "research_thesis",
        }[cast(str, source["schema_version"])]
        if entry.source_kind != expected_kind:
            raise ValueError("recall source kind differs from its authoritative artifact")
        if entry.summary != _safe_navigation_summary(source):
            raise ValueError("recall summary is not derived from its authoritative artifact")
        source_as_of = source.get("as_of")
        if source_as_of != _timestamp(entry.source_as_of):
            raise ValueError("recall source cutoff differs from projection")
        if expected_kind == "research_thesis" and (
            source.get("root_event_id") != entry.root_event_id
            or source.get("thesis_epoch") != entry.thesis_epoch
        ):
            raise ValueError("recall thesis identity differs from its authoritative artifact")

    @staticmethod
    def _assert_safe_source(source: Mapping[str, object]) -> None:
        if source.get("schema_version") not in _ALLOWED_SOURCE_SCHEMAS:
            raise ValueError("recall source is not an admitted decision artifact")

        def inspect(value: object) -> None:
            if isinstance(value, dict):
                mapping = cast(dict[object, object], value)
                for key, item in mapping.items():
                    if not isinstance(key, str):
                        raise TypeError("recall source keys must be strings")
                    if key.lower() in _FORBIDDEN_KEYS:
                        raise ValueError("recall source contains excluded private or outcome data")
                    inspect(item)
            elif isinstance(value, list):
                for item in cast(list[object], value):
                    inspect(item)

        inspect(source)

    @staticmethod
    def _insert(connection: sqlite3.Connection, entry: RecallProjectionEntry) -> None:
        connection.execute(
            """
            INSERT INTO decision_recall_entries(
                recall_id, root_event_id, thesis_epoch, source_kind,
                source_run_id, source_artifact_hash, source_as_of, instrument_ids_json,
                industry_tags_json, summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.recall_id,
                entry.root_event_id,
                entry.thesis_epoch,
                entry.source_kind,
                entry.source_run_id,
                entry.source_artifact_hash,
                _timestamp(entry.source_as_of),
                json.dumps(entry.instrument_ids, separators=(",", ":")),
                json.dumps(entry.industry_tags, separators=(",", ":")),
                entry.summary,
            ),
        )


def decision_recall_tools(
    projection: DecisionRecallProjection,
    *,
    as_of: datetime,
    current_root_event_id: str,
) -> tuple[ToolDescriptor, ToolDescriptor, ToolDescriptor]:
    """Build the three read-only capabilities with Harness-owned cutoff and identity."""

    require_aware(as_of, "recall tool as_of")
    _text(current_root_event_id, "current_root_event_id")

    async def current(_: dict[str, object]) -> object:
        entry = projection.read_current_thesis(
            root_event_id=current_root_event_id,
            as_of=as_of,
        )
        return {"current_thesis": None if entry is None else entry.to_dict()}

    async def search(arguments: dict[str, object]) -> object:
        allowed = {
            "root_event_id",
            "instrument_id",
            "industry_tag",
            "thesis_epoch",
            "query",
            "limit",
        }
        if set(arguments) - allowed:
            raise ValueError("recall search contains unsupported filters")
        hits = projection.search_prior_decisions(
            as_of=as_of,
            root_event_id=_optional_text(arguments, "root_event_id"),
            instrument_id=_optional_text(arguments, "instrument_id"),
            industry_tag=_optional_text(arguments, "industry_tag"),
            thesis_epoch=_optional_text(arguments, "thesis_epoch"),
            query=_optional_text(arguments, "query"),
            limit=_optional_integer(arguments, "limit", MAX_SEARCH_RESULTS),
        )
        return {"hits": [item.to_dict() for item in hits], "evidence": False}

    async def read(arguments: dict[str, object]) -> object:
        if set(arguments) != {"ids"} or not isinstance(arguments.get("ids"), list):
            raise ValueError("recall read requires only an ids array")
        raw_ids = cast(list[object], arguments["ids"])
        if any(not isinstance(item, str) for item in raw_ids):
            raise ValueError("recall read IDs must be strings")
        items = projection.read_prior_decisions(tuple(cast(list[str], raw_ids)), as_of=as_of)
        return {"decisions": [item.to_dict() for item in items], "evidence": True}

    return (
        ToolDescriptor(
            name="read_current_thesis",
            description=(
                "Read the latest pre-cutoff thesis for this root event. Takes no arguments."
            ),
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            handler=current,
            version="v1",
            required_capabilities=frozenset({"decision_recall.read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=5.0,
            max_result_bytes=64_000,
        ),
        ToolDescriptor(
            name="search_prior_decisions",
            description=(
                "Search bounded pre-cutoff decision summaries for navigation only; hits are not "
                "evidence until reopened with read_prior_decisions."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "root_event_id": {"type": "string", "minLength": 1},
                    "instrument_id": {"type": "string", "minLength": 1},
                    "industry_tag": {"type": "string", "minLength": 1},
                    "thesis_epoch": {"type": "string", "minLength": 1},
                    "query": {"type": "string", "minLength": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 8},
                },
            },
            handler=search,
            version="v1",
            required_capabilities=frozenset({"decision_recall.read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=5.0,
            max_result_bytes=64_000,
        ),
        ToolDescriptor(
            name="read_prior_decisions",
            description=(
                "Reopen one to eight selected decision artifacts by content hash after search."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["ids"],
                "properties": {
                    "ids": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 8,
                        "uniqueItems": True,
                        "items": {
                            "type": "string",
                            "pattern": "^decision-recall-v1-[0-9a-f]{64}$",
                        },
                    }
                },
            },
            handler=read,
            version="v1",
            required_capabilities=frozenset({"decision_recall.read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=5.0,
            max_result_bytes=64_000,
        ),
    )


def _entry(row: sqlite3.Row) -> RecallProjectionEntry:
    return RecallProjectionEntry(
        root_event_id=cast(str, row["root_event_id"]),
        thesis_epoch=cast(str, row["thesis_epoch"]),
        source_kind=cast(str, row["source_kind"]),
        source_run_id=cast(str, row["source_run_id"]),
        source_artifact_hash=cast(str, row["source_artifact_hash"]),
        source_as_of=datetime.fromisoformat(cast(str, row["source_as_of"]).replace("Z", "+00:00")),
        instrument_ids=tuple(json.loads(cast(str, row["instrument_ids_json"]))),
        industry_tags=tuple(json.loads(cast(str, row["industry_tags_json"]))),
        summary=cast(str, row["summary"]),
    )


def _source_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("recall source must be a JSON object")
    mapping = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in mapping):
        raise TypeError("recall source must be a JSON object")
    return cast(dict[str, object], mapping)


def _safe_navigation_summary(source: Mapping[str, object]) -> str:
    """Derive a narrow search label; never accept caller-authored narrative text."""

    schema = source.get("schema_version")
    horizon = source.get("primary_horizon_sessions")
    if type(horizon) is not int or horizon not in {1, 3, 5, 10, 20, 60}:
        raise ValueError("recall source has an invalid horizon")
    if schema == "market-impact.research-thesis.v1":
        direction = source.get("base_case_direction")
        if direction not in {"up", "down", "rangebound"}:
            raise ValueError("recall thesis has an invalid direction")
        return f"research_thesis direction={direction} horizon_sessions={horizon}"
    raise ValueError("recall source is not an admitted decision artifact")


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _optional_text(value: Mapping[str, object], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"recall {key} must be text")
    _text(item, key)
    return item


def _optional_integer(value: Mapping[str, object], key: str, default: int) -> int:
    item = value.get(key, default)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"recall {key} must be an integer")
    return item


def _unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be unique and sorted")
    for value in values:
        _text(value, name)


def _text(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _sha256(value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("source_artifact_hash must be a SHA-256 digest")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
