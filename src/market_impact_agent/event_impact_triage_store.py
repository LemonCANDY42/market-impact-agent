from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.event_impact_triage import (
    CompletedTriageRunAuthority,
    CompletedTriageWorkRunAuthority,
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    LegacyTriageWorkDecisionEvidence,
    TriageClusterProposal,
    TriageRunEvidence,
    TriageWorkDecisionEvidence,
    admit_event_impact_triage,
    admit_event_impact_triage_work,
    event_impact_triage_candidate_set_from_dict,
    event_impact_triage_decision_from_dict,
    event_impact_triage_proposal_from_dict,
)
from market_impact_agent.runtime_store import ArtifactStore

if TYPE_CHECKING:
    from market_impact_agent.event_impact_triage_evaluation import EventImpactTriageLabelSet
    from market_impact_agent.event_impact_triage_work import EventImpactTriageWorkManifest
    from market_impact_agent.event_impact_triage_work_evaluation import (
        EventImpactTriageWorkComparisonRegistration,
        EventImpactTriageWorkComparisonReport,
        TriageWorkArmOutcome,
        TriageWorkComparisonReportAuthority,
        TriageWorkComparisonRunAuthority,
    )

EVENT_IMPACT_TRIAGE_TERMINAL_BATCH_SCHEMA = "market-impact.event-impact-triage-terminal-batch.v1"


@dataclass(frozen=True, slots=True)
class EventImpactTriageTerminalBatch:
    terminal_id: str
    candidate_set_id: str
    registration_id: str
    checkpoint_key: str
    route_plan_id: str
    route_admission_id: str
    comparison_id: str
    candidate_set_hash: str
    comparison_registration_hash: str
    comparison_report_id: str
    comparison_report_hash: str
    candidate_version_ids: tuple[str, ...]
    blockers: tuple[str, ...]
    terminalized_at: datetime
    schema_version: str = EVENT_IMPACT_TRIAGE_TERMINAL_BATCH_SCHEMA

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "route_plan_id": self.route_plan_id,
            "route_admission_id": self.route_admission_id,
            "comparison_id": self.comparison_id,
            "candidate_set_hash": self.candidate_set_hash,
            "comparison_registration_hash": self.comparison_registration_hash,
            "comparison_report_id": self.comparison_report_id,
            "comparison_report_hash": self.comparison_report_hash,
            "candidate_version_ids": list(self.candidate_version_ids),
            "blockers": list(self.blockers),
            "terminalized_at": _timestamp(self.terminalized_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {"terminal_id": self.terminal_id, **self.core_dict()}


class EventImpactTriageDecisionStore:
    """Append-only authority for completed or terminally failed Triage versions."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.path = self.root / "event-impact-triage.sqlite3"
        self.artifacts = ArtifactStore(self.root / "artifacts")
        self._initialize()
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS event_impact_triage_decisions (
                    decision_id TEXT PRIMARY KEY,
                    candidate_set_id TEXT NOT NULL UNIQUE,
                    registration_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    route_plan_id TEXT NOT NULL,
                    route_admission_id TEXT NOT NULL,
                    decided_at TEXT NOT NULL,
                    candidate_artifact_hash TEXT NOT NULL,
                    proposal_artifact_hash TEXT NOT NULL,
                    decision_artifact_hash TEXT NOT NULL UNIQUE
                );
                CREATE INDEX IF NOT EXISTS event_impact_triage_epoch
                    ON event_impact_triage_decisions(
                        registration_id, checkpoint_key, route_plan_id,
                        route_admission_id, decided_at, decision_id
                    );
                CREATE TABLE IF NOT EXISTS event_impact_triage_classified_versions (
                    route_admission_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    decision_id TEXT NOT NULL
                        REFERENCES event_impact_triage_decisions(decision_id),
                    PRIMARY KEY(route_admission_id, checkpoint_key, version_id)
                );
                CREATE TABLE IF NOT EXISTS event_impact_triage_terminal_batches (
                    terminal_id TEXT PRIMARY KEY,
                    candidate_set_id TEXT NOT NULL UNIQUE,
                    registration_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    route_plan_id TEXT NOT NULL,
                    route_admission_id TEXT NOT NULL,
                    terminalized_at TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS event_impact_triage_terminal_versions (
                    route_admission_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    terminal_id TEXT NOT NULL
                        REFERENCES event_impact_triage_terminal_batches(terminal_id),
                    PRIMARY KEY(route_admission_id, checkpoint_key, version_id)
                );
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_decisions_no_update
                    BEFORE UPDATE ON event_impact_triage_decisions
                    BEGIN SELECT RAISE(ABORT, 'triage decisions are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_decisions_no_delete
                    BEFORE DELETE ON event_impact_triage_decisions
                    BEGIN SELECT RAISE(ABORT, 'triage decisions are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_versions_no_update
                    BEFORE UPDATE ON event_impact_triage_classified_versions
                    BEGIN SELECT RAISE(
                        ABORT, 'triage version classifications are append-only'
                    ); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_versions_no_delete
                    BEFORE DELETE ON event_impact_triage_classified_versions
                    BEGIN SELECT RAISE(
                        ABORT, 'triage version classifications are append-only'
                    ); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_terminal_batches_no_update
                    BEFORE UPDATE ON event_impact_triage_terminal_batches
                    BEGIN SELECT RAISE(ABORT, 'triage terminal batches are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_terminal_batches_no_delete
                    BEFORE DELETE ON event_impact_triage_terminal_batches
                    BEGIN SELECT RAISE(ABORT, 'triage terminal batches are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_terminal_versions_no_update
                    BEFORE UPDATE ON event_impact_triage_terminal_versions
                    BEGIN SELECT RAISE(ABORT, 'triage terminal versions are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS event_impact_triage_terminal_versions_no_delete
                    BEFORE DELETE ON event_impact_triage_terminal_versions
                    BEGIN SELECT RAISE(ABORT, 'triage terminal versions are append-only'); END;
                """
            )

    def admit(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageRunEvidence,
        run_authority: CompletedTriageRunAuthority,
        decided_at: datetime,
    ) -> EventImpactTriageDecision:
        """Reopen the Agent run, then atomically persist one immutable classification."""

        _strict_utc(decided_at, "triage store decided_at")
        existing = self._row_for_candidate_set(candidate_set.candidate_set_id)
        if existing is not None:
            stored_candidate, stored_proposal, stored_decision = self._reopen(existing)
            if stored_candidate != candidate_set or stored_proposal != proposal:
                raise ValueError("stored triage Candidate Set or Proposal conflicts with caller")
            if stored_decision.run_evidence != run_evidence:
                raise ValueError("stored triage Run Evidence conflicts with caller")
            run_authority.assert_authoritative_completed_triage_run(
                candidate_set=candidate_set,
                proposal=proposal,
                run_evidence=run_evidence,
            )
            return stored_decision

        decision = admit_event_impact_triage(
            candidate_set=candidate_set,
            proposal=proposal,
            run_evidence=run_evidence,
            run_authority=run_authority,
            decided_at=decided_at,
        )
        return self._persist_decision(candidate_set, proposal, decision)

    def admit_work(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageWorkDecisionEvidence,
        run_authority: CompletedTriageWorkRunAuthority,
        decided_at: datetime,
    ) -> EventImpactTriageDecision:
        """Reopen a Work graph, then persist one immutable classification."""

        _strict_utc(decided_at, "triage store decided_at")
        if decided_at != run_evidence.finished_at:
            raise ValueError("triage Work Decision decided_at must equal authoritative finished_at")
        existing = self._row_for_candidate_set(candidate_set.candidate_set_id)
        if existing is not None:
            stored_candidate, stored_proposal, stored_decision = self._reopen(existing)
            if stored_candidate != candidate_set or stored_proposal != proposal:
                raise ValueError("stored triage Candidate Set or Proposal conflicts with caller")
            run_authority.assert_authoritative_completed_triage_work_run(
                candidate_set=candidate_set,
                proposal=proposal,
                run_evidence=run_evidence,
            )
            self._assert_work_retry_compatible(stored_decision, run_evidence)
            return stored_decision

        decision = admit_event_impact_triage_work(
            candidate_set=candidate_set,
            proposal=proposal,
            run_evidence=run_evidence,
            run_authority=run_authority,
            decided_at=decided_at,
        )
        return self._persist_decision(candidate_set, proposal, decision)

    def terminalize_failed_work_comparison(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        comparison: EventImpactTriageWorkComparisonRegistration,
        report: EventImpactTriageWorkComparisonReport,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline: TriageWorkArmOutcome,
        treatment: TriageWorkArmOutcome,
        baseline_authority: TriageWorkComparisonRunAuthority,
        treatment_authority: TriageWorkComparisonRunAuthority,
        comparison_authority: TriageWorkComparisonReportAuthority,
        terminalized_at: datetime,
    ) -> EventImpactTriageTerminalBatch:
        """Record a failed blind batch without manufacturing a semantic Decision."""

        from market_impact_agent.event_impact_triage_work_evaluation import (
            EventImpactTriageWorkComparisonStore,
        )

        _strict_utc(terminalized_at, "triage terminalized_at")
        if type(comparison_authority) is not EventImpactTriageWorkComparisonStore:
            raise TypeError("triage terminalization requires durable Comparison Store authority")
        comparison_authority.assert_authoritative_report(
            report=report,
            registration=comparison,
            candidate_set=candidate_set,
            label_set=label_set,
            work_manifest=work_manifest,
            baseline=baseline,
            treatment=treatment,
            baseline_authority=baseline_authority,
            treatment_authority=treatment_authority,
        )
        if terminalized_at < report.evaluated_at:
            raise ValueError("triage terminalization cannot predate its comparison report")
        if (
            comparison.candidate_set_id != candidate_set.candidate_set_id
            or report.comparison_id != comparison.comparison_id
            or report.batch_gate_passed
        ):
            raise ValueError("triage terminalization requires one failed bound comparison")
        if not report.blockers:
            raise ValueError("failed triage comparison must retain its blockers")
        candidate_artifact = self.artifacts.put_json(candidate_set.to_dict())
        comparison_artifact = self.artifacts.put_json(comparison.to_dict())
        report_artifact = self.artifacts.put_json(report.to_dict())
        core = {
            "schema_version": EVENT_IMPACT_TRIAGE_TERMINAL_BATCH_SCHEMA,
            "candidate_set_id": candidate_set.candidate_set_id,
            "registration_id": candidate_set.registration_id,
            "checkpoint_key": candidate_set.checkpoint_key,
            "route_plan_id": candidate_set.route_plan_id,
            "route_admission_id": candidate_set.route_admission_id,
            "comparison_id": comparison.comparison_id,
            "candidate_set_hash": candidate_artifact.content_hash,
            "comparison_registration_hash": comparison_artifact.content_hash,
            "comparison_report_id": report.report_id,
            "comparison_report_hash": report_artifact.content_hash,
            "candidate_version_ids": list(candidate_set.version_ids),
            "blockers": list(report.blockers),
            "terminalized_at": _timestamp(terminalized_at),
        }
        terminal = _terminal_batch_from_dict(
            {
                "terminal_id": ("event-impact-triage-terminal-batch-" + canonical_hash(core)),
                **core,
            }
        )
        terminal_artifact = self.artifacts.put_json(terminal.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT artifact_hash FROM event_impact_triage_terminal_batches
                WHERE candidate_set_id = ?
                """,
                (candidate_set.candidate_set_id,),
            ).fetchone()
            if existing is not None:
                stored = _terminal_batch_from_dict(
                    self.artifacts.read_json(cast(str, existing["artifact_hash"]))
                )
                if stored != terminal:
                    raise ValueError("stored triage terminal batch conflicts with caller")
                return stored
            completed = connection.execute(
                """
                SELECT decision_id FROM event_impact_triage_decisions
                WHERE candidate_set_id = ?
                """,
                (candidate_set.candidate_set_id,),
            ).fetchone()
            if completed is not None:
                raise ValueError("a completed triage Decision cannot be terminalized")
            placeholders = ",".join("?" for _ in candidate_set.version_ids)
            classified = connection.execute(
                f"""
                SELECT version_id FROM event_impact_triage_classified_versions
                WHERE route_admission_id = ? AND checkpoint_key = ?
                  AND version_id IN ({placeholders})
                """,
                (
                    candidate_set.route_admission_id,
                    candidate_set.checkpoint_key,
                    *candidate_set.version_ids,
                ),
            ).fetchone()
            terminal_conflict = connection.execute(
                f"""
                SELECT version_id FROM event_impact_triage_terminal_versions
                WHERE route_admission_id = ? AND checkpoint_key = ?
                  AND version_id IN ({placeholders})
                """,
                (
                    candidate_set.route_admission_id,
                    candidate_set.checkpoint_key,
                    *candidate_set.version_ids,
                ),
            ).fetchone()
            if classified is not None or terminal_conflict is not None:
                raise ValueError("triage terminal batch overlaps an already handled version")
            connection.execute(
                """
                INSERT INTO event_impact_triage_terminal_batches(
                    terminal_id, candidate_set_id, registration_id, checkpoint_key,
                    route_plan_id, route_admission_id, terminalized_at, artifact_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    terminal.terminal_id,
                    terminal.candidate_set_id,
                    terminal.registration_id,
                    terminal.checkpoint_key,
                    terminal.route_plan_id,
                    terminal.route_admission_id,
                    _timestamp(terminal.terminalized_at),
                    terminal_artifact.content_hash,
                ),
            )
            connection.executemany(
                """
                INSERT INTO event_impact_triage_terminal_versions(
                    route_admission_id, checkpoint_key, version_id, terminal_id
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    (
                        terminal.route_admission_id,
                        terminal.checkpoint_key,
                        version_id,
                        terminal.terminal_id,
                    )
                    for version_id in terminal.candidate_version_ids
                ),
            )
        for content_hash, expected in (
            (candidate_artifact.content_hash, candidate_set.to_dict()),
            (comparison_artifact.content_hash, comparison.to_dict()),
            (report_artifact.content_hash, report.to_dict()),
        ):
            if self.artifacts.read_json(content_hash) != expected:
                raise ValueError("triage terminal authority artifact changed after commit")
        return terminal

    def terminal_batch(self, candidate_set_id: str) -> EventImpactTriageTerminalBatch:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT artifact_hash FROM event_impact_triage_terminal_batches
                WHERE candidate_set_id = ?
                """,
                (candidate_set_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown triage terminal batch: {candidate_set_id}")
        terminal = _terminal_batch_from_dict(
            self.artifacts.read_json(cast(str, row["artifact_hash"]))
        )
        candidate = self.artifacts.read_json(terminal.candidate_set_hash)
        comparison = self.artifacts.read_json(terminal.comparison_registration_hash)
        report = self.artifacts.read_json(terminal.comparison_report_hash)
        if not all(isinstance(item, dict) for item in (candidate, comparison, report)):
            raise ValueError("triage terminal batch authority artifacts must be objects")
        candidate_payload = cast(dict[str, object], candidate)
        comparison_payload = cast(dict[str, object], comparison)
        report_payload = cast(dict[str, object], report)
        if (
            candidate_payload.get("candidate_set_id") != terminal.candidate_set_id
            or comparison_payload.get("comparison_id") != terminal.comparison_id
            or comparison_payload.get("candidate_set_id") != terminal.candidate_set_id
            or report_payload.get("report_id") != terminal.comparison_report_id
            or report_payload.get("comparison_id") != terminal.comparison_id
            or report_payload.get("batch_gate_passed") is not False
        ):
            raise ValueError("triage terminal batch authority artifacts are inconsistent")
        return terminal

    def reopen_failed_work_comparison_terminal(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        comparison: EventImpactTriageWorkComparisonRegistration,
        report: EventImpactTriageWorkComparisonReport,
    ) -> EventImpactTriageTerminalBatch | None:
        """Reopen the exact terminal committed for one already-replayed failed Report."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT terminal_id FROM event_impact_triage_terminal_batches
                WHERE candidate_set_id = ?
                """,
                (candidate_set.candidate_set_id,),
            ).fetchone()
        if row is None:
            return None
        terminal = self.terminal_batch(candidate_set.candidate_set_id)
        if (
            terminal.terminal_id != cast(str, row["terminal_id"])
            or terminal.candidate_set_id != candidate_set.candidate_set_id
            or terminal.registration_id != candidate_set.registration_id
            or terminal.checkpoint_key != candidate_set.checkpoint_key
            or terminal.route_plan_id != candidate_set.route_plan_id
            or terminal.route_admission_id != candidate_set.route_admission_id
            or terminal.candidate_set_hash != canonical_hash(candidate_set.to_dict())
            or terminal.comparison_id != comparison.comparison_id
            or terminal.comparison_registration_hash != canonical_hash(comparison.to_dict())
            or terminal.comparison_report_id != report.report_id
            or terminal.comparison_report_hash != canonical_hash(report.to_dict())
            or terminal.candidate_version_ids != candidate_set.version_ids
            or terminal.blockers != report.blockers
            or report.batch_gate_passed
        ):
            raise ValueError("stored triage terminal batch differs from failed comparison")
        return terminal

    def _persist_decision(
        self,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        decision: EventImpactTriageDecision,
    ) -> EventImpactTriageDecision:
        candidate_artifact = self.artifacts.put_json(candidate_set.to_dict())
        proposal_artifact = self.artifacts.put_json(proposal.to_dict())
        decision_artifact = self.artifacts.put_json(decision.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT * FROM event_impact_triage_decisions WHERE candidate_set_id = ?
                """,
                (candidate_set.candidate_set_id,),
            ).fetchone()
            if existing is not None:
                stored_candidate, stored_proposal, stored_decision = self._reopen(existing)
                if stored_candidate != candidate_set or stored_proposal != proposal:
                    raise ValueError(
                        "stored triage Candidate Set or Proposal conflicts with caller"
                    )
                if type(decision.run_evidence) is TriageWorkDecisionEvidence:
                    self._assert_work_retry_compatible(stored_decision, decision.run_evidence)
                elif stored_decision.run_evidence != decision.run_evidence:
                    raise ValueError("stored triage Run Evidence conflicts with caller")
                return stored_decision
            terminal_batch = connection.execute(
                """
                SELECT terminal_id FROM event_impact_triage_terminal_batches
                WHERE candidate_set_id = ?
                """,
                (candidate_set.candidate_set_id,),
            ).fetchone()
            if terminal_batch is not None:
                raise ValueError("a terminally failed triage batch cannot become a Decision")
            conflict = connection.execute(
                """
                SELECT version_id, decision_id
                FROM event_impact_triage_classified_versions
                WHERE route_admission_id = ? AND checkpoint_key = ?
                  AND version_id IN ({})
                """.format(",".join("?" for _ in candidate_set.version_ids)),
                (
                    candidate_set.route_admission_id,
                    candidate_set.checkpoint_key,
                    *candidate_set.version_ids,
                ),
            ).fetchone()
            if conflict is not None:
                raise ValueError(
                    "triage candidate version was already classified by another Decision"
                )
            terminal_conflict = connection.execute(
                """
                SELECT version_id
                FROM event_impact_triage_terminal_versions
                WHERE route_admission_id = ? AND checkpoint_key = ?
                  AND version_id IN ({})
                """.format(",".join("?" for _ in candidate_set.version_ids)),
                (
                    candidate_set.route_admission_id,
                    candidate_set.checkpoint_key,
                    *candidate_set.version_ids,
                ),
            ).fetchone()
            if terminal_conflict is not None:
                raise ValueError("triage candidate version belongs to a terminally failed batch")
            connection.execute(
                """
                INSERT INTO event_impact_triage_decisions(
                    decision_id, candidate_set_id, registration_id, checkpoint_key,
                    route_plan_id, route_admission_id, decided_at,
                    candidate_artifact_hash, proposal_artifact_hash,
                    decision_artifact_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.decision_id,
                    candidate_set.candidate_set_id,
                    candidate_set.registration_id,
                    candidate_set.checkpoint_key,
                    candidate_set.route_plan_id,
                    candidate_set.route_admission_id,
                    _timestamp(decision.decided_at),
                    candidate_artifact.content_hash,
                    proposal_artifact.content_hash,
                    decision_artifact.content_hash,
                ),
            )
            connection.executemany(
                """
                INSERT INTO event_impact_triage_classified_versions(
                    route_admission_id, checkpoint_key, version_id, decision_id
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    (
                        candidate_set.route_admission_id,
                        candidate_set.checkpoint_key,
                        version_id,
                        decision.decision_id,
                    )
                    for version_id in candidate_set.version_ids
                ),
            )
        return decision

    @staticmethod
    def _assert_work_retry_compatible(
        stored_decision: EventImpactTriageDecision,
        current_evidence: TriageWorkDecisionEvidence,
    ) -> None:
        if type(stored_decision.run_evidence) is TriageWorkDecisionEvidence:
            if stored_decision.run_evidence != current_evidence:
                raise ValueError("stored triage Work Evidence conflicts with caller")
        elif type(stored_decision.run_evidence) is LegacyTriageWorkDecisionEvidence:
            expected_legacy = LegacyTriageWorkDecisionEvidence(
                plan_id=current_evidence.plan_id,
                work_manifest_id=current_evidence.work_manifest_id,
                completed_member_count=current_evidence.completed_member_count,
                usage_ledger_hash=current_evidence.usage_ledger_hash,
                authority_receipt_hash=current_evidence.authority_receipt_hash,
            )
            if stored_decision.run_evidence != expected_legacy:
                raise ValueError("stored legacy triage Work Evidence conflicts with caller")
        else:
            raise ValueError("stored triage Decision uses another run evidence revision")
        if stored_decision.decided_at != current_evidence.finished_at:
            raise ValueError("stored triage Work Decision time conflicts with authority")

    def classified_version_ids(
        self,
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
        at: datetime,
    ) -> tuple[str, ...]:
        """Return versions completed or terminally handled in one route epoch."""

        _strict_utc(at, "triage classification query at")
        with self._connect() as connection:
            rows = tuple(
                connection.execute(
                    """
                    SELECT decision.*
                    FROM event_impact_triage_decisions AS decision
                    WHERE decision.registration_id = ?
                      AND decision.checkpoint_key = ?
                      AND decision.route_plan_id = ?
                      AND decision.route_admission_id = ?
                      AND decision.decided_at <= ?
                    ORDER BY decision.decided_at, decision.decision_id
                    """,
                    (
                        registration_id,
                        checkpoint_key,
                        route_plan_id,
                        route_admission_id,
                        _timestamp(at),
                    ),
                ).fetchall()
            )
            terminal_rows = tuple(
                connection.execute(
                    """
                    SELECT terminal.*
                    FROM event_impact_triage_terminal_batches AS terminal
                    WHERE terminal.registration_id = ?
                      AND terminal.checkpoint_key = ?
                      AND terminal.route_plan_id = ?
                      AND terminal.route_admission_id = ?
                      AND terminal.terminalized_at <= ?
                    ORDER BY terminal.terminalized_at, terminal.terminal_id
                    """,
                    (
                        registration_id,
                        checkpoint_key,
                        route_plan_id,
                        route_admission_id,
                        _timestamp(at),
                    ),
                ).fetchall()
            )
        versions: set[str] = set()
        for row in rows:
            candidate_set, _, decision = self._reopen(row)
            if (
                candidate_set.registration_id != registration_id
                or candidate_set.checkpoint_key != checkpoint_key
                or candidate_set.route_plan_id != route_plan_id
                or candidate_set.route_admission_id != route_admission_id
                or decision.decided_at > at
            ):
                raise ValueError("triage Decision index differs from its stored artifacts")
            versions.update(candidate_set.version_ids)
        for row in terminal_rows:
            terminal = _terminal_batch_from_dict(
                self.artifacts.read_json(cast(str, row["artifact_hash"]))
            )
            if (
                terminal.registration_id != registration_id
                or terminal.checkpoint_key != checkpoint_key
                or terminal.route_plan_id != route_plan_id
                or terminal.route_admission_id != route_admission_id
                or terminal.terminalized_at > at
            ):
                raise ValueError("triage terminal index differs from its stored artifact")
            versions.update(terminal.candidate_version_ids)
        return tuple(sorted(versions))

    def get_context(
        self,
        candidate_set_id: str,
    ) -> tuple[
        EventImpactTriageCandidateSet,
        EventImpactTriageProposal,
        EventImpactTriageDecision,
    ]:
        """Reopen one exact authoritative Triage Decision and its inputs."""

        row = self._row_for_candidate_set(candidate_set_id)
        if row is None:
            raise KeyError(f"unknown event impact Triage Candidate Set: {candidate_set_id}")
        return self._reopen(row)

    def get_watch_context_by_cluster(
        self,
        cluster_id: str,
    ) -> tuple[
        EventImpactTriageCandidateSet,
        EventImpactTriageProposal,
        EventImpactTriageDecision,
        TriageClusterProposal,
    ]:
        """Resolve one globally content-identified Attention Watch parent."""

        if not cluster_id.startswith("event-impact-triage-cluster-"):
            raise ValueError("Triage Watch lookup requires a cluster ID")
        with self._connect() as connection:
            rows = tuple(
                connection.execute(
                    """
                    SELECT * FROM event_impact_triage_decisions
                    ORDER BY decided_at, decision_id
                    """
                ).fetchall()
            )
        matches: list[
            tuple[
                EventImpactTriageCandidateSet,
                EventImpactTriageProposal,
                EventImpactTriageDecision,
                TriageClusterProposal,
            ]
        ] = []
        for row in rows:
            candidate, proposal, decision = self._reopen(row)
            clusters = tuple(item for item in proposal.clusters if item.cluster_id == cluster_id)
            if clusters:
                if len(clusters) != 1:
                    raise ValueError("Triage Decision contains a duplicate cluster identity")
                matches.append((candidate, proposal, decision, clusters[0]))
        if not matches:
            raise KeyError(f"unknown event impact Triage cluster: {cluster_id}")
        if len(matches) != 1:
            raise ValueError("Triage cluster identity resolves to multiple Decisions")
        candidate, proposal, decision, cluster = matches[0]
        if cluster_id not in decision.attention_watch_cluster_ids:
            raise ValueError("Triage cluster is not authorized for Attention Watch")
        return candidate, proposal, decision, cluster

    def route_epoch_contexts(
        self,
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
        at: datetime,
    ) -> tuple[
        tuple[
            EventImpactTriageCandidateSet,
            EventImpactTriageProposal,
            EventImpactTriageDecision,
            TriageClusterProposal,
        ],
        ...,
    ]:
        """Reopen and ready-time order every cluster in one admitted route epoch."""

        _strict_utc(at, "triage route epoch query at")
        with self._connect() as connection:
            rows = tuple(
                connection.execute(
                    """
                    SELECT * FROM event_impact_triage_decisions
                    WHERE registration_id = ? AND checkpoint_key = ?
                      AND route_plan_id = ? AND route_admission_id = ?
                      AND decided_at <= ?
                    ORDER BY decided_at, decision_id
                    """,
                    (
                        registration_id,
                        checkpoint_key,
                        route_plan_id,
                        route_admission_id,
                        _timestamp(at),
                    ),
                ).fetchall()
            )
        contexts: list[
            tuple[
                datetime,
                str,
                str,
                EventImpactTriageCandidateSet,
                EventImpactTriageProposal,
                EventImpactTriageDecision,
                TriageClusterProposal,
            ]
        ] = []
        for row in rows:
            candidate_set, proposal, decision = self._reopen(row)
            if (
                candidate_set.registration_id != registration_id
                or candidate_set.checkpoint_key != checkpoint_key
                or candidate_set.route_plan_id != route_plan_id
                or candidate_set.route_admission_id != route_admission_id
                or decision.decided_at > at
            ):
                raise ValueError("triage Decision index differs from its stored artifacts")
            availability = {
                item.version_id: item.first_available_at for item in candidate_set.observations
            }
            for cluster in proposal.clusters:
                ready_at = max(
                    availability[version_id]
                    for version_id in (
                        *cluster.candidate_version_ids,
                        *cluster.evidence_version_ids,
                    )
                )
                contexts.append(
                    (
                        ready_at,
                        decision.decision_id,
                        cluster.cluster_id,
                        candidate_set,
                        proposal,
                        decision,
                        cluster,
                    )
                )
        return tuple(
            (candidate_set, proposal, decision, cluster)
            for (
                _,
                _,
                _,
                candidate_set,
                proposal,
                decision,
                cluster,
            ) in sorted(contexts, key=lambda item: item[:3])
        )

    def _row_for_candidate_set(self, candidate_set_id: str) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT * FROM event_impact_triage_decisions WHERE candidate_set_id = ?
                """,
                (candidate_set_id,),
            ).fetchone()

    def _reopen(
        self, row: sqlite3.Row
    ) -> tuple[
        EventImpactTriageCandidateSet,
        EventImpactTriageProposal,
        EventImpactTriageDecision,
    ]:
        candidate_set = event_impact_triage_candidate_set_from_dict(
            self.artifacts.read_json(cast(str, row["candidate_artifact_hash"]))
        )
        proposal = event_impact_triage_proposal_from_dict(
            self.artifacts.read_json(cast(str, row["proposal_artifact_hash"]))
        )
        decision = event_impact_triage_decision_from_dict(
            self.artifacts.read_json(cast(str, row["decision_artifact_hash"]))
        )
        if (
            candidate_set.candidate_set_id != cast(str, row["candidate_set_id"])
            or decision.decision_id != cast(str, row["decision_id"])
            or proposal.proposal_id != decision.proposal_id
            or candidate_set.candidate_set_id != decision.candidate_set_id
        ):
            raise ValueError("triage Decision index differs from its stored artifacts")
        return candidate_set, proposal, decision


def _terminal_batch_from_dict(value: object) -> EventImpactTriageTerminalBatch:
    if not isinstance(value, dict):
        raise TypeError("triage terminal batch must be an object")
    payload = cast(dict[str, object], value)
    expected = {
        "schema_version",
        "terminal_id",
        "candidate_set_id",
        "registration_id",
        "checkpoint_key",
        "route_plan_id",
        "route_admission_id",
        "comparison_id",
        "candidate_set_hash",
        "comparison_registration_hash",
        "comparison_report_id",
        "comparison_report_hash",
        "candidate_version_ids",
        "blockers",
        "terminalized_at",
    }
    if set(payload) != expected or payload.get("schema_version") != (
        EVENT_IMPACT_TRIAGE_TERMINAL_BATCH_SCHEMA
    ):
        raise ValueError("triage terminal batch fields are invalid")

    def text(name: str) -> str:
        item = payload.get(name)
        if not isinstance(item, str) or not item or item != item.strip():
            raise ValueError(f"triage terminal batch {name} is invalid")
        return item

    def texts(name: str) -> tuple[str, ...]:
        raw = payload.get(name)
        if not isinstance(raw, list):
            raise ValueError(f"triage terminal batch {name} is invalid")
        values = cast(list[object], raw)
        if any(not isinstance(item, str) or not item or item != item.strip() for item in values):
            raise ValueError(f"triage terminal batch {name} is invalid")
        return tuple(cast(list[str], values))

    version_ids = texts("candidate_version_ids")
    blockers = texts("blockers")
    if not version_ids or len(version_ids) != len(set(version_ids)):
        raise ValueError("triage terminal batch versions must be non-empty and unique")
    if not blockers or blockers != tuple(sorted(set(blockers))):
        raise ValueError("triage terminal batch blockers must be sorted and unique")
    candidate_hash = text("candidate_set_hash")
    comparison_hash = text("comparison_registration_hash")
    report_hash = text("comparison_report_hash")
    for content_hash, label in (
        (candidate_hash, "Candidate Set"),
        (comparison_hash, "comparison registration"),
        (report_hash, "comparison report"),
    ):
        if len(content_hash) != 64:
            raise ValueError(f"triage terminal {label} hash is invalid")
        int(content_hash, 16)
    terminalized_at = datetime.fromisoformat(text("terminalized_at").replace("Z", "+00:00"))
    _strict_utc(terminalized_at, "triage terminalized_at")
    terminal = EventImpactTriageTerminalBatch(
        terminal_id=text("terminal_id"),
        candidate_set_id=text("candidate_set_id"),
        registration_id=text("registration_id"),
        checkpoint_key=text("checkpoint_key"),
        route_plan_id=text("route_plan_id"),
        route_admission_id=text("route_admission_id"),
        comparison_id=text("comparison_id"),
        candidate_set_hash=candidate_hash,
        comparison_registration_hash=comparison_hash,
        comparison_report_id=text("comparison_report_id"),
        comparison_report_hash=report_hash,
        candidate_version_ids=version_ids,
        blockers=blockers,
        terminalized_at=terminalized_at,
    )
    expected_id = f"event-impact-triage-terminal-batch-{canonical_hash(terminal.core_dict())}"
    if terminal.terminal_id != expected_id or terminal.to_dict() != payload:
        raise ValueError("triage terminal batch is not canonical")
    return terminal


def _strict_utc(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
