from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

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


class EventImpactTriageDecisionStore:
    """Append-only authority for versions completed by formal Event Impact Triage."""

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
        """Return only fully reopened Decisions visible in one route epoch at ``at``."""

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


def _strict_utc(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
