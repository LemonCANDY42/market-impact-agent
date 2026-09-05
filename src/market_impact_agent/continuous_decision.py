"""Continuous review scheduling on the existing Harness Run Journal.

This coordinator owns neither model transport nor an account ledger. Callers bind
the signed research/portfolio authorities and the streaming execution adapter.
Every accepted decision is durable before it can reach the execution callback.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.decision_thesis import scheduled_review_offsets
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import RunJournal


class ContinuousCadence(StrEnum):
    EXPIRY_ONLY = "expiry_only"
    SCHEDULED = "scheduled"
    EVENT = "event"


@dataclass(frozen=True, slots=True)
class ReviewFrame:
    """Pre-open input references; closing market data never enters this object."""

    cutoff: datetime
    snapshot_ids: tuple[str, ...]
    input_hash: str
    new_fact_ids: tuple[str, ...] = ()
    gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.cutoff, "review cutoff")
        if len(self.input_hash) != 64 or not self.snapshot_ids:
            raise ValueError("review requires frozen input hash and snapshots")
        if len(set(self.new_fact_ids)) != len(self.new_fact_ids):
            raise ValueError("review fact identities must be unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "cutoff": self.cutoff.isoformat(),
            "snapshot_ids": list(self.snapshot_ids),
            "input_hash": self.input_hash,
            "new_fact_ids": list(self.new_fact_ids),
            "gaps": list(self.gaps),
        }


@dataclass(frozen=True, slots=True)
class ContinuousDecision:
    """References returned by Harness authorities, never IDs supplied by a model."""

    research_run_id: str
    portfolio_run_id: str
    horizon_sessions: int
    action: str
    decision_ref: str
    initial_adoption_ref: str | None = None
    research_successor_ref: str | None = None

    def __post_init__(self) -> None:
        if self.horizon_sessions not in {1, 3, 5, 10, 20, 60}:
            raise ValueError("continuous decision requires a registered thesis horizon")
        if self.action not in {"hold", "open", "increase", "reduce", "close", "rotate"}:
            raise ValueError("incomplete work cannot be represented as an account action")
        if not all((self.research_run_id, self.portfolio_run_id, self.decision_ref)):
            raise ValueError("continuous decision requires signed authority references")

    def to_dict(self) -> dict[str, object]:
        return {
            "research_run_id": self.research_run_id,
            "portfolio_run_id": self.portfolio_run_id,
            "horizon_sessions": self.horizon_sessions,
            "action": self.action,
            "decision_ref": self.decision_ref,
            **(
                {"research_successor_ref": self.research_successor_ref}
                if self.research_successor_ref
                else {}
            ),
            **(
                {"initial_adoption_ref": self.initial_adoption_ref}
                if self.initial_adoption_ref
                else {}
            ),
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> ContinuousDecision:
        return cls(
            research_run_id=cast(str, value["research_run_id"]),
            portfolio_run_id=cast(str, value["portfolio_run_id"]),
            horizon_sessions=cast(int, value["horizon_sessions"]),
            action=cast(str, value["action"]),
            decision_ref=cast(str, value["decision_ref"]),
            initial_adoption_ref=cast(str | None, value.get("initial_adoption_ref")),
            research_successor_ref=cast(str | None, value.get("research_successor_ref")),
        )


@dataclass(frozen=True, slots=True)
class PendingReview:
    reason: str
    continuation_ref: str | None = None


type Decide = Callable[
    [ReviewFrame, ContinuousDecision | None, str, frozenset[int], bool],
    Awaitable[ContinuousDecision | PendingReview],
]
type AdvanceAccount = Callable[[int, ContinuousDecision | None], Awaitable[dict[str, object]]]


class ContinuousReviewCoordinator:
    """Merge schedule/event wakes and resume from durable authority references.

    Session zero is the first pre-open decision. H1 expires before session one;
    the final session closes the observation period without another model call.
    An intermediate review may shorten the current cycle, never silently extend
    it. Expiry starts a fresh cycle. Event identities remain consumed thereafter.
    """

    def __init__(
        self,
        journal: RunJournal,
        *,
        episode_id: str,
        registration_hash: str,
        account_scope: str,
        model_arm: str,
        cadence: ContinuousCadence,
        frames: tuple[ReviewFrame, ...],
        decide: Decide,
        advance_account: AdvanceAccount,
        validate_decision: Callable[[ContinuousDecision, ReviewFrame], None],
        shared_initial: ContinuousDecision | None = None,
    ) -> None:
        if not frames or any(a.cutoff >= b.cutoff for a, b in pairwise(frames)):
            raise ValueError("continuous frames require strictly increasing sessions")
        if not account_scope or not model_arm:
            raise ValueError("continuous episode requires account and model scope")
        self.journal = journal
        self.episode_id = episode_id
        self.cadence = cadence
        self.frames = frames
        self.decide = decide
        self.advance_account = advance_account
        self.validate_decision = validate_decision
        self.shared_initial = shared_initial
        binding = {
            "schema_version": "market-impact.continuous-decision-episode.v1",
            "registration_hash": registration_hash,
            "account_scope": account_scope,
            "model_arm": model_arm,
            "cadence": cadence.value,
            "frames": [frame.to_dict() for frame in frames],
            "shared_initial": None if shared_initial is None else shared_initial.to_dict(),
            "time_contract": "preopen_t0_h1_next_preopen_expiry_v1",
            "revision_policy": (
                "horizon_bounded_by_observation_end_event_cap_reset_only_at_expiry_v1"
            ),
        }
        journal.start_run(
            run_id=episode_id,
            config_hash=canonical_hash(binding),
            created_at=frames[0].cutoff,
        )

    async def run(self, *, stop_after_sessions: int | None = None) -> dict[str, object]:
        stop = len(self.frames) if stop_after_sessions is None else stop_after_sessions
        if not 0 <= stop <= len(self.frames):
            raise ValueError("continuous prefix endpoint exceeds registered frames")
        claim = self.journal.try_claim_run(self.episode_id)
        if claim is None:
            return self._report(0, "in_progress", "episode_owned_by_another_worker")
        try:
            return await self._run_claimed(stop)
        finally:
            claim.release()

    async def _run_claimed(self, stop: int) -> dict[str, object]:
        previous: ContinuousDecision | None = None
        consumed: set[str] = set()
        expires = 0
        event_count = 0
        scheduled: set[int] = set()
        for index, frame in enumerate(self.frames[:stop]):
            if frame.gaps:
                return self._report(index, "awaiting_data", "; ".join(frame.gaps))
            fresh = set(frame.new_fact_ids) - consumed
            expiry = index >= expires
            event_due = self.cadence is ContinuousCadence.EVENT and bool(fresh) and event_count < 3
            plan_due = self.cadence is ContinuousCadence.SCHEDULED and index in scheduled
            should_review = previous is None or expiry or event_due or plan_due
            decision: ContinuousDecision | None = None
            if should_review:
                event_key = f"{self.episode_id}.review.{index}"
                completed = self.journal.event(event_key + ".completed")
                maximum_horizon = len(self.frames) - index
                allowed = frozenset(h for h in (1, 3, 5, 10, 20, 60) if h <= maximum_horizon)
                if completed is not None:
                    decision = ContinuousDecision.from_dict(completed.payload)
                elif index == 0 and self.shared_initial is not None:
                    decision = self.shared_initial
                else:
                    started = self.journal.event(event_key + ".started")
                    if started is None:
                        self._append(
                            event_key + ".started",
                            "continuous.review.started",
                            {
                                "frame": frame.to_dict(),
                                "prior": None if previous is None else previous.decision_ref,
                                "allowed_horizons": sorted(allowed),
                                "reasons": [
                                    name
                                    for name, due in (
                                        ("expiry", expiry),
                                        ("schedule", plan_due),
                                        ("event", event_due),
                                    )
                                    if due
                                ],
                            },
                        )
                    # Resume calls must reopen child authority state. They cannot
                    # start a replacement physical generation for unknown state.
                    result = await self.decide(
                        frame, previous, event_key, allowed, started is not None
                    )
                    if isinstance(result, PendingReview):
                        return self._report(
                            index, "incomplete", result.reason, result.continuation_ref
                        )
                    decision = result
                if decision.horizon_sessions not in allowed:
                    raise ValueError("thesis exceeds observation endpoint")
                self.validate_decision(decision, frame)
                if completed is None:
                    self._append(
                        event_key + ".completed", "continuous.review.completed", decision.to_dict()
                    )
                # An intermediate revision may change the thesis horizon within
                # the market observation window. Only an expiry-originated review
                # replenishes the event allowance; revisions cannot reset it.
                if expiry:
                    event_count = 0
                elif event_due:
                    event_count += 1
                consumed.update(fresh)
                previous = decision
                expires = index + decision.horizon_sessions
                scheduled = {
                    index + offset
                    for offset in scheduled_review_offsets(decision.horizon_sessions)
                    if index + offset < len(self.frames)
                }
            day_key = f"{self.episode_id}.account.{index}"
            advanced = self.journal.event(day_key)
            # Executor owns replay and identity. It rebuilds its engine prefix even
            # when the coordinator already committed this day's result.
            account = await self.advance_account(index, decision)
            if advanced is not None:
                if advanced.payload != account:
                    raise ValueError("account prefix replay differs from frozen result")
            else:
                self._append(day_key, "continuous.account.completed", account)
        return self._report(
            stop, "completed" if stop == len(self.frames) else "prefix_complete", None
        )

    def _append(self, event_id: str, kind: str, payload: dict[str, object]) -> None:
        self.journal.append(
            run_id=self.episode_id,
            event_id=event_id,
            event_type=kind,
            observed_at=datetime.now(UTC),
            payload=payload,
        )

    def _report(
        self, completed: int, status: str, reason: str | None, continuation: str | None = None
    ) -> dict[str, object]:
        return {
            "episode_id": self.episode_id,
            "status": status,
            "completed_sessions": completed,
            "registered_sessions": len(self.frames),
            "reason": reason,
            "continuation_ref": continuation,
            "cadence": self.cadence.value,
            "live_execution": False,
        }
