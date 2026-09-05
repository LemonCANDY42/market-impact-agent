"""In-flight budget admission in an existing parent Run Journal.

Reservations are operational evidence, not a second Usage Ledger. Known usage
settles a reservation by reference to its immutable native response; an unknown
generation keeps its conservative reservation. All children share the parent's
Journal/Run and cancellation rather than creating another billing store.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.runtime_store import RunJournal


@dataclass(frozen=True, slots=True)
class ModelBudget:
    journal: RunJournal
    owner_run_id: str
    max_requests: int
    max_cost_microusd: int | None
    prior_requests: int = 0
    prior_cost_microusd: int = 0
    append: Callable[[str, str, dict[str, object]], None] | None = None
    check_cancel: Callable[[], None] = lambda: None

    def __post_init__(self) -> None:
        if not 0 <= self.prior_requests <= self.max_requests or self.max_requests < 1:
            raise ValueError("invalid physical request budget")
        if self.prior_cost_microusd < 0 or (
            self.max_cost_microusd is not None and self.prior_cost_microusd > self.max_cost_microusd
        ):
            raise ValueError("invalid model cost budget")

    @property
    def binding(self) -> dict[str, object]:
        return {
            "max_requests": self.max_requests,
            "max_cost_microusd": self.max_cost_microusd,
            "prior_requests": self.prior_requests,
            "prior_cost_microusd": self.prior_cost_microusd,
        }

    def _append(self, suffix: str, kind: str, payload: dict[str, object]) -> None:
        if self.append is not None:
            self.append(suffix, kind, payload)
        else:
            self.journal.append(
                run_id=self.owner_run_id,
                event_id=f"{self.owner_run_id}.{suffix}",
                event_type=kind,
                observed_at=datetime.now(UTC),
                payload=payload,
            )

    def summary(self) -> dict[str, int]:
        reserved: dict[str, int] = {}
        settled: dict[str, int] = {}
        for event in self.journal.events(self.owner_run_id):
            if event.event_type == "pi.budget.reserved":
                if event.payload["binding"] != self.binding:
                    raise ValueError("parent model budget changed; cannot reset spent authority")
                reserved[cast(str, event.payload["request_key"])] = cast(
                    int, event.payload["reserved_microusd"]
                )
            elif event.event_type == "pi.budget.settled":
                settled[cast(str, event.payload["request_key"])] = cast(
                    int, event.payload["estimated_cost_microusd"]
                )
        if not settled.keys() <= reserved.keys():
            raise ValueError("model budget settlement has no reservation")
        return {
            "physical_requests": self.prior_requests + len(reserved),
            "known_cost_microusd": self.prior_cost_microusd + sum(settled.values()),
            "reserved_microusd": sum(cost for key, cost in reserved.items() if key not in settled),
            "unsettled_requests": len(reserved.keys() - settled.keys()),
        }

    async def reserve(self, request_key: str, estimated_cost_microusd: int) -> None:
        from market_impact_agent.agent_engine import (
            _BudgetExceeded,  # pyright: ignore[reportPrivateUsage]
        )

        if estimated_cost_microusd < 0:
            raise ValueError("negative model reservation")
        # Reuse the existing kernel-backed Journal claim. Check + committed append
        # is serialized across workers. A crash releases the lock, not the evidence.
        while (claim := self.journal.try_claim_run(f"{self.owner_run_id}.model-budget")) is None:
            self.check_cancel()
            await asyncio.sleep(0.02)
        try:
            self.check_cancel()
            state = self.summary()
            event_id = f"{self.owner_run_id}.budget.{canonical_hash(request_key)}.reserved"
            if self.journal.event(event_id) is not None:
                # Replaying completion is allowed elsewhere; repeating an admitted
                # physical dispatch with the same identity is never a retry policy.
                raise PermissionError("physical request was already admitted; no regeneration")
            if state["physical_requests"] >= self.max_requests or (
                self.max_cost_microusd is not None
                and state["known_cost_microusd"]
                + state["reserved_microusd"]
                + estimated_cost_microusd
                > self.max_cost_microusd
            ):
                raise _BudgetExceeded(
                    "parent model budget has no unreserved request/cost allowance"
                )
            self._append(
                f"budget.{canonical_hash(request_key)}.reserved",
                "pi.budget.reserved",
                {
                    "binding": self.binding,
                    "request_key": request_key,
                    "reserved_microusd": estimated_cost_microusd,
                },
            )
        finally:
            claim.release()

    def settle(self, request_key: str, *, cost_microusd: int, evidence_ref: str) -> None:
        if cost_microusd < 0 or not evidence_ref:
            raise ValueError("model budget settlement requires nonnegative cost and evidence")
        if (
            self.journal.event(f"{self.owner_run_id}.budget.{canonical_hash(request_key)}.reserved")
            is None
        ):
            raise ValueError("model budget settlement has no admitted request")
        self._append(
            f"budget.{canonical_hash(request_key)}.settled",
            "pi.budget.settled",
            {
                "request_key": request_key,
                "estimated_cost_microusd": cost_microusd,
                "evidence_ref": evidence_ref,
            },
        )
