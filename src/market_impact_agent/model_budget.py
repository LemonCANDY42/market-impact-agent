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
class ModelBudgetScope:
    name: str
    max_cost_microusd: int
    prior_cost_microusd: int = 0
    prior_reserved_microusd: int = 0

    def __post_init__(self) -> None:
        if not self.name or self.name != self.name.strip():
            raise ValueError("budget scope requires a name")
        if min(self.max_cost_microusd, self.prior_cost_microusd, self.prior_reserved_microusd) < 0:
            raise ValueError("budget scope amounts must be nonnegative")
        if self.prior_cost_microusd + self.prior_reserved_microusd > self.max_cost_microusd:
            raise ValueError("budget scope is already over its authorized limit")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "max_cost_microusd": self.max_cost_microusd,
            "prior_cost_microusd": self.prior_cost_microusd,
            "prior_reserved_microusd": self.prior_reserved_microusd,
        }


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
    prior_reserved_microusd: int = 0
    prior_unsettled_requests: int = 0
    scope_limits: tuple[ModelBudgetScope, ...] = ()
    scope: str | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.prior_requests <= self.max_requests or self.max_requests < 1:
            raise ValueError("invalid physical request budget")
        if min(
            self.prior_cost_microusd, self.prior_reserved_microusd, self.prior_unsettled_requests
        ) < 0 or (
            self.max_cost_microusd is not None
            and self.prior_cost_microusd + self.prior_reserved_microusd > self.max_cost_microusd
        ):
            raise ValueError("invalid model cost budget")
        if self.prior_unsettled_requests > self.prior_requests:
            raise ValueError("unknown prior requests must remain in the physical denominator")
        if self.scope_limits:
            names = [limit.name for limit in self.scope_limits]
            if len(set(names)) != len(names) or self.scope not in names:
                raise ValueError("model budget requires a registered scope")
            if (
                sum(limit.prior_cost_microusd for limit in self.scope_limits)
                != self.prior_cost_microusd
                or sum(limit.prior_reserved_microusd for limit in self.scope_limits)
                != self.prior_reserved_microusd
                or self.max_cost_microusd is None
                or sum(limit.max_cost_microusd for limit in self.scope_limits)
                > self.max_cost_microusd
            ):
                raise ValueError("budget scopes must reconcile with the parent authorization")
        elif self.scope is not None:
            raise ValueError("model budget scope has no registered limit")

    @property
    def binding(self) -> dict[str, object]:
        result: dict[str, object] = {
            "max_requests": self.max_requests,
            "max_cost_microusd": self.max_cost_microusd,
            "prior_requests": self.prior_requests,
            "prior_cost_microusd": self.prior_cost_microusd,
        }
        # Preserve legacy authorization hashes. New scope limits are frozen once
        # for the shared parent, not separately for each continuation or stage.
        if self.prior_reserved_microusd or self.prior_unsettled_requests:
            result["prior_reserved_microusd"] = self.prior_reserved_microusd
            result["prior_unsettled_requests"] = self.prior_unsettled_requests
        if self.scope_limits:
            result["scope_limits"] = [limit.to_dict() for limit in self.scope_limits]
        return result

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
            "reserved_microusd": self.prior_reserved_microusd
            + sum(cost for key, cost in reserved.items() if key not in settled),
            "unsettled_requests": self.prior_unsettled_requests
            + len(reserved.keys() - settled.keys()),
        }

    def scope_summary(self) -> dict[str, int]:
        if not self.scope_limits:
            return self.summary()
        limit = next(item for item in self.scope_limits if item.name == self.scope)
        reserved: dict[str, int] = {}
        settled: dict[str, int] = {}
        events = self.journal.events(self.owner_run_id)
        for event in events:
            if event.event_type == "pi.budget.reserved":
                if event.payload["binding"] != self.binding:
                    raise ValueError("parent model budget changed; cannot reset spent authority")
                if event.payload.get("scope") == self.scope:
                    reserved[cast(str, event.payload["request_key"])] = cast(
                        int, event.payload["reserved_microusd"]
                    )
        for event in events:
            if event.event_type == "pi.budget.settled" and event.payload["request_key"] in reserved:
                settled[cast(str, event.payload["request_key"])] = cast(
                    int, event.payload["estimated_cost_microusd"]
                )
        return {
            "physical_requests": len(reserved),
            "known_cost_microusd": limit.prior_cost_microusd + sum(settled.values()),
            "reserved_microusd": limit.prior_reserved_microusd
            + sum(cost for key, cost in reserved.items() if key not in settled),
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
            if self.scope_limits:
                limit = next(item for item in self.scope_limits if item.name == self.scope)
                scoped = self.scope_summary()
                if (
                    scoped["known_cost_microusd"]
                    + scoped["reserved_microusd"]
                    + estimated_cost_microusd
                    > limit.max_cost_microusd
                ):
                    raise _BudgetExceeded("registered study stage has no unreserved cost allowance")
            self._append(
                f"budget.{canonical_hash(request_key)}.reserved",
                "pi.budget.reserved",
                {
                    "binding": self.binding,
                    "request_key": request_key,
                    "reserved_microusd": estimated_cost_microusd,
                    **({"scope": self.scope} if self.scope_limits else {}),
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
