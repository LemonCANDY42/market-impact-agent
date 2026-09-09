"""In-flight budget admission in an existing parent Run Journal.

Reservations are operational evidence, not a second Usage Ledger. Known usage
settles a reservation by reference to its immutable native response; an unknown
generation keeps its conservative reservation. All children share the parent's
Journal/Run and cancellation rather than creating another billing store.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.runtime_store import RunJournal, RuntimeEvent


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
class ModelBudgetGroupMember:
    """Worst-case physical calls for one complete arm, including its successors."""

    member_id: str
    max_requests: int
    max_cost_microusd: int

    def __post_init__(self) -> None:
        if not self.member_id or self.member_id != self.member_id.strip():
            raise ValueError("group member requires a trimmed identity")
        if (
            type(self.max_requests) is not int
            or type(self.max_cost_microusd) is not int
            or self.max_requests < 1
            or self.max_cost_microusd < 1
        ):
            raise ValueError("group member requires positive request and cost ceilings")

    def to_dict(self) -> dict[str, object]:
        return {
            "member_id": self.member_id,
            "max_requests": self.max_requests,
            "max_cost_microusd": self.max_cost_microusd,
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
    group_id: str | None = None
    group_member_id: str | None = None

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
        if self.group_member_id is not None and self.group_id is None:
            raise ValueError("group member has no admitted group")

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
        return self._summary(self.journal.events(self.owner_run_id))

    def _summary(self, events: tuple[RuntimeEvent, ...]) -> dict[str, int]:
        reserved: dict[str, int] = {}
        settled: dict[str, int] = {}
        for event in events:
            if (
                event.event_type == "pi.budget.group_admitted"
                and event.payload["binding"] != self.binding
            ):
                raise ValueError("parent model budget changed; cannot reset spent authority")
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

    def _group_members(
        self, events: tuple[RuntimeEvent, ...] | None = None
    ) -> list[dict[str, object]]:
        """Derive unused allocations from the same request events; never call them spend."""
        if events is None:
            events = self.journal.events(self.owner_run_id)
        settled = {
            cast(str, event.payload["request_key"]): cast(
                int, event.payload["estimated_cost_microusd"]
            )
            for event in events
            if event.event_type == "pi.budget.settled"
        }
        result: list[dict[str, object]] = []
        for event in events:
            if event.event_type != "pi.budget.group_admitted":
                continue
            if event.payload["binding"] != self.binding:
                raise ValueError("parent model budget changed; cannot reset spent authority")
            for member in cast(list[dict[str, object]], event.payload["members"]):
                reservations = [
                    item.payload
                    for item in events
                    if item.event_type == "pi.budget.reserved"
                    and item.payload.get("group_id") == event.payload["group_id"]
                    and item.payload.get("group_member_id") == member["member_id"]
                ]
                committed = sum(
                    settled.get(
                        cast(str, item["request_key"]), cast(int, item["reserved_microusd"])
                    )
                    for item in reservations
                )
                result.append(
                    {
                        **member,
                        "group_id": event.payload["group_id"],
                        "scope": event.payload.get("scope"),
                        "allocated_requests_remaining": max(
                            0, cast(int, member["max_requests"]) - len(reservations)
                        ),
                        "allocated_microusd_remaining": max(
                            0, cast(int, member["max_cost_microusd"]) - committed
                        ),
                    }
                )
        return result

    def group_allocation_summary(self) -> dict[str, int]:
        members = self._group_members()
        if self.scope_limits:
            members = [member for member in members if member["scope"] == self.scope]
        return {
            "allocated_requests_remaining": sum(
                cast(int, item["allocated_requests_remaining"]) for item in members
            ),
            "allocated_microusd_remaining": sum(
                cast(int, item["allocated_microusd_remaining"]) for item in members
            ),
        }

    async def admit_group(
        self,
        *,
        group_id: str,
        members: tuple[ModelBudgetGroupMember, ...],
        call_graph_hash: str,
    ) -> ModelBudget:
        """Atomically protect every arm before the first physical request.

        This is execution-time admission under an already authorized parent. Offline
        preparation must only calculate ceilings and must never invoke this method.
        Allocations are not Usage and do not create physical request reservations.
        """
        from market_impact_agent.agent_engine import (
            _BudgetExceeded,  # pyright: ignore[reportPrivateUsage]
        )

        if self.group_id is not None or not group_id or group_id != group_id.strip():
            raise ValueError("group admission requires an ungrouped owner and a new identity")
        if len(members) < 2 or len({member.member_id for member in members}) != len(members):
            raise ValueError("complete paired or multi-arm group requires distinct members")
        if len(call_graph_hash) != 64 or any(c not in "0123456789abcdef" for c in call_graph_hash):
            raise ValueError("group admission requires its frozen call graph hash")
        payload: dict[str, object] = {
            "binding": self.binding,
            "group_id": group_id,
            "members": [member.to_dict() for member in members],
            "call_graph_hash": call_graph_hash,
            **({"scope": self.scope} if self.scope_limits else {}),
        }
        while (claim := self.journal.try_claim_run(f"{self.owner_run_id}.model-budget")) is None:
            self.check_cancel()
            await asyncio.sleep(0.02)
        try:
            self.check_cancel()
            event_id = f"{self.owner_run_id}.budget.group.{canonical_hash(group_id)}"
            previous = self.journal.event(event_id)
            if previous is not None:
                if previous.payload != payload:
                    raise ValueError("admitted group cannot change its arms or call graph")
                return replace(self, group_id=group_id)
            events = self.journal.events(self.owner_run_id)
            state = self._summary(events)
            allocated = self._group_members(events)
            requests = sum(member.max_requests for member in members)
            cost = sum(member.max_cost_microusd for member in members)
            held_requests = sum(
                cast(int, item["allocated_requests_remaining"]) for item in allocated
            )
            held_cost = sum(cast(int, item["allocated_microusd_remaining"]) for item in allocated)
            if state["physical_requests"] + held_requests + requests > self.max_requests or (
                self.max_cost_microusd is not None
                and state["known_cost_microusd"] + state["reserved_microusd"] + held_cost + cost
                > self.max_cost_microusd
            ):
                raise _BudgetExceeded("parent budget cannot fund the complete group")
            if self.scope_limits:
                limit = next(item for item in self.scope_limits if item.name == self.scope)
                scoped = self._scope_summary(events)
                scoped_held = sum(
                    cast(int, item["allocated_microusd_remaining"])
                    for item in allocated
                    if item["scope"] == self.scope
                )
                if (
                    scoped["known_cost_microusd"] + scoped["reserved_microusd"] + scoped_held + cost
                    > limit.max_cost_microusd
                ):
                    raise _BudgetExceeded("registered stage cannot fund the complete group")
            self._append(
                f"budget.group.{canonical_hash(group_id)}", "pi.budget.group_admitted", payload
            )
            return replace(self, group_id=group_id)
        finally:
            claim.release()

    def for_group_member(self, member_id: str) -> ModelBudget:
        if not any(
            member["group_id"] == self.group_id
            and member["member_id"] == member_id
            and member["scope"] == self.scope
            for member in self._group_members()
        ):
            raise PermissionError("member does not belong to this admitted group and stage")
        return replace(self, group_member_id=member_id)

    def scope_summary(self) -> dict[str, int]:
        return self._scope_summary(self.journal.events(self.owner_run_id))

    def _scope_summary(self, events: tuple[RuntimeEvent, ...]) -> dict[str, int]:
        if not self.scope_limits:
            return self._summary(events)
        limit = next(item for item in self.scope_limits if item.name == self.scope)
        reserved: dict[str, int] = {}
        settled: dict[str, int] = {}
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
            # Settlement may be appended by another process during admission.
            # Derive spend, outstanding physical requests and unused allocations
            # from one immutable Journal prefix, never a mixture of two snapshots.
            events = self.journal.events(self.owner_run_id)
            state = self._summary(events)
            members = self._group_members(events)
            own = next(
                (
                    item
                    for item in members
                    if item["group_id"] == self.group_id
                    and item["member_id"] == self.group_member_id
                    and item["scope"] == self.scope
                ),
                None,
            )
            if self.group_id is not None:
                if own is None:
                    raise PermissionError("physical request requires an admitted group member")
                if (
                    cast(int, own["allocated_requests_remaining"]) < 1
                    or cast(int, own["allocated_microusd_remaining"]) < estimated_cost_microusd
                ):
                    raise _BudgetExceeded("complete group member allowance is exhausted")
            others = [item for item in members if item is not own]
            held_requests = sum(cast(int, item["allocated_requests_remaining"]) for item in others)
            held_cost = sum(cast(int, item["allocated_microusd_remaining"]) for item in others)
            event_id = f"{self.owner_run_id}.budget.{canonical_hash(request_key)}.reserved"
            if self.journal.event(event_id) is not None:
                # Replaying completion is allowed elsewhere; repeating an admitted
                # physical dispatch with the same identity is never a retry policy.
                raise PermissionError("physical request was already admitted; no regeneration")
            if state["physical_requests"] + held_requests >= self.max_requests or (
                self.max_cost_microusd is not None
                and state["known_cost_microusd"]
                + state["reserved_microusd"]
                + held_cost
                + estimated_cost_microusd
                > self.max_cost_microusd
            ):
                raise _BudgetExceeded(
                    "parent model budget has no unreserved request/cost allowance"
                )
            if self.scope_limits:
                limit = next(item for item in self.scope_limits if item.name == self.scope)
                scoped = self._scope_summary(events)
                if (
                    scoped["known_cost_microusd"]
                    + scoped["reserved_microusd"]
                    + sum(
                        cast(int, item["allocated_microusd_remaining"])
                        for item in others
                        if item["scope"] == self.scope
                    )
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
                    **(
                        {"group_id": self.group_id, "group_member_id": self.group_member_id}
                        if self.group_id is not None
                        else {}
                    ),
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
