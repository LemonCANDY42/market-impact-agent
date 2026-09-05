"""One fixed pi cutover qualification; no market or broker execution authority.

The direct upstream executable exists only in this qualification. Every request
still uses the production physical admission, parent budget and Usage Ledger.
Failures remain in the fixed denominator; completed replies are never regenerated.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.agent_engine import (
    AgentRunRequest,
    CancellationToken,
    compose_authoritative_agent_engine,
)
from market_impact_agent.agent_runtime import (
    SkillRegistry,
    ToolAccessContext,
    ToolDescriptor,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import (
    load_builtin_model_provider_profile,
    model_provider_profile_from_dict,
)
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import (
    PI_RUNTIME_ROOT,
    Callback,
    PiRuntimeProvider,
    runtime_identity,
    shared_admission_root,
)
from market_impact_agent.prospective_checkpoint_sets import split_selected_reader
from market_impact_agent.research import EvidenceTier
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus, RuntimeEvent
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord, reconcile_usage_ledgers


def _runner_identity() -> str:
    return canonical_hash(
        {
            "coordinator": sha256(Path(__file__).read_bytes()).hexdigest(),
            "upstream_control": sha256(
                (PI_RUNTIME_ROOT / "test" / "qualification-worker.ts").read_bytes()
            ).hexdigest(),
        }
    )


def _physical_intervals(events: tuple[RuntimeEvent, ...]) -> list[tuple[datetime, datetime]]:
    """Pair physical attempts, never counting retry backoff as network overlap."""
    dispatches = {
        event.event_hash: event
        for event in events
        if event.event_type == "model.attempt.dispatched"
    }
    intervals: list[tuple[datetime, datetime]] = []
    for event in events:
        if event.event_type not in {"model.attempt.succeeded", "model.attempt.failed"}:
            continue
        dispatch = dispatches.get(cast(str, event.payload.get("dispatch_event_hash")))
        if dispatch is not None and dispatch.observed_at < event.observed_at:
            intervals.append((dispatch.observed_at, event.observed_at))
    return intervals


def _prior_usage(roots: tuple[Path, ...]) -> dict[str, object]:
    union = reconcile_usage_ledgers(tuple(root / "pi-canary-usage.sqlite3" for root in roots))
    attempts = sum(record.metrics.provider_attempts for record in union.records)
    return {
        "requests": attempts,
        "cost_microusd": union.total_estimated_cost_microusd,
        "union_hash": union.union_hash,
        "roots": [str(root.resolve()) for root in roots],
    }


def _bind_qualification_authority(
    root: Path, registration: dict[str, object], *, create: bool = False
) -> None:
    """One authorized final batch on this host, not one fresh allowance per directory.

    The immutable pointer owns only the authorization-to-batch identity. Its
    original Run Journal remains the sole reservation and cost authority.
    Atomic link publication also serializes independent processes preparing it.
    """
    scope = shared_admission_root()
    followup = registration.get("experiment") == "pi-runtime-cutover-followup-v1"
    repair = registration.get("experiment") == "pi-runtime-cutover-repair-v1"
    claim = scope / (
        "pi-cutover-repair-authorization.json"
        if repair
        else "pi-cutover-followup-authorization.json"
        if followup
        else "pi-cutover-authorization.json"
    )
    expected = {
        "authorization": "pi-runtime-replacement-usd3-48",
        "state_root": str(root.resolve()),
        "registration_hash": registration["registration_hash"],
        "prior_usage_union_hash": cast(dict[str, object], registration["prior"])["union_hash"],
    }
    if followup or repair:
        parent = cast(dict[str, object], registration["parent"])
        _bind_qualification_authority(
            Path(cast(str, parent["root"])),
            _stored_registration(Path(cast(str, parent["root"]))),
        )
        expected["parent_report_hash"] = parent["report_hash"]
    if create:
        artifact = ArtifactStore(scope / "authorization-artifacts").put_json(expected)
        try:
            os.link(artifact.path, claim)
        except FileExistsError:
            pass
        else:
            descriptor = os.open(scope, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    if claim.is_symlink() or not claim.is_file() or json.loads(claim.read_text()) != expected:
        raise PermissionError("runtime qualification authorization belongs to another final batch")


def _verification(verification: Path) -> dict[str, object]:
    verified = cast(dict[str, object], json.loads(verification.read_text()))
    checks = cast(dict[str, object], verified.get("checks", {}))
    if (
        verified.get("runtime") != runtime_identity()
        or not all(
            checks.get(name) == "passed"
            for name in (
                "ruff",
                "format",
                "pyright",
                "pytest",
                "typescript",
                "node_tests",
                "clean_source_install",
                "independent_review",
            )
        )
        or not verified.get("evidence_refs")
    ):
        raise ValueError("final build lacks bound offline/deployment/independent-review evidence")
    return verified


def prepare_pi_canary(
    root: Path, *, prior_roots: tuple[Path, ...], verification: Path
) -> dict[str, object]:
    """Freeze the final cleaned build only after offline checks and independent review."""
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if (root / "pi-canary-registration.json").exists():
        registration = _registration(root)
        _bind_qualification_authority(root, registration, create=True)
        return registration
    verified = _verification(verification)
    prior = _prior_usage(prior_roots)
    if (prior["requests"], prior["cost_microusd"]) != (8, 25_153):
        raise ValueError("prior runtime authorization does not reconcile to its closed stages")
    profiles: dict[str, object] = {}
    for route, alias in (("cpa", "pi-cpa-luna-max-v2"), ("minimax", "pi-minimax-m3-v2")):
        raw = load_builtin_model_provider_profile(alias).to_dict()
        budget = dict(cast(dict[str, object], raw["budget"]))
        budget.update(max_turns=3, max_estimated_cost_microusd=100_000)
        raw["budget"] = budget
        raw.pop("profile_id")
        raw["profile_id"] = f"model-provider-{canonical_hash(raw)}"
        profiles[route] = model_provider_profile_from_dict(raw).to_dict()
    cases: list[dict[str, object]] = []
    for group in (1, 2):
        for route in profiles:
            for arm in ("direct", "bridge") if group == 1 else ("bridge", "direct"):
                cases.append(
                    {
                        "case": f"{route}-{group}-{arm}",
                        "route": route,
                        "mode": arm,
                        "group": group,
                        "max_requests": 3,
                    }
                )
    cases.extend(
        {
            "case": f"{route}-compression",
            "route": route,
            "mode": "compression",
            "max_requests": 3,
            "history_case": f"{route}-2-bridge",
        }
        for route in profiles
    )
    cases.extend(
        {
            "case": f"cpa-concurrent-{i}",
            "route": "cpa",
            "mode": "bridge",
            "max_requests": 2,
            "restart": i == 1,
        }
        for i in (1, 2)
    )
    cases.append(
        {
            "case": "cpa-cancelled",
            "route": "cpa",
            "mode": "bridge",
            "max_requests": 0,
            "cancel": True,
        }
    )
    value: dict[str, object] = {
        "experiment": "pi-runtime-cutover-v2",
        "registered_at": datetime.now(UTC).isoformat(),
        "runtime": runtime_identity(),
        "runner_hash": _runner_identity(),
        "verification": verified,
        "prior": prior,
        "total_authorization_microusd": 3_000_000,
        "total_authorization_requests": 48,
        "profiles": profiles,
        "cases": cases,
        "execution_capability": False,
    }
    value["registration_hash"] = canonical_hash(value)
    with (root / "pi-canary-registration.json").open("x", encoding="utf8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    _bind_qualification_authority(root, value, create=True)
    return value


async def _followup_parent(
    root: Path, skill_root: Path
) -> tuple[dict[str, object], dict[str, object]]:
    registration = _registration(root, replay_only=True)
    if registration["experiment"] != "pi-runtime-cutover-v2":
        raise ValueError("only one focused follow-up of the original qualification is authorized")
    report = await run_pi_canary(root, skill_root, replay_only=True)
    cases = cast(list[dict[str, object]], report["cases"])
    retained = {
        f"{route}-{group}-{mode}"
        for route in ("cpa", "minimax")
        for group in (1, 2)
        for mode in ("direct", "bridge")
    } | {"cpa-compression", "minimax-compression", "cpa-concurrent-1"}
    if (
        {row["case"] for row in cases if row["passed"]} != retained
        or {row["case"] for row in cases if not row["passed"]} != {"cpa-concurrent-2"}
        or report["not_run"] != ["cpa-cancelled"]
        or not report["reconciled"]
        or not all(
            row["passed"] for row in cast(list[dict[str, object]], report["cache_comparison"])
        )
    ):
        raise ValueError("parent does not have the approved retained qualification evidence")
    return registration, report


async def prepare_pi_canary_followup(
    root: Path, *, parent_root: Path, verification: Path, skill_root: Path
) -> dict[str, object]:
    """One explicitly authorized continuation-only pair, not a replacement batch."""
    verified = _verification(verification)
    parent, report = await _followup_parent(parent_root, skill_root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if (root / "pi-canary-registration.json").exists():
        value = _registration(root)
        if cast(dict[str, object], value.get("parent", {})).get("root") != str(
            parent_root.resolve()
        ):
            raise ValueError("follow-up parent differs from the frozen authority")
        _bind_qualification_authority(root, value, create=True)
        return value
    previous = cast(dict[str, object], parent["prior"])
    prior = _prior_usage(
        (*(Path(path) for path in cast(list[str], previous["roots"])), parent_root)
    )
    usage = cast(dict[str, int], report["usage"])
    if (prior["requests"], prior["cost_microusd"]) != (
        usage["physical_requests"],
        usage["known_cost_microusd"],
    ) or 48 - cast(int, prior["requests"]) < 12:
        raise ValueError("follow-up lacks reconciled remaining authority for its fixed pair")
    value: dict[str, object] = {
        "experiment": "pi-runtime-cutover-followup-v1",
        "registered_at": datetime.now(UTC).isoformat(),
        "runtime": runtime_identity(),
        "runner_hash": _runner_identity(),
        "verification": verified,
        "prior": prior,
        "profiles": parent["profiles"],
        "parent": {
            "root": str(parent_root.resolve()),
            "registration_hash": parent["registration_hash"],
            "report_hash": report["report_hash"],
        },
        "total_authorization_microusd": 3_000_000,
        "total_authorization_requests": 48,
        "cases": [
            {
                "case": f"cpa-concurrent-{i}",
                "route": "cpa",
                "mode": "bridge",
                "max_requests": 6,
                "continuation_only": True,
                "history_case": "cpa-2-bridge",
            }
            for i in (1, 2)
        ]
        + [
            {
                "case": "cpa-cancelled",
                "route": "cpa",
                "mode": "bridge",
                "max_requests": 0,
                "cancel": True,
            }
        ],
        "execution_capability": False,
    }
    value["registration_hash"] = canonical_hash(value)
    with (root / "pi-canary-registration.json").open("x", encoding="utf8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    _bind_qualification_authority(root, value, create=True)
    return value


def _stored_registration(root: Path) -> dict[str, object]:
    value = cast(dict[str, object], json.loads((root / "pi-canary-registration.json").read_text()))
    core = {key: item for key, item in value.items() if key != "registration_hash"}
    if canonical_hash(core) != value["registration_hash"]:
        raise ValueError("canary registration changed")
    return value


def _registration(root: Path, *, replay_only: bool = False) -> dict[str, object]:
    value = _stored_registration(root)
    if value["runtime"] != runtime_identity() or (
        not replay_only and value["runner_hash"] != _runner_identity()
    ):
        raise ValueError("canary runtime changed; use its Git version for historical replay")
    return value


def _closed_evidence(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    """Verify retained evidence, never execute or reinterpret an old runtime."""
    registration = _stored_registration(root)
    _bind_qualification_authority(root, registration)
    report = cast(dict[str, object], json.loads((root / "pi-canary-report.json").read_text()))
    if (
        canonical_hash({key: item for key, item in report.items() if key != "report_hash"})
        != report["report_hash"]
        or report["registration_hash"] != registration["registration_hash"]
        or report["runtime"] != registration["runtime"]
        or not report["reconciled"]
        or UsageLedger(root / "pi-canary-usage.sqlite3").ledger_hash != report["ledger_hash"]
    ):
        raise ValueError("retained qualification evidence changed or is unreconciled")
    for case in cast(list[dict[str, object]], report["cases"]):
        store = LocalDataSnapshotStore(root / "cases" / cast(str, case["case"]))
        journal = RunJournal.authoritative(store)
        run_id = cast(str, case["run_id"])
        if (
            not journal.get_run(run_id).status.terminal
            or journal.journal_hash(run_id) != case["journal_hash"]
        ):
            raise ValueError("retained case journal changed or has no terminal")
        store.artifacts.read_json(cast(str, case["terminal"]))
    return registration, report


def _repair_parent(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    focused, failed = _closed_evidence(root)
    if focused["experiment"] != "pi-runtime-cutover-followup-v1" or failed["stage_passed"]:
        raise ValueError("repair requires the closed failed focused qualification")
    parent = cast(dict[str, object], focused["parent"])
    original, retained = _closed_evidence(Path(cast(str, parent["root"])))
    if (
        original["registration_hash"] != parent["registration_hash"]
        or retained["report_hash"] != parent["report_hash"]
        or failed.get("inherited_report") != retained
        or focused["profiles"] != original["profiles"]
        or {row["case"] for row in cast(list[dict[str, object]], failed["cases"])}
        != {"cpa-concurrent-1", "cpa-concurrent-2"}
        or any(row["passed"] for row in cast(list[dict[str, object]], failed["cases"]))
        or failed["not_run"] != ["cpa-cancelled"]
    ):
        raise ValueError("repair predecessor differs from the authorized failed pair")
    return original, failed


def prepare_pi_canary_repair(
    root: Path, *, parent_root: Path, verification: Path
) -> dict[str, object]:
    """One newly authorized build-repair qualification within cumulative authority."""
    verified = _verification(verification)
    original, failed = _repair_parent(parent_root)
    previous = _stored_registration(parent_root)
    previous_prior = cast(dict[str, object], previous["prior"])
    prior = _prior_usage(
        (*(Path(path) for path in cast(list[str], previous_prior["roots"])), parent_root)
    )
    usage = cast(dict[str, int], failed["usage"])
    changed = {
        name
        for name, value in cast(dict[str, str], runtime_identity()["files"]).items()
        if cast(dict[str, dict[str, str]], previous["runtime"])["files"].get(name) != value
    }
    if (
        (prior["requests"], prior["cost_microusd"])
        != (usage["physical_requests"], usage["known_cost_microusd"])
        or 48 - cast(int, prior["requests"]) < 9
        or cast(int, prior["cost_microusd"]) >= 3_000_000
        or not changed
        or changed - {"loop_adapter", "authority_callbacks", "context_contract", "process_adapter"}
        or verified.get("retained_report_hash")
        != cast(dict[str, object], failed["inherited_report"])["report_hash"]
        or verified.get("reviewed_changed_files") != sorted(changed)
    ):
        raise ValueError(
            "repair requires reviewed bounded build differences and remaining authority"
        )
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if (root / "pi-canary-registration.json").exists():
        value = _registration(root)
        if cast(dict[str, object], value["parent"])["root"] != str(parent_root.resolve()):
            raise ValueError("repair parent differs from frozen authority")
        _bind_qualification_authority(root, value, create=True)
        return value
    cases: list[dict[str, object]] = [
        {
            "case": f"cpa-concurrent-{number}",
            "route": "cpa",
            "mode": "bridge",
            "max_requests": 6,
            "continuation_only": True,
            "history_case": "cpa-2-bridge",
            "restart": number == 1,
        }
        for number in (1, 2)
    ]
    cases.append(
        {
            "case": "minimax-continuation",
            "route": "minimax",
            "mode": "bridge",
            "max_requests": 6,
            "continuation_only": True,
            "history_case": "minimax-2-bridge",
        }
    )
    cases.extend(
        {
            "case": f"{route}-compression",
            "route": route,
            "mode": "compression",
            "max_requests": 6,
            "history_case": f"{route}-2-bridge",
        }
        for route in ("cpa", "minimax")
    )
    cases.append(
        {
            "case": "cpa-cancelled",
            "route": "cpa",
            "mode": "bridge",
            "max_requests": 0,
            "cancel": True,
        }
    )
    value = {
        "experiment": "pi-runtime-cutover-repair-v1",
        "registered_at": datetime.now(UTC).isoformat(),
        "runtime": runtime_identity(),
        "runner_hash": _runner_identity(),
        "verification": verified,
        "prior": prior,
        "profiles": original["profiles"],
        "parent": {
            "root": str(parent_root.resolve()),
            "registration_hash": previous["registration_hash"],
            "report_hash": failed["report_hash"],
        },
        "history_root": cast(dict[str, object], previous["parent"])["root"],
        "total_authorization_microusd": 3_000_000,
        "total_authorization_requests": 48,
        "cases": cases,
        "execution_capability": False,
    }
    value["registration_hash"] = canonical_hash(value)
    with (root / "pi-canary-registration.json").open("x", encoding="utf8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    _bind_qualification_authority(root, value, create=True)
    return value


class _CompletionRestart(BaseException):
    """Deliberate child replacement after a durable response, never a model retry."""


class _QualificationProvider(PiRuntimeProvider):
    def __init__(
        self,
        *args: object,
        case: dict[str, object],
        journal: RunJournal,
        history: list[object] | None = None,
        **kwargs: object,
    ):
        super().__init__(*args, **kwargs)  # pyright: ignore[reportArgumentType]
        self.case, self.history, self.journal = case, history, journal

    def entry_point(self) -> Path:
        if self.case["mode"] in {"direct", "compression"}:
            return PI_RUNTIME_ROOT / "test" / "qualification-worker.ts"
        return super().entry_point()

    async def execute(self, payload: dict[str, object], callback: Callback) -> dict[str, object]:
        assert self.budget is not None
        budget = self.budget
        run_id = cast(str, payload["runId"])

        async def controlled(method: str, body: dict[str, object]) -> dict[str, object]:
            if method in {"model_admit", "attempt_start"}:
                from market_impact_agent.agent_engine import (
                    _BudgetExceeded,  # pyright: ignore[reportPrivateUsage]
                )

                # At model_admit, a completed response is replay, not another call.
                # This also leaves a known pre-dispatch cap exhaustion outside
                # model.turn.started instead of mislabeling it a lost response.
                replay = method == "model_admit" and (
                    self.journal.event(f"{run_id}.turn.{body['number']}") is not None
                    or self.journal.event(f"{run_id}.pi.response.{body['number']}") is not None
                )
                dispatched = sum(
                    event.event_type == "pi.budget.reserved"
                    and str(event.payload["request_key"]).startswith(f"{run_id}:")
                    for event in budget.journal.events(budget.owner_run_id)
                )
                if not replay and dispatched >= cast(int, self.case["max_requests"]):
                    raise _BudgetExceeded(
                        "fixed qualification case exhausted its physical request cap"
                    )
            result = await callback(method, body)
            if method == "model_completed" and self.case.get("restart"):
                event_id = f"{budget.owner_run_id}.restart.{canonical_hash(run_id)}"
                if budget.journal.event(event_id) is None:
                    budget.journal.append(
                        run_id=budget.owner_run_id,
                        event_id=event_id,
                        event_type="pi.acceptance.completed_boundary_restart",
                        observed_at=datetime.now(UTC),
                        payload={"run_id": run_id},
                    )
                    raise _CompletionRestart()
            return result

        if self.case["mode"] in {"direct", "compression"}:
            payload = {**payload, "qualification": self.case["mode"]}
        if self.history is not None:
            payload = {**payload, "nativeMessages": self.history}
        return await super().execute(payload, controlled)


def _inputs(store: LocalDataSnapshotStore, registration: dict[str, object]):
    at = datetime.fromisoformat(cast(str, registration["registered_at"]))
    facts: dict[str, dict[str, object]] = {
        "event_revelation": {
            "evidence_id": "synthetic-event",
            "fact": "Synthetic outage lasts 18 hours; duration beyond that is unknown.",
        },
        "exposure_candidates": {
            "evidence_id": "synthetic-exposure",
            "fact": (
                "600028.XSHG is the only authorized research target; "
                "no verified net exposure coefficient."
            ),
        },
        "market_context": {
            "evidence_id": "synthetic-market",
            "fact": (
                "No executable quote or broker/account authority is provided. "
                "Valuation is not an execution price."
            ),
        },
    }
    evidence = tuple(
        EvidenceReference(
            evidence_id=cast(str, fact["evidence_id"]),
            claim_id=kind,
            source_ref=f"synthetic://pi-canary/{kind}",
            source_tier=EvidenceTier.OFFICIAL,
            available_at=at,
            content_hash=canonical_hash(fact),
            summary=f"Synthetic {kind} evidence is available through its selected reader.",
        )
        for kind, fact in facts.items()
    )
    pack = EvidencePack.build(
        event_id="pi-synthetic-outage",
        as_of=at,
        research_question="What does this synthetic outage imply and what remains unknown?",
        evidence=evidence,
        pattern_packs=(),
        allowed_targets=("600028.XSHG",),
        data_gaps=(),
    )
    registry = ToolRegistry(store.artifacts)
    for kind, fact in facts.items():

        async def read(args: dict[str, object], fact: dict[str, object] = fact) -> object:
            matched = not args.get("query") or str(args["query"]) in str(fact["fact"])
            records = [fact] if matched and not args.get("offset") else []
            return {
                "records": records,
                "page": {
                    "total_available": 1,
                    "total_matched": int(matched),
                    "returned": len(records),
                    "offset": args.get("offset", 0),
                    "next_offset": None,
                },
            }

        descriptor = ToolDescriptor(
            name=f"lookup_{kind}",
            version=f"canary-{canonical_hash(fact)}",
            description=f"Read frozen synthetic {kind}.",
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "query": {"type": "string"},
                    "offset": {"type": "integer", "minimum": 0},
                },
            },
            required_capabilities=frozenset({"market.read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=2,
            max_result_bytes=10000,
            handler=read,
        )
        for part in split_selected_reader(descriptor):
            registry.register(part)
    access = ToolAccessContext(
        allowed_capabilities=frozenset({"market.read"}),
        allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
        allowed_tools=frozenset(
            f"{prefix}_{kind}"
            for prefix in ("read_selected", "read_next", "search")
            for kind in facts
        ),
    )
    return pack, registry, access


def _native_history(
    store: LocalDataSnapshotStore, journal: RunJournal, run_id: str
) -> list[object]:
    events = journal.events(run_id)
    inputs = [event for event in events if event.event_type == "pi.context.frozen"]
    turns = [event for event in events if event.event_type == "pi.response.received"]
    if not inputs or not turns or journal.get_run(run_id).status != RunStatus.COMPLETED:
        raise ValueError("compression source did not complete with actual native history")
    frozen = cast(
        dict[str, object], store.artifacts.read_json(cast(str, inputs[-1].payload["artifact_hash"]))
    )
    context = cast(dict[str, object], frozen["context"])
    messages = cast(list[dict[str, object]], context["messages"])
    response = cast(
        dict[str, object], store.artifacts.read_json(cast(str, turns[-1].payload["artifact_hash"]))
    )
    # Retain real paired assistant/tool messages; current authority is re-pinned separately.
    return [message for message in messages if message["role"] in {"assistant", "toolResult"}] + [
        response["message"]
    ]


def _preserves_observed_duration(text: str) -> bool:
    """A numeric fact check for this fixture, not a general semantic judge."""
    durations = re.findall(r"(?<![\w.])(\d+(?:\.\d+)?)\s*[- ]?(?:hours?\b|小时)", text, re.I)
    return bool(durations) and all(float(value) == 18 for value in durations)


async def run_pi_canary(
    root: Path, skill_root: Path, *, replay_only: bool = False
) -> dict[str, object]:
    report_path = root / "pi-canary-report.json"
    replay_only = replay_only or report_path.exists()
    registration = _registration(root, replay_only=replay_only)
    _bind_qualification_authority(root, registration)
    inherited: dict[str, object] | None = None
    parent_registration: dict[str, object] | None = None
    failed_predecessor: dict[str, object] | None = None
    if registration.get("experiment") == "pi-runtime-cutover-repair-v1":
        parent_ref = cast(dict[str, object], registration["parent"])
        parent_root = Path(cast(str, parent_ref["root"]))
        parent_registration, failed_predecessor = _repair_parent(parent_root)
        inherited = cast(dict[str, object], failed_predecessor["inherited_report"])
        previous = _stored_registration(parent_root)
        if (
            previous["registration_hash"] != parent_ref["registration_hash"]
            or failed_predecessor["report_hash"] != parent_ref["report_hash"]
            or parent_registration["profiles"] != registration["profiles"]
            or registration["history_root"] != cast(dict[str, object], previous["parent"])["root"]
        ):
            raise ValueError("repair inherited evidence or model routes changed")
    if registration.get("experiment") == "pi-runtime-cutover-followup-v1":
        parent_ref = cast(dict[str, object], registration["parent"])
        parent_registration, inherited = await _followup_parent(
            Path(cast(str, parent_ref["root"])), skill_root
        )
        if (
            parent_registration["registration_hash"] != parent_ref["registration_hash"]
            or inherited["report_hash"] != parent_ref["report_hash"]
            or parent_registration["profiles"] != registration["profiles"]
        ):
            raise ValueError("follow-up inherited evidence or model routes changed")
    with (root / "pi-canary.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store = LocalDataSnapshotStore(root)
        journal = RunJournal.authoritative(store)
        ledger = UsageLedger(root / "pi-canary-usage.sqlite3")
        owner = f"pi-cutover-{registration['registration_hash']}"
        journal.start_run(
            run_id=owner,
            config_hash=canonical_hash(registration),
            created_at=datetime.fromisoformat(cast(str, registration["registered_at"])),
        )
        prior = cast(dict[str, object], registration["prior"])
        if _prior_usage(tuple(Path(path) for path in cast(list[str], prior["roots"]))) != prior:
            raise ValueError("previous authorization usage changed")
        budget = ModelBudget(
            journal,
            owner,
            48,
            3_000_000,
            cast(int, prior["requests"]),
            cast(int, prior["cost_microusd"]),
        )
        cases = cast(list[dict[str, object]], registration["cases"])
        profiles = cast(dict[str, dict[str, object]], registration["profiles"])
        ids = {cast(str, case["case"]): f"{owner}.{case['case']}" for case in cases}
        permit = PiRuntimePermit(
            canonical_hash(registration["runtime"]),
            tuple(
                model_provider_profile_from_dict(profile).route_identity
                for profile in profiles.values()
            ),
            cast(str, registration["registration_hash"]),
            tuple(ids.values()),
            owner,
        )
        # A terminal batch can only be reopened, never extended after a failed case.

        async def one(case: dict[str, object]) -> dict[str, object]:
            name = cast(str, case["case"])
            profile = model_provider_profile_from_dict(profiles[cast(str, case["route"])])
            case_store = LocalDataSnapshotStore(root / "cases" / name)
            history = None
            if "history_case" in case:
                source = cast(str, case["history_case"])
                history_root, history_id = root, ids.get(source)
                if parent_registration is not None:
                    history_root = Path(
                        cast(
                            str,
                            registration.get("history_root")
                            or cast(dict[str, object], registration["parent"])["root"],
                        )
                    )
                    history_id = f"pi-cutover-{parent_registration['registration_hash']}.{source}"
                assert history_id is not None
                history_store = LocalDataSnapshotStore(history_root / "cases" / source)
                history = _native_history(
                    history_store, RunJournal.authoritative(history_store), history_id
                )
            provider = _QualificationProvider(
                profile,
                case=case,
                journal=RunJournal.authoritative(case_store),
                history=history,
                dispatch_allowed=not replay_only,
                budget=budget,
                permit=permit,
            )
            pack, registry, access = _inputs(case_store, registration)
            if case.get("continuation_only"):
                access = replace(access, allowed_tools=frozenset())
            engine = compose_authoritative_agent_engine(
                store=case_store,
                provider=provider,
                config=profile.runtime_config(),
                tool_registry=registry,
                skill_registry=SkillRegistry(skill_root),
                secret_values=tuple(filter(None, (os.environ.get(profile.credential_env, ""),))),
            )
            request = AgentRunRequest(
                run_id=ids[name],
                evidence_pack=pack,
                selected_skills=()
                if case.get("continuation_only")
                else ("earnings-reassessment-readers",),
                tool_access=access,
                research_instruction=(
                    "Synthetic concurrency acceptance only. The native history contains the "
                    "three previously read frozen evidence records. No new retrieval is needed "
                    "or authorized. Return a JudgmentProposal, not an order, preserving the "
                    "observed duration in numeric hours and unknown net exposure/quote. "
                    "Abstention is legitimate. A previous answer is not additional evidence."
                )
                if case.get("continuation_only")
                else (
                    "Synthetic runtime acceptance only. Read all three selected evidence tools "
                    "without filters before deciding. State the observed outage duration in "
                    "numeric hours in your summary, distinguish unknown net exposure, and do "
                    "not invent a quote. Abstention is legitimate. Return the required "
                    "JudgmentProposal, not an order."
                ),
            )
            if replay_only and not engine.journal.get_run(ids[name]).status.terminal:
                raise ValueError("qualification has no terminal to replay")
            binding = engine.execution_binding(request, runtime_ref="pi-cutover-v2")
            cancellation = CancellationToken()
            if case.get("cancel"):
                cancellation.cancel()
            try:
                try:
                    result = await engine.run(request, cancellation=cancellation)
                except _CompletionRestart:
                    await provider.close()
                    result = await engine.run(request, cancellation=cancellation)
            finally:
                await provider.close()
            events = engine.journal.events(ids[name])
            ledger.append(
                UsageRecord.from_result(
                    experiment_id=cast(str, registration["registration_hash"]),
                    arm_id=name,
                    recorded_at=events[-1].observed_at,
                    provider_profile_id=profile.profile_id,
                    provider_profile_hash=profile.profile_hash,
                    execution_binding_hash=binding.binding_hash,
                    run_journal_hash=engine.journal.journal_hash(ids[name]),
                    result=result,
                )
            )
            reads = {
                event.payload["tool_name"]
                for event in events
                if event.event_type == "tool.call.completed"
            }
            expected = {
                f"read_selected_{kind}"
                for kind in ("event_revelation", "exposure_candidates", "market_context")
            }
            consumed = (
                result.judgment is not None
                and _preserves_observed_duration(result.judgment.proposal.summary)
                and (expected <= reads or history is not None)
            )
            compactions = [event for event in events if event.event_type == "pi.context.compacted"]
            native_inputs = [event for event in events if event.event_type == "pi.context.frozen"]
            completed = [event for event in events if event.event_type == "model.turn.completed"]
            contexts = [
                cast(
                    dict[str, object],
                    case_store.artifacts.read_json(cast(str, event.payload["artifact_hash"])),
                )
                for event in native_inputs
            ]
            decision_contexts = [
                cast(dict[str, object], item["context"])
                for item in contexts
                if item["purpose"] == "decision"
            ]
            prefix = None
            if decision_contexts:
                first = decision_contexts[0]
                prefix = canonical_hash(
                    {
                        "system": first.get("systemPrompt"),
                        "tools": first.get("tools"),
                        "first_messages": first["messages"] if history is None else [],
                    }
                )
            physical = sum(event.event_type == "model.attempt.dispatched" for event in events)
            checks = result.status == RunStatus.COMPLETED and consumed
            retained_compaction_evidence: bool | None = None
            if case["mode"] == "compression":
                summaries = [
                    cast(
                        dict[str, object],
                        case_store.artifacts.read_json(cast(str, event.payload["entry_hash"])),
                    ).get("summary", "")
                    for event in compactions
                ]
                retained_compaction_evidence = (
                    len(summaries) == 2
                    and all(
                        isinstance(summary, str)
                        and _preserves_observed_duration(summary)
                        and all(
                            ref in summary
                            for ref in ("synthetic-event", "synthetic-exposure", "synthetic-market")
                        )
                        for summary in summaries
                    )
                    and bool(decision_contexts)
                    and bool(decision_contexts[-1].get("systemPrompt"))
                    and expected
                    <= {
                        tool["name"]
                        for tool in cast(list[dict[str, object]], decision_contexts[-1]["tools"])
                    }
                    and json.dumps(summaries[-1], ensure_ascii=False)[1:-1]
                    in json.dumps(decision_contexts[-1]["messages"], ensure_ascii=False)
                )
                checks = checks and retained_compaction_evidence
            if case.get("cancel"):
                checks = result.status == RunStatus.CANCELLED and physical == 0
            return {
                "case": name,
                "mode": case["mode"],
                "route": case["route"],
                "group": case.get("group"),
                "status": result.status.value,
                "passed": checks,
                "consumed_selected_evidence": consumed,
                "terminal": result.terminal_store_hash,
                "run_id": ids[name],
                "physical_dispatches": physical,
                "cache_observations": [event.payload["usage"] for event in completed],
                "prefix_hash": prefix,
                "compactions": len(compactions),
                "retained_compaction_evidence": retained_compaction_evidence,
                "journal_hash": engine.journal.journal_hash(ids[name]),
                "history_hash": canonical_hash(history) if history is not None else None,
            }

        results: list[dict[str, object]] = []
        existing_report = (
            cast(dict[str, object], json.loads(report_path.read_text()))
            if report_path.exists()
            else None
        )
        prior_cases = (
            {item["case"] for item in cast(list[dict[str, object]], existing_report["cases"])}
            if existing_report
            else None
        )
        index = 0
        while index < len(cases):
            case = cases[index]
            if replay_only and prior_cases is not None and case["case"] not in prior_cases:
                break
            if str(case["case"]) == "cpa-concurrent-1":
                pair = await asyncio.gather(*(one(item) for item in cases[index : index + 2]))
                results.extend(pair)
                index += 2
                if not all(row["passed"] for row in pair):
                    break
            else:
                row = await one(case)
                results.append(row)
                index += 1
                if not row["passed"]:
                    break
        cache: list[dict[str, object]] = []
        for route in () if inherited else profiles:
            deltas: list[float] = []
            pairs_complete = True
            for group in (1, 2):
                pair = [row for row in results if row["route"] == route and row["group"] == group]
                by_mode = {row["mode"]: row for row in pair}
                if (
                    set(by_mode) != {"direct", "bridge"}
                    or by_mode["direct"]["prefix_hash"] != by_mode["bridge"]["prefix_hash"]
                ):
                    pairs_complete = False
                    continue
                rates: dict[str, float] = {}
                for mode, row in by_mode.items():
                    usage = cast(list[dict[str, object]], row["cache_observations"])
                    # First request is cold; a warm request must actually exist.
                    if len(usage) < 2 or any(
                        type(item.get("cache_read_tokens")) is not int for item in usage[1:]
                    ):
                        continue
                    inputs = sum(cast(int, item["input_tokens"]) for item in usage[1:])
                    if inputs > 0:
                        rates[cast(str, mode)] = (
                            sum(cast(int, item["cache_read_tokens"]) for item in usage[1:]) / inputs
                        )
                if len(rates) != 2:
                    pairs_complete = False
                else:
                    deltas.append(rates["bridge"] - rates["direct"])
            cache.append(
                {
                    "route": route,
                    "warm_rate_deltas": deltas,
                    "passed": pairs_complete
                    and len(deltas) == 2
                    and not all(delta < -0.05 for delta in deltas),
                }
            )
        if inherited is not None:
            cache = cast(list[dict[str, object]], inherited["cache_comparison"])
        state = budget.summary()
        new_attempts = sum(cast(int, row["physical_dispatches"]) for row in results)
        accounted = sum(item.record.metrics.estimated_cost_microusd for item in ledger.records())
        reconciled = (
            state["physical_requests"] == cast(int, prior["requests"]) + new_attempts
            and state["known_cost_microusd"] == cast(int, prior["cost_microusd"]) + accounted
            and state["unsettled_requests"] == 0
        )
        report: dict[str, object] = {
            "registration_hash": registration["registration_hash"],
            "runtime": registration["runtime"],
            "cases": results,
            "not_run": [
                case["case"]
                for case in cases
                if case["case"] not in {row["case"] for row in results}
            ],
            "cache_comparison": cache,
            "usage": state,
            "ledger_hash": ledger.ledger_hash,
            "reconciled": reconciled,
            "stage_passed": len(results) == len(cases)
            and all(row["passed"] for row in results)
            and all(row["passed"] for row in cache)
            and reconciled,
            "runtime_accepted": False,
            "execution_capability": False,
        }
        if inherited is not None:
            intervals: list[list[tuple[datetime, datetime]]] = []
            for name in ("cpa-concurrent-1", "cpa-concurrent-2"):
                case_journal = RunJournal.authoritative(
                    LocalDataSnapshotStore(root / "cases" / name)
                )
                intervals.append(_physical_intervals(case_journal.events(ids[name])))
            overlap = any(
                max(left[0], right[0]) < min(left[1], right[1])
                for left in intervals[0]
                for right in intervals[1]
            )
            report["inherited_report"] = inherited
            if failed_predecessor is not None:
                report["failed_predecessor"] = failed_predecessor
            report["concurrent_requests_overlapped"] = overlap
            report["stage_passed"] = bool(report["stage_passed"] and overlap)
        report["report_hash"] = canonical_hash(report)
        if existing_report is not None:
            if report != existing_report:
                raise ValueError("qualification replay differs from its immutable terminal report")
        elif not replay_only:
            with report_path.open("x", encoding="utf8") as output:
                json.dump(report, output, indent=2, sort_keys=True)
                output.flush()
                os.fsync(output.fileno())
        return report
