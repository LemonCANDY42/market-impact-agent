"""Authoritative dynamic-horizon research roles executed by the upstream pi loop.

The Harness selects and freezes every input before dispatch.  The model authors
only the analytical thesis.  This is intentionally a single-turn, zero-tool
role: evidence discovery belongs to the prospective/Modeled-PIT preparation
layer, while this authority answers a decision question over that frozen set.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    RunMetrics,
    _PrivilegedEventSink,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.decision_thesis import (
    BaseCaseDirection,
    HorizonBand,
    ResearchThesisV1,
    parse_research_thesis,
    research_thesis_text_normalizations,
)
from market_impact_agent.dynamic_effectiveness import DatePresentation
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_json import load_model_json
from market_impact_agent.model_provider import ModelProvider
from market_impact_agent.pi_execution import (
    PiInvocationContext,
    PiRoleJournal,
    execute_pi_once,
    native_turn,
)
from market_impact_agent.provider_reliability import ProviderAttemptEvent
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

RESEARCH_THESIS_PROMPT = """Act as a senior public-equity analyst. Produce the best
defensible forecast from the point-in-time inputs; uncertainty lowers confidence in
the thesis but future outcomes do not need to be proven first. Distinguish what the
market likely priced in from the incremental fact, trace the transmission to the
registered target, state a counter-scenario, observable invalidation conditions, and
the next review point. Never use facts after the cutoff and never abstain.

Return exactly one JSON object with primary_horizon_sessions (only an allowed
value), base_case_direction
(up/down/rangebound), thesis, priced_in_assessment, transmission (an array of
nonempty strings),
counter_scenario, evidence_refs (frozen evidence IDs), counterevidence_refs (optional
array of frozen evidence IDs), invalidation_conditions (an array of nonempty strings),
review_after_sessions (positive and not beyond the primary horizon), and
typed_unknowns (optional strings). Do not output confidence, target quantity, order,
Run identity, horizon_band, timestamps, hashes, markdown, or prose outside the JSON
object. The Harness derives horizon_band from primary_horizon_sessions. A frozen
evidence item may legitimately appear in both evidence_refs and counterevidence_refs
when it supports competing interpretations.
"""

RESEARCH_THESIS_JUDGE_PROMPT = """Act as a senior investment-committee judge.
Two independent analysts reviewed the same point-in-time evidence and disagree on
direction or horizon. Read the original evidence and both complete analyses. Resolve
the disagreement from first principles: inspect assumptions, what was already priced
in, transmission, counter-scenarios, and invalidation conditions. Do not count votes,
average conclusions, or prefer an analyst because of identity. You may select either
view or form a third conclusion. Future outcomes do not need to be proven first.
\nReturn exactly""" + RESEARCH_THESIS_PROMPT.split("Return exactly", maxsplit=1)[1]

RESEARCH_THESIS_UPDATE_PROMPT = """Act as a senior public-equity analyst reviewing
an earlier signed thesis at a later point-in-time cutoff. The prior thesis is context,
not current truth. Use only the newly frozen evidence and the reopened prior artifact
to keep, revise, or reverse the forecast. Explicitly explain what changed, whether the
old view was already priced in, and which invalidation or review condition fired.
Never use facts after the new cutoff and never abstain.
\nReturn exactly""" + RESEARCH_THESIS_PROMPT.split("Return exactly", maxsplit=1)[1]


@dataclass(frozen=True, slots=True)
class ResearchThesisRunInputs:
    repository: FrozenResearchRepository
    target_id: str
    thesis_epoch: str
    allowed_horizons: frozenset[int]
    date_presentation: DatePresentation = DatePresentation.TRUE_DATE
    candidate_theses: tuple[ResearchThesisV1, ...] = ()
    research_question: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.target_id, "target_id"),
            (self.thesis_epoch, "thesis_epoch"),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be nonempty trimmed text")
        if self.target_id not in self.repository.evidence_pack.allowed_targets:
            raise ValueError("research target is outside the frozen Evidence Pack")
        if not self.allowed_horizons or not self.allowed_horizons <= frozenset(
            {1, 3, 5, 10, 20, 60}
        ):
            raise ValueError("research horizons are not registered")
        if self.candidate_theses and len(self.candidate_theses) != 2:
            raise ValueError("judge input requires exactly two independent analyses")
        if self.research_question is not None and (
            not self.research_question.strip()
            or self.research_question != self.research_question.strip()
        ):
            raise ValueError("research_question must be nonempty trimmed text")
        if self.candidate_theses:
            pack = self.repository.evidence_pack
            evidence_ids = {item.evidence_id for item in pack.evidence}
            for thesis in self.candidate_theses:
                if (
                    thesis.root_event_id != pack.event_id
                    or thesis.as_of != pack.as_of
                    or thesis.primary_horizon_sessions not in self.allowed_horizons
                    or not set(thesis.evidence_refs + thesis.counterevidence_refs) <= evidence_ids
                ):
                    raise ValueError("judge candidate differs from the frozen research input")

    async def selected_inputs(self) -> dict[str, object]:
        pack = self.repository.evidence_pack
        evidence = [
            await self.repository.read_evidence({"evidence_id": item.evidence_id})
            for item in pack.evidence
        ]
        patterns = [
            await self.repository.read_pattern_pack({"pack_id": item.pack_id})
            for item in pack.pattern_packs
        ]
        value: dict[str, object] = {
            "point_in_time_cutoff": _timestamp(pack.as_of),
            "research_question": self.research_question or pack.research_question,
            "target_id": self.target_id,
            "allowed_horizons": sorted(self.allowed_horizons),
            "data_gaps": list(pack.data_gaps),
            "evidence": evidence,
            "pattern_packs": patterns,
        }
        if self.candidate_theses:
            value["candidate_analyses"] = [thesis.to_dict() for thesis in self.candidate_theses]
        if self.date_presentation is DatePresentation.RELATIVE_OFFSET:
            return cast(dict[str, object], _relative_temporal_view(value, pack.as_of.date()))
        return value

    def identity_dict(self) -> dict[str, object]:
        pack = self.repository.evidence_pack
        return {
            "root_event_id": pack.event_id,
            "evidence_pack_id": pack.pack_id,
            "evidence_pack_hash": canonical_hash(pack.to_dict()),
            "as_of": _timestamp(pack.as_of),
            "target_id": self.target_id,
            "thesis_epoch": self.thesis_epoch,
            "allowed_horizons": sorted(self.allowed_horizons),
            "date_presentation": self.date_presentation.value,
            "candidate_thesis_hashes": [
                canonical_hash(item.to_dict()) for item in self.candidate_theses
            ],
            "research_question": self.research_question or pack.research_question,
        }


class ResearchThesisAuthority:
    """Produce and replay one signed ResearchThesis terminal."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        experiment_id: str,
        arm_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        for value, name in ((experiment_id, "experiment_id"), (arm_id, "arm_id")):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be nonempty trimmed text")
        self.store = store
        self.journal = RunJournal.authoritative(store)
        self.experiment_id = experiment_id
        self.arm_id = arm_id
        self.clock = clock
        self.usage_ledger = UsageLedger(store.index_path)
        key = (store.root / ".harness-event-hmac.key").read_bytes()
        self._events = _PrivilegedEventSink(
            journal=self.journal,
            authority_id=store.harness_authority_id,
            signer=lambda value: hmac.new(key, value, sha256).hexdigest(),
        )

    async def analyze(
        self,
        *,
        run_id: str,
        provider: ModelProvider,
        inputs: ResearchThesisRunInputs,
        max_output_tokens: int | None = None,
        prior_thesis_run_id: str | None = None,
    ) -> dict[str, object]:
        output_limit = (
            provider.profile.reserved_output_tokens
            if max_output_tokens is None
            else max_output_tokens
        )
        if not 16 <= output_limit <= provider.profile.reserved_output_tokens:
            raise ValueError("research thesis output limit is outside the accepted Profile")
        if inputs.repository.evidence_pack.as_of > self.clock():
            raise PermissionError("research thesis evidence is after the authority clock")
        claim = self.journal.try_claim_run(run_id)
        if claim is None:
            raise RuntimeError("research thesis Run already has an owner")
        with claim:
            selected = await inputs.selected_inputs()
            prior: dict[str, object] | None = None
            if prior_thesis_run_id is not None:
                prior_thesis, prior = reopen_completed_research_thesis(
                    journal=self.journal,
                    artifact_store=self.store.artifacts,
                    run_id=prior_thesis_run_id,
                )
                if (
                    prior_thesis.as_of >= inputs.repository.evidence_pack.as_of
                    or prior_thesis.primary_horizon_sessions not in inputs.allowed_horizons
                ):
                    raise ValueError("prior thesis is not an earlier compatible review state")
                selected["prior_thesis"] = (
                    _relative_temporal_view(prior, inputs.repository.evidence_pack.as_of.date())
                    if inputs.date_presentation is DatePresentation.RELATIVE_OFFSET
                    else prior
                )
            selected_hash = self.store.artifacts.put_json(selected).content_hash
            role_prompt = (
                RESEARCH_THESIS_JUDGE_PROMPT
                if inputs.candidate_theses
                else RESEARCH_THESIS_UPDATE_PROMPT
                if prior is not None
                else RESEARCH_THESIS_PROMPT
            )
            binding: dict[str, object] = {
                "schema_version": "market-impact.research-thesis-binding.v1",
                "harness_authority_id": self.store.harness_authority_id,
                "run_id": run_id,
                "inputs": inputs.identity_dict(),
                "prior_thesis": prior,
                "selected_inputs_artifact_hash": selected_hash,
                "profile": provider.profile.to_dict(),
                "runtime": provider.runtime_identity,
                "prompt": role_prompt,
                "max_output_tokens": output_limit,
                "budget_owner": {
                    "journal_path": str(
                        self.journal.path
                        if provider.budget is None
                        else provider.budget.journal.path
                    ),
                    "run_id": run_id if provider.budget is None else provider.budget.owner_run_id,
                    "binding": None if provider.budget is None else provider.budget.binding,
                },
            }
            binding_hash = self.store.artifacts.put_json(binding).content_hash
            try:
                previous = self.journal.get_run(run_id)
            except KeyError:
                previous = None
            if previous is not None:
                if previous.config_hash != binding_hash:
                    raise PermissionError(
                        "research thesis Run identity already belongs to different frozen inputs"
                    )
                if previous.status.terminal:
                    self._record_usage(run_id)
                    return self.replay(run_id)
                terminal_event = self.journal.event(f"{run_id}.research-thesis.terminal")
                if terminal_event is None:
                    raise PermissionError(
                        "interrupted research thesis requires reconciliation, not regeneration"
                    )
                terminal_hash = _string(terminal_event.payload, "terminal_hash")
                status = RunStatus(_string(terminal_event.payload, "run_status"))
                self.store.artifacts.read_json(terminal_hash)
                self.journal.finish(
                    run_id=run_id,
                    status=status,
                    finished_at=terminal_event.observed_at,
                    terminal_artifact_id=terminal_hash,
                )
                self._record_usage(run_id)
                return self.replay(run_id)
            self.journal.start_run(run_id=run_id, config_hash=binding_hash, created_at=self.clock())
            self._events.append(
                run_id=run_id,
                event_id=f"{run_id}.research-thesis.frozen",
                event_type="research.thesis.frozen",
                observed_at=self.clock(),
                payload={
                    "binding_hash": binding_hash,
                    "selected_inputs_artifact_hash": selected_hash,
                },
            )
            role_journal = cast(PiRoleJournal, PiRoleJournal.authoritative(self.store))
            role_journal.bind(run_id=run_id, writer=self._events)
            cancellation: asyncio.CancelledError | None = None
            try:
                turn = await execute_pi_once(
                    provider,
                    context=PiInvocationContext(
                        run_id=run_id,
                        ordinal=1,
                        journal=role_journal,
                        artifacts=self.store.artifacts,
                        clock=self.clock,
                    ),
                    messages=(
                        {"role": "system", "content": role_prompt},
                        {
                            "role": "user",
                            "content": json.dumps(selected, ensure_ascii=False, sort_keys=True),
                        },
                    ),
                    max_output_tokens=output_limit,
                    timeout_seconds=provider.profile.budget.max_wall_seconds,
                    attempt_observer=lambda event: self._observe_attempt(run_id, event),
                )
                parsed = load_model_json(_string(turn.assistant_message, "content"))
                pack = inputs.repository.evidence_pack
                thesis = parse_research_thesis(
                    parsed.value,
                    root_event_id=pack.event_id,
                    thesis_epoch=inputs.thesis_epoch,
                    as_of=pack.as_of,
                    evidence_ids=frozenset(item.evidence_id for item in pack.evidence),
                    allowed_horizons=inputs.allowed_horizons,
                )
                reselected = await inputs.selected_inputs()
                if prior is not None:
                    _, reopened_prior = reopen_completed_research_thesis(
                        journal=self.journal,
                        artifact_store=self.store.artifacts,
                        run_id=prior_thesis_run_id or "",
                    )
                    reselected["prior_thesis"] = (
                        _relative_temporal_view(reopened_prior, pack.as_of.date())
                        if inputs.date_presentation is DatePresentation.RELATIVE_OFFSET
                        else reopened_prior
                    )
                if reselected != selected:
                    raise PermissionError("research thesis inputs changed during analysis")
                terminal: dict[str, object] = {
                    "schema_version": "market-impact.research-thesis-terminal.v1",
                    "run_id": run_id,
                    "status": "completed",
                    "binding_hash": binding_hash,
                    "thesis": thesis.to_dict(),
                    "thesis_artifact_hash": self.store.artifacts.put_json(
                        thesis.to_dict()
                    ).content_hash,
                    "raw_response_hash": self.store.artifacts.put_json(
                        turn.raw_response
                    ).content_hash,
                    "parsed_thesis": parsed.value,
                    "parse_evidence": parsed.evidence.to_dict(),
                    "text_normalizations": list(research_thesis_text_normalizations(parsed.value)),
                    "usage": turn.usage.to_dict(),
                    "completed_at": _timestamp(self.clock()),
                }
                status = RunStatus.COMPLETED
            except (Exception, asyncio.CancelledError) as error:
                if isinstance(error, asyncio.CancelledError):
                    cancellation = error
                terminal = {
                    "schema_version": "market-impact.research-thesis-terminal.v1",
                    "run_id": run_id,
                    "status": "incomplete",
                    "binding_hash": binding_hash,
                    "reason": type(error).__name__,
                    "completed_at": _timestamp(self.clock()),
                }
                status = RunStatus.CANCELLED if cancellation is not None else RunStatus.FAILED

            artifact = self.store.artifacts.put_json(terminal)
            self._events.append(
                run_id=run_id,
                event_id=f"{run_id}.research-thesis.terminal",
                event_type=(
                    "research.thesis.validated"
                    if status is RunStatus.COMPLETED
                    else "research.thesis.incomplete"
                ),
                observed_at=self.clock(),
                payload={
                    "terminal_hash": artifact.content_hash,
                    "binding_hash": binding_hash,
                    "run_status": status.value,
                },
            )
            self.journal.finish(
                run_id=run_id,
                status=status,
                finished_at=self.clock(),
                terminal_artifact_id=artifact.content_hash,
            )
            self._record_usage(run_id)
            if cancellation is not None:
                raise cancellation
            return self.replay(run_id)

    def replay(self, run_id: str) -> dict[str, object]:
        record = self.journal.get_run(run_id)
        events = self.journal.events(run_id)
        if not record.status.terminal or record.terminal_artifact_id is None:
            raise PermissionError("research thesis has no terminal result")
        terminal = _object(self.store.artifacts.read_json(record.terminal_artifact_id))
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        event = next(
            (item for item in events if item.event_id == f"{run_id}.research-thesis.terminal"),
            None,
        )
        if (
            event is None
            or event.payload.get("terminal_hash") != record.terminal_artifact_id
            or event.payload.get("binding_hash") != record.config_hash
            or event.payload.get("run_status") != record.status.value
            or terminal.get("run_id") != run_id
            or terminal.get("binding_hash") != record.config_hash
        ):
            raise PermissionError("research thesis terminal differs from its signed Run")
        if terminal.get("status") == "completed":
            raw = _object(self.store.artifacts.read_json(_string(terminal, "raw_response_hash")))
            profile = _object(binding["profile"])
            turn = native_turn(raw, _string(profile, "model"))
            parsed = load_model_json(_string(turn.assistant_message, "content"))
            inputs = _object(binding["inputs"])
            selected = _object(
                self.store.artifacts.read_json(_string(binding, "selected_inputs_artifact_hash"))
            )
            references = cast(list[object], selected["evidence"])
            evidence_ids = frozenset(
                _string(_object(_object(item)["reference"]), "evidence_id") for item in references
            )
            thesis = parse_research_thesis(
                parsed.value,
                root_event_id=_string(inputs, "root_event_id"),
                thesis_epoch=_string(inputs, "thesis_epoch"),
                as_of=_datetime(_string(inputs, "as_of")),
                evidence_ids=evidence_ids,
                allowed_horizons=frozenset(
                    _integers(inputs["allowed_horizons"], "allowed_horizons")
                ),
            )
            if (
                thesis.to_dict() != terminal.get("thesis")
                or self.store.artifacts.read_json(_string(terminal, "thesis_artifact_hash"))
                != thesis.to_dict()
                or parsed.value != terminal.get("parsed_thesis")
                or parsed.evidence.to_dict() != terminal.get("parse_evidence")
                or list(research_thesis_text_normalizations(parsed.value))
                != terminal.get("text_normalizations", [])
                or turn.usage.to_dict() != terminal.get("usage")
            ):
                raise PermissionError("research thesis differs from its native response")
        elif terminal.get("status") != "incomplete":
            raise PermissionError("research thesis has an unknown terminal status")
        return terminal

    def _observe_attempt(self, run_id: str, event: ProviderAttemptEvent) -> None:
        self._events.append(
            run_id=run_id,
            event_id=(
                f"{run_id}.research-thesis.attempt.{event.physical_attempt}.{event.phase.value}"
            ),
            event_type="research.thesis.model.attempt",
            observed_at=self.clock(),
            payload={
                "request_id": event.request_id,
                "attempt": event.physical_attempt,
                "phase": event.phase.value,
                "latency_ms": event.elapsed_latency_ms,
                "failure": None if event.failure is None else event.failure.safe_fields(),
            },
        )

    def _record_usage(self, run_id: str) -> None:
        record = self.journal.get_run(run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        profile = _object(binding["profile"])
        owner = _object(binding["budget_owner"])
        budget_path = _string(owner, "journal_path")
        budget_journal = (
            self.journal if budget_path == str(self.journal.path) else RunJournal(Path(budget_path))
        )
        reserved: dict[str, int] = {}
        settled: dict[str, int] = {}
        for event in budget_journal.events(_string(owner, "run_id")):
            key = event.payload.get("request_key")
            if not isinstance(key, str) or not key.startswith(f"{run_id}.pi-invocation."):
                continue
            if event.event_type == "pi.budget.reserved":
                reserved[key] = cast(int, event.payload["reserved_microusd"])
            elif event.event_type == "pi.budget.settled":
                settled[key] = cast(int, event.payload["estimated_cost_microusd"])
        turns = input_tokens = output_tokens = attempts = 0
        latency = 0.0
        for event in self.journal.events(run_id):
            if event.event_type == "research.thesis.model.attempt":
                attempts += int(event.payload["phase"] == "dispatched")
                if event.payload["phase"] != "dispatched":
                    latency += float(cast(float, event.payload["latency_ms"]))
            elif event.event_type == "pi.role.response.completed":
                raw = _object(
                    self.store.artifacts.read_json(_string(event.payload, "artifact_hash"))
                )
                turn = native_turn(raw, _string(profile, "model"))
                turns += 1
                input_tokens += turn.usage.input_tokens
                output_tokens += turn.usage.output_tokens
        metrics = RunMetrics(
            turns=turns,
            tool_calls=0,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            result_bytes=0,
            latency_ms=latency,
            provider_attempts=attempts,
            estimated_cost_microusd=sum(settled.values())
            + sum(value for key, value in reserved.items() if key not in settled),
        )
        self.usage_ledger.append(
            UsageRecord(
                experiment_id=self.experiment_id,
                arm_id=self.arm_id,
                run_id=run_id,
                recorded_at=record.updated_at,
                status=record.status,
                provider_profile_id=_string(profile, "profile_id"),
                provider_profile_hash=canonical_hash(profile),
                execution_binding_hash=record.config_hash,
                terminal_artifact_hash=record.terminal_artifact_id,
                run_journal_hash=self.journal.journal_hash(run_id),
                metrics=metrics,
            )
        )


def reopen_completed_research_thesis(
    *,
    journal: RunJournal,
    artifact_store: ArtifactStore,
    run_id: str,
) -> tuple[ResearchThesisV1, dict[str, object]]:
    """Reopen a completed same-root terminal for a downstream authority."""

    record = journal.get_run(run_id)
    journal.events(run_id)
    if record.status is not RunStatus.COMPLETED or record.terminal_artifact_id is None:
        raise PermissionError("research thesis is not completed")
    terminal = _object(artifact_store.read_json(record.terminal_artifact_id))
    binding = _object(artifact_store.read_json(record.config_hash))
    event = journal.event(f"{run_id}.research-thesis.terminal")
    if (
        event is None
        or event.event_type != "research.thesis.validated"
        or event.payload.get("terminal_hash") != record.terminal_artifact_id
        or event.payload.get("binding_hash") != record.config_hash
        or event.payload.get("run_status") != RunStatus.COMPLETED.value
        or terminal.get("status") != "completed"
    ):
        raise PermissionError("research thesis has no signed completed terminal")
    value = _object(terminal["thesis"])
    inputs = _object(binding["inputs"])
    thesis = ResearchThesisV1(
        root_event_id=_string(value, "root_event_id"),
        thesis_epoch=_string(value, "thesis_epoch"),
        as_of=_datetime(_string(value, "as_of")),
        horizon_band=HorizonBand(_string(value, "horizon_band")),
        primary_horizon_sessions=_integer(value, "primary_horizon_sessions"),
        base_case_direction=BaseCaseDirection(_string(value, "base_case_direction")),
        thesis=_string(value, "thesis"),
        priced_in_assessment=_string(value, "priced_in_assessment"),
        transmission=tuple(_strings(value["transmission"], "transmission")),
        counter_scenario=_string(value, "counter_scenario"),
        evidence_refs=tuple(_strings(value["evidence_refs"], "evidence_refs")),
        counterevidence_refs=tuple(_strings(value["counterevidence_refs"], "counterevidence_refs")),
        invalidation_conditions=tuple(
            _strings(value["invalidation_conditions"], "invalidation_conditions")
        ),
        review_after_sessions=_integer(value, "review_after_sessions"),
        typed_unknowns=tuple(_strings(value["typed_unknowns"], "typed_unknowns")),
    )
    if (
        thesis.to_dict() != value
        or thesis.root_event_id != _string(inputs, "root_event_id")
        or thesis.thesis_epoch != _string(inputs, "thesis_epoch")
    ):
        raise PermissionError("research thesis identity differs from its binding")
    return thesis, {
        "run_id": run_id,
        "terminal_hash": record.terminal_artifact_id,
        "binding_hash": record.config_hash,
        "thesis": thesis.to_dict(),
        "journal_hash": journal.journal_hash(run_id),
    }


def theses_semantically_disagree(first: ResearchThesisV1, second: ResearchThesisV1) -> bool:
    """Trigger a Judge only for a materially different conclusion, not wording."""

    if first.root_event_id != second.root_event_id or first.as_of != second.as_of:
        raise ValueError("semantic comparison requires the same frozen event input")
    return (
        first.base_case_direction != second.base_case_direction
        or first.primary_horizon_sessions != second.primary_horizon_sessions
    )


def _relative_temporal_view(value: object, cutoff: date, *, key: str = "") -> object:
    if isinstance(value, dict):
        return {
            item_key: (
                "relative-source://withheld"
                if item_key == "source_ref"
                else _relative_temporal_view(item, cutoff, key=item_key)
            )
            for item_key, item in cast(dict[str, object], value).items()
        }
    if isinstance(value, list):
        return [
            _relative_temporal_view(item, cutoff, key=key) for item in cast(list[object], value)
        ]
    if isinstance(value, str):
        if key.endswith("_id") or key in {
            "code",
            "doi",
            "isin",
            "symbol",
            "ticker",
        }:
            return value
        if "://" in value or key.endswith("_url") or key.endswith("_uri"):
            return "relative-locator://" + canonical_hash(value)
        if (
            key in {"point_in_time_cutoff", "date", "as_of"}
            or key.endswith("_at")
            or key.endswith("_date")
        ):
            try:
                observed = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
            except ValueError:
                try:
                    observed = date.fromisoformat(value)
                except ValueError:
                    observed = None
            if observed is not None:
                offset = (observed - cutoff).days
                return "T0" if offset == 0 else f"T{offset:+d} calendar days"

        def replace_date(match: re.Match[str]) -> str:
            observed = date.fromisoformat(match.group(0))
            offset = (observed - cutoff).days
            return "T0" if offset == 0 else f"T{offset:+d}d"

        rendered = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", replace_date, value)
        month_numbers = {
            name: number
            for number, name in enumerate(
                (
                    "jan",
                    "feb",
                    "mar",
                    "apr",
                    "may",
                    "jun",
                    "jul",
                    "aug",
                    "sep",
                    "oct",
                    "nov",
                    "dec",
                ),
                start=1,
            )
        }

        def replace_text_date(match: re.Match[str]) -> str:
            observed = date(
                int(match.group("year")),
                month_numbers[match.group("month")[:3].lower()],
                int(match.group("day")),
            )
            offset = (observed - cutoff).days
            return "T0" if offset == 0 else f"T{offset:+d}d"

        rendered = re.sub(
            r"\b(?P<day>\d{1,2})\s+(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?),?\s+(?P<year>\d{4})\b",
            replace_text_date,
            rendered,
            flags=re.IGNORECASE,
        )

        def replace_month_first_date(match: re.Match[str]) -> str:
            month = month_numbers[match.group("month")[:3].lower()]
            day = int(match.group("day"))
            explicit_year = match.group("year")
            if explicit_year is not None:
                observed = date(int(explicit_year), month, day)
            else:
                candidates = tuple(
                    date(year, month, day)
                    for year in (cutoff.year - 1, cutoff.year, cutoff.year + 1)
                )
                observed = min(candidates, key=lambda item: abs((item - cutoff).days))
            offset = (observed - cutoff).days
            return "T0" if offset == 0 else f"T{offset:+d}d"

        rendered = re.sub(
            r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+"
            r"(?P<day>\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(?P<year>\d{4}))?\b",
            replace_month_first_date,
            rendered,
            flags=re.IGNORECASE,
        )

        def replace_chinese_date(match: re.Match[str]) -> str:
            observed = date(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
            )
            offset = (observed - cutoff).days
            return "T0" if offset == 0 else f"T{offset:+d}d"

        rendered = re.sub(
            r"(?<![\w./-])(?P<year>(?:19|20)\d{2})年"
            r"(?P<month>\d{1,2})月(?P<day>\d{1,2})日",
            replace_chinese_date,
            rendered,
        )

        def replace_year(match: re.Match[str]) -> str:
            offset = int(match.group(0)) - cutoff.year
            return "T0y" if offset == 0 else f"T{offset:+d}y"

        return re.sub(
            r"(?<![\w./-])(?:19|20)\d{2}(?=(?:年|[\s,.;:)\]]|$))",
            replace_year,
            rendered,
        )
    return value


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("research thesis artifact must be a JSON object")
    return cast(dict[str, object], value)


def _string(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip() or item != item.strip():
        raise ValueError(f"research thesis {key} must be nonempty trimmed text")
    return item


def _integer(value: dict[str, object], key: str) -> int:
    item = value.get(key)
    if type(item) is not int:
        raise ValueError(f"research thesis {key} must be an integer")
    return item


def _integers(value: object, name: str) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise ValueError(f"research thesis {name} must be integers")
    items = cast(list[object], value)
    if any(type(item) is not int for item in items):
        raise ValueError(f"research thesis {name} must be integers")
    return tuple(cast(list[int], items))


def _strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in cast(list[object], value)
    ):
        raise ValueError(f"research thesis {name} must be strings")
    return tuple(cast(list[str], value))


def _datetime(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("research thesis timestamp must be timezone-aware")
    return result


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("research thesis timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
