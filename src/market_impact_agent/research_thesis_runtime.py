"""Authoritative dynamic-horizon research roles executed by the upstream pi loop.

The Harness selects and freezes every input before dispatch.  The model authors
only the analytical thesis. Harness-injected read-only tools use the same upstream
pi loop. Newly acquired
data requires a new frozen Snapshot and continuation Run; tools cannot enlarge
the evidence IDs accepted by the current thesis.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import EvidenceReference, canonical_hash
from market_impact_agent.agent_engine import (
    RunMetrics,
    _PrivilegedEventSink,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.agent_runtime import ToolDescriptor
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.decision_thesis import (
    BaseCaseDirection,
    HorizonBand,
    ResearchThesisV1,
    ResearchThesisV2,
    parse_research_thesis,
    parse_research_thesis_v2,
    research_thesis_text_normalizations,
)
from market_impact_agent.dynamic_effectiveness import DatePresentation
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.method_catalog import FrozenMethodCatalog
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

if TYPE_CHECKING:
    from market_impact_agent.research_thesis_watch import ResearchThesisWatchDelegation

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


RESEARCH_THESIS_V2_PROMPT = """Act as a senior public-equity analyst using only frozen
point-in-time inputs. State a defensible direction up/down/rangebound/unknown and
separately assess event_support supported/uncertain/unsupported. Context or a recent
retrieval is not proof of a new event. Empty transmission and evidence_refs are valid
when event evidence is absent; explain the gaps in typed_unknowns. Unknown is an
analytical conclusion and conveys no portfolio action.
Distinguish source-stated facts, your hypothesized transmission mechanisms, and
unknowns. A plausible mechanism is not evidence that a policy action occurred.
Treat publication, policy effective date, modeled availability, and actual receipt
as distinct times; absent source dates remain unknown. Availability alone does not
establish novelty or what investors expected. Compare prices on one consistent
basis, using cutoff-adjusted closes for returns when supplied; never mix raw and
adjusted levels. Label priced-in assessments as hypotheses unless evidenced.
Return exactly one JSON object: primary_horizon_sessions (an allowed value),
base_case_direction, event_support, thesis (text), expectations (text),
priced_in_assessment (text),
transmission (string array), counter_scenario (text), evidence_refs (string array),
counterevidence_refs (string array), invalidation_conditions (nonempty string array),
review_after_sessions (integer), typed_unknowns (string array, not an object),
revision_new_facts (string array), revision_old_assumptions (string array),
revision_conclusion (text; explain retained or changed conclusion on review), and
candidate_comparisons (zero to five objects: candidate_ref, transmission, support_refs,
counter_refs, comparison_reason, gaps). Evidence refs may use the exact descriptive
choices or exact frozen IDs. Candidate comparisons must cite only the candidate's
frozen candidate_proofs, and distinguish event linkage from mere investability.
Each candidate_ref must be an exact key of candidate_proofs. Evidence about an
additional benchmark does not add it to that candidate list; discuss benchmark
and cash comparisons in thesis or expectations instead. Narrative text fields
must be strings, not arrays or objects.
When prior_thesis is supplied, revision_new_facts and revision_old_assumptions must
be nonempty explanations; explicitly say if no new facts changed the old assumption.
The Harness binds target and identity. Do not author IDs, timestamps, quantity or orders.
"""


@dataclass(frozen=True, slots=True)
class ResearchThesisRunInputs:
    repository: FrozenResearchRepository
    target_id: str
    thesis_epoch: str
    allowed_horizons: frozenset[int]
    date_presentation: DatePresentation = DatePresentation.TRUE_DATE
    candidate_theses: tuple[ResearchThesisV1, ...] = ()
    research_question: str | None = None
    watch_delegation: ResearchThesisWatchDelegation | None = None
    schema_version: str = "market-impact.research-thesis-inputs.v2"
    candidate_proofs: Mapping[str, tuple[str, ...]] = field(
        default_factory=lambda: dict[str, tuple[str, ...]]()
    )

    def __post_init__(self) -> None:
        if self.schema_version not in {
            "market-impact.research-thesis-inputs.v1",
            "market-impact.research-thesis-inputs.v2",
        }:
            raise ValueError("unsupported research input version")
        if self.candidate_proofs and self.schema_version.endswith(".v1"):
            raise ValueError("candidate proof comparison requires V2 inputs")
        if len(self.candidate_proofs) > 5:
            raise ValueError("candidate proofs exceed five")
        pack = self.repository.evidence_pack
        frozen_ids = {item.evidence_id for item in pack.evidence}
        for candidate, refs in self.candidate_proofs.items():
            if candidate not in pack.allowed_targets or not refs or not set(refs) <= frozen_ids:
                raise ValueError("candidate proofs must bind offered targets and frozen evidence")

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
        if self.watch_delegation is not None:
            from market_impact_agent.research_thesis_watch import ResearchThesisWatchDelegation

            if (
                type(self.watch_delegation) is not ResearchThesisWatchDelegation
                or self.watch_delegation.subject.canonical_id
                != self.repository.evidence_pack.event_id
            ):
                raise PermissionError(
                    "Watch delegation must bind the exact Harness research root event"
                )
        if self.candidate_theses:
            pack = self.repository.evidence_pack
            evidence_ids = {item.evidence_id for item in pack.evidence}
            for thesis in self.candidate_theses:
                if (
                    (isinstance(thesis, ResearchThesisV2) and thesis.target_id != self.target_id)
                    or thesis.root_event_id != pack.event_id
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
        if self.schema_version.endswith(".v2"):
            value["schema_version"] = self.schema_version
            choices = {
                f"{index + 1}. {item.summary}": item.evidence_id
                for index, item in enumerate(pack.evidence)
            }
            labels = {ref: label for label, ref in choices.items()}
            value["evidence_choices"] = choices
            value["candidate_proofs"] = {
                candidate: [labels[ref] for ref in refs]
                for candidate, refs in self.candidate_proofs.items()
            }
            value["evidence_metadata"] = [
                research_evidence_metadata(item, _object(loaded)["document"], pack.as_of)
                for item, loaded in zip(pack.evidence, evidence, strict=True)
            ]
        if self.candidate_theses:
            value["candidate_analyses"] = [thesis.to_dict() for thesis in self.candidate_theses]
        if self.date_presentation is DatePresentation.RELATIVE_OFFSET:
            return cast(dict[str, object], _relative_temporal_view(value, pack.as_of.date()))
        return value

    def identity_dict(self) -> dict[str, object]:
        pack = self.repository.evidence_pack
        return {
            **(
                {
                    "schema_version": self.schema_version,
                    "candidate_proofs": {
                        key: list(refs) for key, refs in self.candidate_proofs.items()
                    },
                }
                if self.schema_version.endswith(".v2")
                else {}
            ),
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
            **(
                {"watch_delegation": self.watch_delegation.to_dict()}
                if self.watch_delegation is not None
                else {}
            ),
        }


def research_evidence_metadata(
    reference: EvidenceReference,
    document: object,
    cutoff: datetime,
) -> dict[str, object]:
    """Describe source content and time without deriving facts from receipt or IDs."""
    payload = cast(dict[str, object], document) if isinstance(document, dict) else {}
    raw_api = payload.get("source_api")
    source_api = raw_api if isinstance(raw_api, str) else None
    fields = payload.get("fields", [])
    names = (
        [item for item in cast(list[object], fields) if isinstance(item, str)]
        if isinstance(fields, list)
        else []
    )
    field_names = set(names)
    rows: list[dict[str, object]] = []
    has_collection = False
    for key in ("articles", "records", "rows", "observations"):
        raw = payload.get(key)
        if not isinstance(raw, list):
            continue
        has_collection = True
        for item in cast(list[object], raw):
            if isinstance(item, dict):
                row = dict(cast(dict[str, object], item))
                for nested_key in ("times", "modeled_fact"):
                    nested = row.get(nested_key)
                    if isinstance(nested, dict):
                        row.update(cast(dict[str, object], nested))
                rows.append(row)
            elif isinstance(item, list) and isinstance(fields, list):
                values = cast(list[object], item)
                if len(values) == len(names):
                    rows.append(dict(zip(names, values, strict=True)))
    if not has_collection:
        rows = [payload] if payload else []

    def timestamp(value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return at if at.tzinfo is not None else None

    def source_label(value: object) -> str | None:
        return value if isinstance(value, str) and value.strip() else None

    document_retrieved_at = timestamp(payload.get("retrieved_at"))

    publications: list[dict[str, object]] = []
    published_times: list[datetime] = []
    event_times: list[datetime] = []
    for row in rows:
        availability_basis = source_label(
            row.get("availability_basis", payload.get("availability_basis"))
        )
        source_published = timestamp(row.get("published_at"))
        modeled_published = (
            source_published if availability_basis == "modeled_source_date_eod_plus_5m" else None
        )
        published = None if modeled_published is not None else source_published
        occurred = (
            None
            if row.get("occurrence_basis")
            in {"aggregator_snapshot", "retrieval_observed", "retrospective_series"}
            else timestamp(row.get("occurred_at"))
        )
        effective = timestamp(row.get("effective_at"))
        retrieved = timestamp(row.get("retrieved_at")) or document_retrieved_at
        if source_published is None and occurred is None:
            continue
        record: dict[str, object] = {}
        identity = row.get("evidence_record_id")
        if isinstance(identity, str):
            record["evidence_record_id"] = identity
        for key, at in (
            ("published_at", published),
            ("modeled_published_at", modeled_published),
            ("occurred_at", occurred),
            ("effective_at", effective),
            ("available_at", timestamp(row.get("available_at"))),
            ("retrieved_at", retrieved),
        ):
            record[key] = _timestamp(at) if at is not None else None
        record["availability_basis"] = availability_basis
        record["historical_authentication"] = source_label(
            row.get("historical_authentication", payload.get("historical_authentication"))
        )
        record["publication_age_seconds"] = (
            (cutoff - published).total_seconds() if published else None
        )
        record["event_age_seconds"] = (cutoff - occurred).total_seconds() if occurred else None
        publications.append(record)
        if published is not None:
            published_times.append(published)
        if occurred is not None:
            event_times.append(occurred)
    session_dates = sorted({str(row["trade_date"]) for row in rows if row.get("trade_date")})
    price_shape = bool(session_dates) and (
        bool(field_names & {"raw_close", "cutoff_adjusted_close", "close", "open"})
        or any(set(row) & {"raw_close", "cutoff_adjusted_close", "close", "open"} for row in rows)
    )
    category = (
        "price_session_history"
        if source_api in {"daily", "fund_daily", "index_daily"} or price_shape
        else "security_identity"
        if source_api in {"stock_basic", "fund_basic", "etf_basic"}
        or any("ts_code" in row and set(row) & {"list_date", "exchange", "market"} for row in rows)
        else "dated_publication"
        if publications
        else "background"
    )
    return {
        "evidence_id": reference.evidence_id,
        "source_ref": reference.source_ref,
        "source_api": source_api,
        "source_tier": reference.source_tier.value,
        "category": category,
        "coverage": {
            "record_count": len(rows),
            "session_start": session_dates[0] if session_dates else None,
            "session_end": session_dates[-1] if session_dates else None,
            "status": payload.get("status"),
            "gaps": payload.get("gaps", []),
        },
        "time_semantics": {
            "available_at": (
                "frozen evidence-availability gate; it may be modeled PIT or actual receipt "
                "only as the source-provided availability_basis states, and is not a "
                "publication, occurrence, or effective time"
            ),
            "published_at": "source-stated publication time; null when absent from the source",
            "modeled_published_at": (
                "conservative modeled publication boundary for a date-only source; it is not a "
                "source-stated publication time"
            ),
            "occurred_at": (
                "source-stated occurrence time; null when absent or when the source marks it as "
                "retrieval-observed"
            ),
            "effective_at": (
                "source-stated policy or economic effective time; null when absent and never "
                "derived from publication or availability"
            ),
            "retrieved_at": (
                "recorded source capture time; it establishes actual receipt only when the "
                "source-provided availability_basis says actual receipt"
            ),
            "historical_authentication": (
                "source-provided historical lane label; a modeled-PIT label does not establish "
                "contemporaneous receipt"
            ),
        },
        "available_at": _timestamp(reference.available_at),
        "retrieved_at": _timestamp(document_retrieved_at)
        if document_retrieved_at is not None
        else None,
        "availability_age_seconds": (cutoff - reference.available_at).total_seconds(),
        "publication_age_seconds": (cutoff - max(published_times)).total_seconds()
        if published_times
        else None,
        "event_age_seconds": (cutoff - max(event_times)).total_seconds() if event_times else None,
        "publication_records": publications,
    }


class ResearchThesisAuthority:
    """Produce and replay one signed ResearchThesis terminal."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        experiment_id: str,
        arm_id: str,
        account_scope: str | None = None,
        prior_adoption_validator: Callable[[str, str, str, datetime], dict[str, object]]
        | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        method_catalog: FrozenMethodCatalog | None = None,
    ) -> None:
        for value, name in ((experiment_id, "experiment_id"), (arm_id, "arm_id")):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be nonempty trimmed text")
        self.store = store
        self.journal = RunJournal.authoritative(store)
        self.experiment_id = experiment_id
        self.arm_id = arm_id
        self.account_scope = account_scope
        self.prior_adoption_validator = prior_adoption_validator
        self.clock = clock
        self.method_catalog = method_catalog
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
        prior_adoption_ref: str | None = None,
        readonly_tools: tuple[ToolDescriptor, ...] = (),
    ) -> dict[str, object]:
        method_catalog = self.method_catalog
        output_limit = (
            provider.profile.reserved_output_tokens
            if max_output_tokens is None
            else max_output_tokens
        )
        if not 16 <= output_limit <= provider.profile.reserved_output_tokens:
            raise ValueError("research thesis output limit is outside the accepted Profile")
        if inputs.watch_delegation is not None:
            if provider.budget is None or self.account_scope is None:
                raise PermissionError(
                    "Watch delegation requires an account and shared parent budget"
                )
            inputs.watch_delegation.verify_episode(self.store, provider.budget)
            from market_impact_agent.research_thesis_watch import (
                RESEARCH_WATCH_TOOL,
                research_thesis_watch_tool,
            )

            watch_tool = research_thesis_watch_tool(inputs, run_id)
            offered = tuple(tool for tool in readonly_tools if tool.name == RESEARCH_WATCH_TOOL)
            if offered and (
                len(offered) != 1 or offered[0].manifest_hash != watch_tool.manifest_hash
            ):
                raise PermissionError("research Watch tool differs from the frozen Harness offer")
            if not offered:
                readonly_tools += (watch_tool,)
        if inputs.repository.evidence_pack.as_of > self.clock():
            raise PermissionError("research thesis evidence is after the authority clock")
        if inputs.date_presentation is DatePresentation.RELATIVE_OFFSET:
            readonly_tools = tuple(
                _relative_tool(tool, inputs.repository.evidence_pack.as_of.date())
                for tool in readonly_tools
            )
        claim = self.journal.try_claim_run(run_id)
        if claim is None:
            raise RuntimeError("research thesis Run already has an owner")
        with claim:
            selected = await inputs.selected_inputs()
            prior: dict[str, object] | None = None
            adopted_prior: dict[str, object] | None = None
            if prior_adoption_ref is not None:
                if prior_thesis_run_id is None or self.prior_adoption_validator is None:
                    raise PermissionError("adopted prior requires registered receipt authority")
                adopted_prior = self.prior_adoption_validator(
                    prior_adoption_ref,
                    prior_thesis_run_id,
                    inputs.target_id,
                    inputs.repository.evidence_pack.as_of,
                )
            if prior_thesis_run_id is not None:
                prior_thesis, prior = reopen_completed_research_thesis(
                    journal=self.journal,
                    artifact_store=self.store.artifacts,
                    run_id=prior_thesis_run_id,
                )
                prior_binding = _object(
                    self.store.artifacts.read_json(
                        self.journal.get_run(prior_thesis_run_id).config_hash
                    )
                )
                if (
                    prior_thesis.as_of >= inputs.repository.evidence_pack.as_of
                    or prior_binding.get("experiment_id") != self.experiment_id
                    or (
                        adopted_prior is None
                        and (
                            prior_binding.get("arm_id") != self.arm_id
                            or prior_binding.get("account_scope") != self.account_scope
                        )
                    )
                    or _object(prior_binding["inputs"]).get("target_id") != inputs.target_id
                    or prior_binding.get("method_catalog")
                    != (None if method_catalog is None else method_catalog.identity())
                ):
                    raise ValueError("prior thesis is not an earlier compatible review state")
                selected["prior_thesis"] = (
                    _relative_temporal_view(prior, inputs.repository.evidence_pack.as_of.date())
                    if inputs.date_presentation is DatePresentation.RELATIVE_OFFSET
                    else prior
                )
            selected_hash = self.store.artifacts.put_json(selected).content_hash
            delivery_policy: str | None = None
            if prior is not None:
                try:
                    old_binding = _object(
                        self.store.artifacts.read_json(self.journal.get_run(run_id).config_hash)
                    )
                    delivery_policy = cast(str | None, old_binding.get("recall_delivery_policy"))
                except KeyError:
                    delivery_policy = "injected-prior-reference-v1"
                if delivery_policy is not None:
                    if delivery_policy != "injected-prior-reference-v1":
                        raise PermissionError("unrecognized frozen Recall delivery policy")
                    readonly_tools = tuple(
                        _injected_prior_reference(tool, prior, selected_hash)
                        for tool in readonly_tools
                    )
            role_prompt = (
                RESEARCH_THESIS_JUDGE_PROMPT
                if inputs.candidate_theses
                else RESEARCH_THESIS_UPDATE_PROMPT
                if prior is not None
                else RESEARCH_THESIS_PROMPT
            )
            if inputs.schema_version.endswith(".v2"):
                role_prompt = RESEARCH_THESIS_V2_PROMPT + (
                    "\nCompare both candidate analyses from original evidence; do not count votes."
                    if inputs.candidate_theses
                    else "\nReview the prior signed opinion using new facts and old assumptions."
                    if prior is not None
                    else ""
                )
            binding: dict[str, object] = {
                "schema_version": "market-impact.research-thesis-binding.v1",
                "harness_authority_id": self.store.harness_authority_id,
                "run_id": run_id,
                "inputs": inputs.identity_dict(),
                "prior_thesis": prior,
                **(
                    {"recall_delivery_policy": delivery_policy}
                    if delivery_policy is not None
                    else {}
                ),
                **({"prior_adoption": adopted_prior} if adopted_prior is not None else {}),
                "selected_inputs_artifact_hash": selected_hash,
                "profile": provider.profile.to_dict(),
                "runtime": provider.runtime_identity,
                "experiment_id": self.experiment_id,
                "arm_id": self.arm_id,
                "account_scope": self.account_scope,
                "readonly_tool_hashes": sorted(t.manifest_hash for t in readonly_tools),
                **(
                    {"method_catalog": method_catalog.identity()}
                    if method_catalog is not None
                    else {}
                ),
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
                    readonly_tools=readonly_tools,
                    method_catalog=method_catalog,
                    expect_json=bool(method_catalog or readonly_tools),
                    initial_history=""
                    if prior is None
                    else json.dumps(
                        {"prior_thesis": selected["prior_thesis"]},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                parsed = load_model_json(_string(turn.assistant_message, "content"))
                pack = inputs.repository.evidence_pack
                thesis = _parse_versioned_thesis(
                    parsed.value,
                    selected=selected,
                    inputs=inputs.identity_dict(),
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
            thesis = _parse_versioned_thesis(
                parsed.value,
                selected=selected,
                inputs=inputs,
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
                f"{run_id}.research-thesis.attempt.{canonical_hash(event.request_id)}.{event.physical_attempt}.{event.phase.value}"
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
        tool_calls = result_bytes = 0
        turns = input_tokens = output_tokens = attempts = 0
        latency = 0.0
        for event in self.journal.events(run_id):
            if event.event_type == "pi.role.tool.completed":
                tool_calls += 1
                result_bytes += len(
                    json.dumps(
                        self.store.artifacts.read_json(_string(event.payload, "artifact_hash"))
                    ).encode("utf-8")
                )
            elif event.event_type == "research.thesis.model.attempt":
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
            tool_calls=tool_calls,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            result_bytes=result_bytes,
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
    if value.get("schema_version") == "market-impact.research-thesis.v2":
        selected = _object(
            artifact_store.read_json(_string(binding, "selected_inputs_artifact_hash"))
        )
        model_fields = {
            key: item
            for key, item in value.items()
            if key
            not in {
                "schema_version",
                "thesis_id",
                "root_event_id",
                "thesis_epoch",
                "as_of",
                "target_id",
            }
        }
        thesis = _parse_versioned_thesis(
            model_fields,
            selected=selected,
            inputs=inputs,
            root_event_id=_string(value, "root_event_id"),
            thesis_epoch=_string(value, "thesis_epoch"),
            as_of=_datetime(_string(value, "as_of")),
            evidence_ids=frozenset(
                _string(_object(_object(item)["reference"]), "evidence_id")
                for item in cast(list[object], selected["evidence"])
            ),
            allowed_horizons=frozenset(_integers(inputs["allowed_horizons"], "allowed_horizons")),
        )
    else:
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
            counterevidence_refs=tuple(
                _strings(value["counterevidence_refs"], "counterevidence_refs")
            ),
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


def _parse_versioned_thesis(
    value: object,
    *,
    selected: dict[str, object],
    inputs: dict[str, object],
    root_event_id: str,
    thesis_epoch: str,
    as_of: datetime,
    evidence_ids: frozenset[str],
    allowed_horizons: frozenset[int],
) -> ResearchThesisV1:
    if inputs.get("schema_version") == "market-impact.research-thesis-inputs.v2":
        proofs = _object(inputs.get("candidate_proofs", {}))
        choices = _object(selected.get("evidence_choices", {}))
        return parse_research_thesis_v2(
            value,
            root_event_id=root_event_id,
            thesis_epoch=thesis_epoch,
            as_of=as_of,
            target_id=_string(inputs, "target_id"),
            evidence_ids=evidence_ids,
            allowed_horizons=allowed_horizons,
            candidate_proofs={
                key: tuple(_strings(refs, "candidate proofs")) for key, refs in proofs.items()
            },
            evidence_choices={key: _string(choices, key) for key in choices},
            requires_revision=selected.get("prior_thesis") is not None,
        )
    if inputs.get("schema_version") not in {None, "market-impact.research-thesis-inputs.v1"}:
        raise ValueError("unsupported frozen research input version")
    return parse_research_thesis(
        value,
        root_event_id=root_event_id,
        thesis_epoch=thesis_epoch,
        as_of=as_of,
        evidence_ids=evidence_ids,
        allowed_horizons=allowed_horizons,
    )


def theses_semantically_disagree(first: ResearchThesisV1, second: ResearchThesisV1) -> bool:
    """Trigger a Judge only for a materially different conclusion, not wording."""

    if first.root_event_id != second.root_event_id or first.as_of != second.as_of:
        raise ValueError("semantic comparison requires the same frozen event input")
    return (
        first.base_case_direction != second.base_case_direction
        or first.primary_horizon_sessions != second.primary_horizon_sessions
    )


def _injected_prior_reference(
    tool: ToolDescriptor, prior: dict[str, object], selected_hash: str
) -> ToolDescriptor:
    """Reopen normally, then omit a verified opinion already present in the input.

    The underlying handler still enforces scope, signature and PIT. The compact
    result is charged normally by pi; no source-hash exemption hides delivered text.
    """
    if tool.name not in {"read_current_thesis", "read_prior_decisions"}:
        return tool
    source_hash = canonical_hash(prior["thesis"])
    reference = {
        "status": "already_supplied",
        "source_run_id": prior["run_id"],
        "source_artifact_hash": source_hash,
        "selected_inputs_artifact_hash": selected_hash,
        "json_pointer": "/prior_thesis/thesis",
        "authority": "prior_signed_opinion_not_source_fact",
        "evidence": False,
    }

    async def read(arguments: dict[str, object]) -> object:
        value = await tool.handler(arguments)
        if not isinstance(value, dict):
            return value
        payload = cast(dict[str, object], value)
        if tool.name == "read_current_thesis":
            current = payload.get("current_thesis")
            if isinstance(current, dict):
                current = cast(dict[str, object], current)
                if (
                    current.get("source_run_id") == prior["run_id"]
                    and current.get("source_artifact_hash") == source_hash
                ):
                    return {
                        "current_thesis": reference,
                        "evidence": False,
                        "authority": "prior_signed_opinion_not_source_fact",
                    }
        elif isinstance(payload.get("decisions"), list):
            return {
                **payload,
                "decisions": [
                    {"recall_id": item.get("recall_id"), **reference}
                    if item.get("source_artifact_hash") == source_hash
                    else item
                    for raw in cast(list[object], payload["decisions"])
                    for item in (_object(raw),)
                ],
            }
        return payload

    return replace(
        tool,
        handler=read,
        description=tool.description
        + " An already injected current opinion returns its exact input reference"
        + " instead of duplicate text.",
        version="injected-prior-reference-v1-"
        + canonical_hash({"tool": tool.manifest_hash, "reference": reference}),
    )


def _relative_tool(tool: ToolDescriptor, cutoff: date) -> ToolDescriptor:
    """Mask model-visible tool data while preserving underlying source authority."""

    async def read(arguments: dict[str, object]) -> object:
        return _relative_temporal_view(await tool.handler(arguments), cutoff)

    return replace(
        tool,
        handler=read,
        description=cast(str, _relative_temporal_view(tool.description, cutoff)),
        version="relative-v1-"
        + canonical_hash({"manifest": tool.manifest_hash, "cutoff": cutoff.isoformat()}),
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
