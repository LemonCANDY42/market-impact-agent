"""Native news-led discovery, acquired identity, successor thesis, then admission.

This composition never supplies a candidate to the model and never backdates a
current receipt into the historical execution lane. Refusal is a report, not an
execution authority or a fabricated portfolio action.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import cast

from market_impact_agent.account_state import AccountStateSnapshot
from market_impact_agent.agent_contracts import EvidencePack, canonical_hash, pattern_pack_from_dict
from market_impact_agent.agent_runtime import ToolCall
from market_impact_agent.data_inputs import FrozenDataSnapshotInput
from market_impact_agent.domain import TradingMandateV3
from market_impact_agent.dynamic_ashare_admission import (
    DynamicAShareAdmission,
    HistoricalSecurityEvidenceAuthority,
    SecurityAdmission,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_provider import ModelProvider
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchContinuation
from market_impact_agent.pi_execution import native_turn
from market_impact_agent.portfolio_review import PortfolioReviewAuthority
from market_impact_agent.research_acquisition_runtime import (
    AcquisitionResearchResult,
    PreparedResearchSuccessor,
    analyze_with_acquisition,
    freeze_acquired_research,
)
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
    reopen_completed_research_thesis,
)
from market_impact_agent.runtime_store import RunJournal, RuntimeEvent

_POLICY = "native-profile-identity-successor-v1"
_SEEDS = frozenset({"510300.SH", "510500.SH"})


def latest_discovery_report(
    journal: RunJournal, event_id: str, artifact_key: str
) -> RuntimeEvent | None:
    """Follow the single append-only revision chain of an original report."""
    event = journal.event(event_id)
    while event is not None:
        successor = journal.event(event.event_id + ".revision." + str(event.payload[artifact_key]))
        if successor is None:
            return event
        if (
            successor.event_type != event.event_type
            or successor.run_id != event.run_id
            or successor.payload.get("previous_report_event_id") != event.event_id
        ):
            raise PermissionError("discovery report revision crosses its original authority")
        event = successor
    return None


def discovery_acquisition_wait(proof: dict[str, object]) -> bool:
    """Recognize existing v1 wait proofs without reopening generic model failures."""
    return proof.get("status") == "incomplete" and any(
        _object(item).get("status") in {"pending", "uncertain"}
        for item in cast(list[object], proof.get("acquisitions", []))
    )


@dataclass(frozen=True, slots=True)
class ProspectiveDiscoveryResult:
    status: str
    candidate: str | None
    acquisition: AcquisitionResearchResult
    thesis_run_id: str | None
    portfolio_run_id: str | None
    portfolio_terminal_ref: str | None
    gaps: tuple[str, ...]
    proof_artifact_hash: str
    watch_admission_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.prospective-discovery-result.v1",
            "status": self.status,
            "candidate": self.candidate,
            "run_ids": list(self.acquisition.run_ids),
            "final_inputs": self.acquisition.final_inputs.identity_dict(),
            "frozen_snapshot_ids": sorted(
                ()
                if self.acquisition.frozen_input is None
                else self.acquisition.frozen_input.authorized_snapshot_ids
            ),
            "thesis_run_id": self.thesis_run_id,
            "portfolio_run_id": self.portfolio_run_id,
            "portfolio_terminal_ref": self.portfolio_terminal_ref,
            "gaps": list(self.gaps),
            "proof_artifact_hash": self.proof_artifact_hash,
            "watch_admission_ids": list(self.watch_admission_ids),
            "execution_dispatched": False,
        }


class _CandidateSuccessor:
    def __init__(
        self,
        authority: ResearchThesisAuthority,
        initial: ResearchThesisRunInputs,
        current_sources: bool = False,
    ) -> None:
        self.authority, self.initial = authority, initial
        self.current_sources = current_sources
        self.policy = "native-profile-current-mock-successor-v2" if current_sources else _POLICY
        self.candidate: str | None = None
        self.provenance: dict[str, object] = {}
        self.gaps: set[str] = set()

    def _identity(
        self, acquisition: OnDemandResearch, results: tuple[ResearchContinuation, ...]
    ) -> tuple[str, str, dict[str, object]] | None:
        old = self.authority.replay(acquisition.run_id)
        if old.get("status") != "incomplete" or old.get("reason") != "ResearchAcquisitionRequired":
            raise PermissionError("candidate discovery requires a signed sealed acquisition Run")
        record = self.authority.journal.get_run(acquisition.run_id)
        binding = _object(self.authority.store.artifacts.read_json(record.config_hash))
        profile = _object(binding["profile"])
        native_calls: list[tuple[str, str, ToolCall]] = []
        for event in self.authority.journal.events(acquisition.run_id):
            if event.event_type != "pi.role.response.completed":
                continue
            raw_hash = str(event.payload["artifact_hash"])
            raw = _object(self.authority.store.artifacts.read_json(raw_hash))
            turn = native_turn(raw, str(profile["model"]))
            for call in turn.tool_calls:
                if call.name in {"lookup_company_profile", "lookup_fund_profile"}:
                    native_calls.append((event.event_id, raw_hash, call))
        confirmed: dict[str, tuple[str, str, dict[str, object]]] = {}
        for result in results:
            if result.snapshot_id is None or result.successor_cutoff is None:
                continue
            requested = acquisition.budget.journal.event(
                f"{acquisition.budget.owner_run_id}.{result.request_id}.requested"
            )
            if requested is None or requested.payload.get("binding") != acquisition.binding:
                raise PermissionError("candidate metadata request belongs to another Run")
            if requested.payload.get("origin") != "agent_tool":
                continue
            template = acquisition.templates[str(requested.payload["template_id"])]
            if template.api_name not in {"stock_basic", "etf_basic"}:
                continue
            parameters = _object(requested.payload["parameters"])
            symbol = parameters.get("ts_code")
            if (
                not isinstance(symbol, str)
                or len(symbol) != 9
                or not symbol[:6].isdigit()
                or symbol[6:] not in {".SH", ".SZ"}
                or symbol in _SEEDS | set(self.initial.repository.evidence_pack.allowed_targets)
            ):
                continue
            calls = [
                item
                for item in native_calls
                if item[2].name == template.tool_name
                and {
                    key: value
                    for key, value in item[2].arguments.items()
                    if key not in {"offset", "limit"}
                }
                == parameters
            ]
            if not calls:
                raise PermissionError("candidate request lacks its actual native profile tool call")
            acquisition.successor_input((result,))
            snapshot = acquisition.store.get(result.snapshot_id)
            if (
                not snapshot.coverage_complete
                or snapshot.query.sources != (template.source,)
                or snapshot.query.parameters != parameters
                or snapshot.query.source_policy_id != template.template_id
                or snapshot.query.pit_lane != acquisition.pit_lane
                or snapshot.query.as_of > result.successor_cutoff
            ):
                self.gaps.add("candidate_metadata_coverage_unverified")
                continue
            config = template.provider.public_source_config(template.source.upstream_source)
            rows: list[tuple[dict[str, object], str]] = []
            for observation in snapshot.observations:
                raw = _object(acquisition.store.artifacts.read_json(observation.raw_content_hash))
                if raw.get("fields") != config["fields"]:
                    raise PermissionError(
                        "candidate metadata raw fields differ from source contract"
                    )
                row = dict(
                    zip(
                        cast(list[str], raw["fields"]),
                        cast(list[object], raw["values"]),
                        strict=True,
                    )
                )
                if row.get("ts_code") == symbol:
                    rows.append((row, observation.raw_content_hash))
            identities = {canonical_hash(row): (row, raw_hash) for row, raw_hash in rows}
            if len(identities) != 1:
                self.gaps.add("candidate_metadata_identity_unverified")
                continue
            row, raw_hash = next(iter(identities.values()))
            exchange = "SSE" if symbol.endswith(".SH") else "SZSE"
            if (
                row.get("exchange") != exchange
                or not isinstance(row.get("name") or row.get("csname"), str)
                or not str(row.get("name") or row.get("csname")).strip()
            ):
                self.gaps.add("candidate_metadata_identity_unverified")
                continue
            event_id, native_hash, call = calls[0]
            proof: dict[str, object] = {
                "policy": self.policy,
                "candidate": symbol,
                "profile_api": template.api_name,
                "predecessor_run_id": acquisition.run_id,
                "predecessor_terminal_hash": record.terminal_artifact_id,
                "predecessor_binding_hash": record.config_hash,
                "initial_evidence_pack_hash": canonical_hash(
                    self.initial.repository.evidence_pack.to_dict()
                ),
                "root_event_id": self.initial.repository.evidence_pack.event_id,
                "native_response_event_id": event_id,
                "native_response_hash": native_hash,
                "native_call_id": call.call_id,
                "native_tool_name": call.name,
                "native_arguments": dict(call.arguments),
                "request_id": result.request_id,
                "metadata_snapshot_id": snapshot.snapshot_id,
                "metadata_raw_record_hash": raw_hash,
            }
            confirmed[symbol] = (symbol, template.api_name, proof)
        if len(confirmed) != 1:
            self.gaps.add("no_candidate" if not confirmed else "ambiguous_native_candidates")
            return None
        return next(iter(confirmed.values()))

    async def __call__(
        self,
        inputs: ResearchThesisRunInputs,
        acquisition: OnDemandResearch,
        results: tuple[ResearchContinuation, ...],
    ) -> PreparedResearchSuccessor:
        if self.candidate is None:
            identity = self._identity(acquisition, results)
            if identity is None:
                unchanged, frozen = await freeze_acquired_research(inputs, acquisition, results)
                return PreparedResearchSuccessor(
                    unchanged,
                    frozen,
                    results,
                    {"policy": self.policy, "gaps": sorted(self.gaps)},
                    "candidate_identity_unverified",
                )
            self.candidate, api, self.provenance = identity
            end = acquisition.cutoff.strftime("%Y%m%d")
            start = (acquisition.cutoff - timedelta(days=30)).strftime("%Y%m%d")
            symbol = self.candidate
            interval: dict[str, object] = {"ts_code": symbol, "start_date": start, "end_date": end}
            requests: list[tuple[str, dict[str, object]]] = [
                ("lookup_stock_prices" if api == "stock_basic" else "lookup_fund_prices", interval),
                ("lookup_suspensions", interval),
                ("lookup_price_limits", interval),
                (
                    "lookup_exchange_calendar",
                    {
                        "exchange": "SSE" if symbol.endswith(".SH") else "SZSE",
                        "start_date": start,
                        "end_date": end,
                    },
                ),
                (
                    "lookup_company_distributions"
                    if api == "stock_basic"
                    else "lookup_fund_distributions",
                    {"ts_code": symbol},
                ),
                (
                    "lookup_company_adjustments"
                    if api == "stock_basic"
                    else "lookup_fund_adjustments",
                    interval,
                ),
                (
                    "lookup_industry_members"
                    if api == "stock_basic"
                    else "lookup_fund_constituents",
                    {"ts_code": symbol}
                    if api == "stock_basic"
                    else {"ts_code": symbol, "trade_date": end},
                ),
            ]
            if self.current_sources:
                if api == "etf_basic":
                    requests.append(("lookup_fund_asset_class", {"ts_code": symbol}))
                requests.append(
                    (
                        "lookup_stock_quote" if api == "stock_basic" else "lookup_fund_quote",
                        {"ts_code": symbol, "freq": "1MIN"},
                    )
                )
            if self.current_sources:
                seed_interval: dict[str, object] = {
                    "ts_code": "510300.SH",
                    "start_date": start,
                    "end_date": end,
                }
                requests.extend(
                    [
                        ("lookup_fund_profile", {"ts_code": "510300.SH"}),
                        ("lookup_fund_asset_class", {"ts_code": "510300.SH"}),
                        ("lookup_fund_prices", seed_interval),
                        ("lookup_fund_adjustments", seed_interval),
                        ("lookup_fund_distributions", {"ts_code": "510300.SH"}),
                        ("lookup_fund_constituents", {"ts_code": "510300.SH", "trade_date": end}),
                        ("lookup_price_limits", seed_interval),
                        ("lookup_suspensions", seed_interval),
                        (
                            "lookup_exchange_calendar",
                            {"exchange": "SSE", "start_date": start, "end_date": end},
                        ),
                        ("lookup_fund_quote", {"ts_code": "510300.SH", "freq": "1MIN"}),
                    ]
                )
            for tool, arguments in requests:
                queued = await acquisition.request(tool, arguments)
                if queued.get("status") == "data_gap":
                    self.gaps.add(str(queued.get("error_kind", "source_gap")) + ":" + tool)
            # Old Run is sealed; all requests share its bounded durable acquisition owner.
            results = await acquisition.fulfill_pending()
        successor, frozen = await freeze_acquired_research(inputs, acquisition, results)
        pack = successor.repository.evidence_pack
        documents = {
            ref.evidence_id: _object(
                await successor.repository.read_evidence({"evidence_id": ref.evidence_id})
            )["document"]
            for ref in pack.evidence
        }
        patterns = {
            ref.pack_id: pattern_pack_from_dict(
                await successor.repository.read_pattern_pack({"pack_id": ref.pack_id})
            )
            for ref in pack.pattern_packs
        }
        promoted = FrozenResearchRepository(
            evidence_pack=EvidencePack.build(
                event_id=pack.event_id,
                as_of=pack.as_of,
                research_question=pack.research_question,
                evidence=pack.evidence,
                pattern_packs=pack.pattern_packs,
                allowed_targets=tuple(sorted(set(pack.allowed_targets) | {self.candidate})),
                data_gaps=pack.data_gaps,
            ),
            evidence_documents=documents,
            pattern_packs=patterns,
        )
        return PreparedResearchSuccessor(
            replace(
                successor,
                repository=promoted,
                target_id=self.candidate,
                research_question=(
                    f"Assess {self.candidate} using the frozen news and acquired source evidence; "
                    "report uncertainty and a review horizon."
                ),
            ),
            frozen,
            results,
            {**self.provenance, "preparation_gaps": sorted(self.gaps)},
        )


async def run_prospective_discovery(
    *,
    authority: ResearchThesisAuthority,
    provider: ModelProvider,
    inputs: ResearchThesisRunInputs,
    acquisition: OnDemandResearch,
    account_source: Callable[[], AccountStateSnapshot],
    account_max_age: timedelta,
    admission_authority_factory: Callable[
        [ResearchThesisRunInputs, FrozenDataSnapshotInput], HistoricalSecurityEvidenceAuthority
    ],
    portfolio_authority_factory: Callable[
        [ResearchThesisRunInputs, FrozenDataSnapshotInput, AccountStateSnapshot, SecurityAdmission],
        PortfolioReviewAuthority,
    ]
    | None = None,
    portfolio_context_source: Callable[
        [ResearchThesisRunInputs, FrozenDataSnapshotInput], tuple[AccountStateSnapshot, datetime]
    ]
    | None = None,
    maximum_runs: int = 4,
    prior_thesis_run_id: str | None = None,
) -> ProspectiveDiscoveryResult:
    if not 1 <= maximum_runs <= 4 or account_max_age <= timedelta(0):
        raise ValueError("prospective discovery requires a bounded Run count and account age")
    transform = _CandidateSuccessor(authority, inputs, portfolio_context_source is not None)
    if prior_thesis_run_id is not None:
        prior, _ = reopen_completed_research_thesis(
            journal=authority.journal,
            artifact_store=authority.store.artifacts,
            run_id=prior_thesis_run_id,
        )
        prior_binding = _object(
            authority.store.artifacts.read_json(
                authority.journal.get_run(prior_thesis_run_id).config_hash
            )
        )
        if (
            prior_binding.get("account_scope") != authority.account_scope
            or prior_binding.get("arm_id") != authority.arm_id
            or prior_binding.get("experiment_id") != authority.experiment_id
            or _object(prior_binding["inputs"]).get("target_id") != inputs.target_id
            or prior.root_event_id != inputs.repository.evidence_pack.event_id
            or prior.as_of >= inputs.repository.evidence_pack.as_of
        ):
            raise PermissionError("prospective review requires an earlier exact same-scope thesis")
        transform.candidate = inputs.target_id
        transform.provenance = {"prior_thesis_run_id": prior_thesis_run_id}
    result = await analyze_with_acquisition(
        authority=authority,
        provider=provider,
        inputs=inputs,
        acquisition=acquisition,
        maximum_runs=maximum_runs,
        successor_transform=transform,
        successor_transform_id=transform.policy,
        prior_thesis_run_id=prior_thesis_run_id,
    )
    watch_admission_ids: tuple[str, ...] = ()
    if result.status == "completed" and result.final_inputs.watch_delegation is not None:
        from market_impact_agent.research_thesis_watch import (
            ResearchThesisWatchAuthorityResolver,
            admit_research_thesis_watch_proposals,
        )

        if provider.budget is None or authority.account_scope is None:
            raise PermissionError("Watch admission requires the original account and budget")
        resolver = ResearchThesisWatchAuthorityResolver(
            authority.store,
            experiment_id=authority.experiment_id,
            arm_id=authority.arm_id,
            account_scope=authority.account_scope,
            target_id=result.final_inputs.target_id,
            parent_budget=provider.budget,
            episode_id=result.final_inputs.watch_delegation.episode_id,
            clock=authority.clock,
        )
        watch_admission_ids = tuple(
            item.admission_id
            for item in admit_research_thesis_watch_proposals(
                resolver=resolver,
                run_id=result.run_ids[-1],
                admitted_at=authority.journal.get_run(result.run_ids[-1]).updated_at,
            )
        )
    gaps: set[str] = set()
    thesis_run_id = None
    portfolio_run_id = portfolio_terminal_ref = None
    security: SecurityAdmission | None = None
    status = "incomplete"
    if transform.candidate is None:
        gaps.update(transform.gaps or {"no_candidate"})
    elif result.status != "completed":
        gaps.add("candidate_research_" + result.status)
    else:
        final = result.final_inputs
        if result.frozen_input is None:
            raise PermissionError("candidate successor lacks frozen metadata snapshots")
        thesis_run_id = result.run_ids[-1]
        authority.replay(thesis_run_id)
        bound = _object(
            authority.store.artifacts.read_json(
                authority.journal.get_run(thesis_run_id).config_hash
            )
        )
        if (
            _object(bound["inputs"]) != final.identity_dict()
            or final.target_id != transform.candidate
        ):
            raise PermissionError("candidate thesis differs from its verified successor binding")
        portfolio_cutoff = final.repository.evidence_pack.as_of
        account: AccountStateSnapshot | None = None
        if portfolio_context_source is not None:
            try:
                account, portfolio_cutoff = portfolio_context_source(final, result.frozen_input)
            except (PermissionError, FileNotFoundError, LookupError) as error:
                gaps.add("current_portfolio_context_missing:" + str(error))
            if account is not None and (
                portfolio_cutoff < authority.journal.get_run(thesis_run_id).updated_at
                or account.as_of > portfolio_cutoff
            ):
                raise PermissionError(
                    "portfolio cutoff must follow thesis completion and account capture"
                )
        source = admission_authority_factory(final, result.frozen_input)
        security = DynamicAShareAdmission(source).discover(
            (transform.candidate,), portfolio_cutoff
        )[0]
        gaps.update(security.gaps)
        status = "admission_refused"
        if not gaps:
            try:
                if account is None:
                    account = account_source()
            except (PermissionError, FileNotFoundError, LookupError):
                gaps.add("account_authority_missing")
            if account is not None:
                if account.account_reference_hash != authority.account_scope:
                    raise PermissionError("candidate portfolio crosses the research account scope")
                if not account.readiness(
                    evaluated_at=portfolio_cutoff, max_age=account_max_age
                ).exposure_increase_ready:
                    gaps.add("account_authority_incomplete_or_stale")
            if not gaps and portfolio_authority_factory is None:
                gaps.add("portfolio_authority_missing")
            if not gaps and len(result.run_ids) >= maximum_runs:
                gaps.add("portfolio_run_limit")
            if not gaps:
                assert account is not None and portfolio_authority_factory is not None
                portfolio = portfolio_authority_factory(
                    final, result.frozen_input, account, security
                )
                current = portfolio.input_source()
                basis = current.price_bases.get(transform.candidate)
                if (
                    portfolio.store.root.resolve() != authority.store.root.resolve()
                    or current.account_state != account
                    or current.cutoff != portfolio_cutoff
                    or not isinstance(current.mandate, TradingMandateV3)
                    or transform.candidate not in current.mandate.allowed_instruments
                    or basis is None
                    or security.evidence is None
                    or basis.source_version != canonical_hash(security.evidence.to_dict())
                ):
                    raise PermissionError(
                        "prospective portfolio lacks exact current dynamic source/account binding"
                    )
                portfolio_run_id = acquisition.run_id + ".portfolio"
                review = await portfolio.review(
                    run_id=portfolio_run_id,
                    provider=provider,
                    research_run_ids=(),
                    research_thesis_run_ids=(thesis_run_id,),
                )
                portfolio_terminal_ref = authority.journal.get_run(
                    portfolio_run_id
                ).terminal_artifact_id
                if (
                    review.get("status") != "completed"
                    or _object(review.get("decision", {})).get("outcome") == "rejected"
                ):
                    gaps.add("portfolio_action_not_admitted")
                else:
                    status = "portfolio_completed"
    proof = {
        "schema_version": "market-impact.prospective-discovery-proof.v1",
        "status": status,
        "candidate_provenance": transform.provenance,
        "research_run_ids": list(result.run_ids),
        "final_inputs": result.final_inputs.identity_dict(),
        "frozen_snapshot_ids": sorted(
            () if result.frozen_input is None else result.frozen_input.authorized_snapshot_ids
        ),
        "acquisitions": [item.to_dict() for item in result.acquisitions],
        "security_admission": None if security is None else security.to_dict(),
        "portfolio_run_id": portfolio_run_id,
        "portfolio_terminal_ref": portfolio_terminal_ref,
        "gaps": sorted(gaps),
        "preparation_gaps": sorted(transform.gaps),
    }
    proof_hash = authority.store.artifacts.put_json(proof).content_hash
    suffix = "prospective.discovery." + canonical_hash(acquisition.run_id)
    previous = latest_discovery_report(
        authority.journal,
        acquisition.budget.owner_run_id + "." + suffix,
        "proof_artifact_hash",
    )
    payload: dict[str, object] = {"proof_artifact_hash": proof_hash}
    if previous is not None:
        previous_hash = str(previous.payload["proof_artifact_hash"])
        suffix = previous.event_id.removeprefix(acquisition.budget.owner_run_id + ".")
        payload = previous.payload
        if previous_hash != proof_hash:
            if not discovery_acquisition_wait(
                _object(authority.store.artifacts.read_json(previous_hash))
            ):
                raise PermissionError("only an acquisition wait report can be continued")
            suffix += ".revision." + previous_hash
            payload = {
                "proof_artifact_hash": proof_hash,
                "previous_report_event_id": previous.event_id,
            }
    acquisition._append(  # pyright: ignore[reportPrivateUsage]
        suffix,
        "prospective.discovery.reported",
        payload,
    )
    return ProspectiveDiscoveryResult(
        status,
        transform.candidate,
        result,
        thesis_run_id,
        portfolio_run_id,
        portfolio_terminal_ref,
        tuple(sorted(gaps)),
        proof_hash,
        watch_admission_ids,
    )


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("discovery authority requires a frozen object")
    return cast(dict[str, object], value)
