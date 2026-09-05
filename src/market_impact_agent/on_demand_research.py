"""Frozen semantic research tools and outside-Run acquisition continuations.

Requests, receipt staging and completions live in the existing parent Run Journal.
The active tool closure never performs acquisition or expands its frozen inputs.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.data_acquisition import AcquisitionPending, AcquisitionUncertain
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSnapshot,
    DataSourceBinding,
    FrozenDataSnapshotInput,
    LocalDataSnapshotStore,
    ProviderDataResponse,
    source_observation_from_dict,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.observations import ObservationCapability, ObservationProviderManifest
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_capture_bundle,
    tushare_observation_source_from_dict,
)
from market_impact_agent.tushare_range_cache import TushareDailyRangeCache

_PROJECTION_VERSION = "compact-facts-v2"


class ResearchQueryValidationError(ValueError):
    """Correctable model query arguments, never an authority or acquisition failure."""


_ROUTES = {
    "daily": ("lookup_stock_prices", "Raw stock daily prices"),
    "fund_daily": ("lookup_fund_prices", "Raw fund daily prices"),
    "stock_basic": ("lookup_company_profile", "Currently observed company master metadata"),
    "etf_basic": ("lookup_fund_profile", "Currently observed ETF master metadata"),
    "index_classify": ("lookup_industry_taxonomy", "Observed industry taxonomy codes and names"),
    "index_member_all": (
        "lookup_industry_members",
        "Observed industry members; filter by industry code or company",
    ),
    "etf_sh_cons": (
        "lookup_fund_constituents",
        "Observed Shanghai ETF constituent disclosure for a trade date",
    ),
    "news": ("lookup_news_events", "News in a bounded window; dates use YYYY-MM-DD HH:MM:SS"),
    "suspend_d": ("lookup_suspensions", "Reported suspension records in a date interval"),
    "stk_limit": ("lookup_price_limits", "Reported upper/lower price limits for a trade date"),
    "fund_adj": (
        "lookup_fund_adjustments",
        "Observed fund adjustment factors; not executable prices",
    ),
    "adj_factor": (
        "lookup_company_adjustments",
        "Observed stock adjustment factors; not cash distributions or executable prices",
    ),
    "fund_div": ("lookup_fund_distributions", "Observed fund distribution records and schedules"),
    "dividend": (
        "lookup_company_distributions",
        "Observed company dividend and corporate-action records",
    ),
    "trade_cal": (
        "lookup_exchange_calendar",
        "Observed exchange calendar for a bounded date interval",
    ),
}
_UNAVAILABLE = {
    "lookup_industry_context": {"index_classify", "index_member_all", "etf_sh_cons"},
    "lookup_event_context": {"news"},
    "lookup_tradability": {"suspend_d", "stk_limit"},
}
_PARAMETERS = {
    "daily": ("ts_code", "start_date", "end_date"),
    "fund_daily": ("ts_code", "start_date", "end_date"),
    "stock_basic": ("ts_code",),
    "etf_basic": ("ts_code",),
    "index_classify": ("index_code", "level", "src"),
    "index_member_all": ("l1_code", "l2_code", "l3_code", "ts_code", "is_new"),
    "etf_sh_cons": ("ts_code", "trade_date"),
    "news": ("start_date", "end_date"),
    "suspend_d": ("ts_code", "start_date", "end_date"),
    "stk_limit": ("ts_code", "trade_date", "start_date", "end_date"),
    "fund_adj": ("ts_code", "start_date", "end_date"),
    "adj_factor": ("ts_code", "start_date", "end_date"),
    "fund_div": ("ts_code", "ann_date", "ex_date", "pay_date"),
    "dividend": ("ts_code", "ann_date", "record_date", "ex_date", "imp_ann_date"),
    "trade_cal": ("exchange", "start_date", "end_date", "is_open"),
}


@dataclass(frozen=True, slots=True)
class ResearchSourceTemplate:
    tool_name: str
    description: str
    api_name: str
    source: DataSourceBinding
    capability: ObservationCapability
    provider: TushareObservationProvider = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        self.assert_binding()

    def assert_binding(self) -> None:
        config = self.provider.public_source_config(self.source.upstream_source)
        if (
            self.api_name not in _ROUTES
            or self.tool_name != _ROUTES[self.api_name][0]
            or config.get("api_name") != self.api_name
            or self.source.source_config_hash != canonical_hash(config)
            or self.source.manifest_hash != canonical_hash(self.provider.manifest.to_dict())
            or self.source.provider_id != self.provider.manifest.provider_id
            or self.source.provider_version != self.provider.manifest.provider_version
            or self.capability.value != config.get("capability")
        ):
            raise ValueError("research template does not match its registered Provider route")

    @classmethod
    def from_tushare(
        cls, provider: TushareObservationProvider, source_id: str
    ) -> ResearchSourceTemplate:
        config = provider.public_source_config(source_id)
        api_name = cast(str, config["api_name"])
        if api_name not in _ROUTES:
            raise ValueError("source has no accepted on-demand semantic template")
        name, description = _ROUTES[api_name]
        return cls(
            name,
            description,
            api_name,
            DataSourceBinding(
                provider_id=provider.manifest.provider_id,
                provider_version=provider.manifest.provider_version,
                upstream_source=source_id,
                manifest_hash=canonical_hash(provider.manifest.to_dict()),
                source_config_hash=canonical_hash(config),
                required=True,
            ),
            ObservationCapability(cast(str, config["capability"])),
            provider,
        )

    @property
    def parameters(self) -> tuple[str, ...]:
        return _PARAMETERS[self.api_name]

    @property
    def required_parameters(self) -> tuple[str, ...]:
        if self.api_name in {"index_classify", "index_member_all"}:
            return ()
        if self.api_name in {"fund_div", "dividend"}:
            return ("ts_code",)
        if self.api_name == "stk_limit":
            return ("ts_code",)
        if self.api_name == "trade_cal":
            return ("exchange", "start_date", "end_date")
        return self.parameters

    @property
    def binding(self) -> dict[str, object]:
        return {
            "tool_name": self.tool_name,
            "api_name": self.api_name,
            "source": self.source.to_dict(),
            "capability": self.capability.value,
            "parameters": list(self.parameters),
            "required_parameters": list(self.required_parameters),
        }

    @property
    def template_id(self) -> str:
        return f"research-template-{canonical_hash(self.binding)}"

    def validate(self, arguments: dict[str, object]) -> None:
        if not set(arguments) <= set(self.parameters) or not set(self.required_parameters) <= set(
            arguments
        ):
            raise ResearchQueryValidationError(
                "research query accepts only its declared domain parameters"
            )
        if any(not isinstance(value, str) or not value.strip() for value in arguments.values()):
            raise ResearchQueryValidationError(
                "research domain parameters must be nonempty strings"
            )
        if "ts_code" in arguments and "," in cast(str, arguments["ts_code"]):
            raise ResearchQueryValidationError("research query requires one instrument")
        if self.api_name == "stk_limit" and not (
            set(arguments) == {"ts_code", "trade_date"}
            or set(arguments) == {"ts_code", "start_date", "end_date"}
        ):
            raise ResearchQueryValidationError(
                "price limits require ts_code with either trade_date or both start_date and "
                "end_date; do not combine trade_date with the interval"
            )
        if self.api_name == "index_member_all" and not set(arguments) & {
            "l1_code",
            "l2_code",
            "l3_code",
            "ts_code",
        }:
            raise ResearchQueryValidationError(
                "industry membership requires an industry or company code"
            )
        for name in (
            "trade_date",
            "ann_date",
            "ex_date",
            "pay_date",
            "record_date",
            "imp_ann_date",
        ):
            if name in arguments:
                _parse_domain_date(cast(str, arguments[name]), "%Y%m%d")
        if "start_date" in arguments:
            date_format = "%Y-%m-%d %H:%M:%S" if self.api_name == "news" else "%Y%m%d"
            start = _parse_domain_date(cast(str, arguments["start_date"]), date_format)
            end = _parse_domain_date(cast(str, arguments["end_date"]), date_format)
            if start > end:
                raise ResearchQueryValidationError("research query start_date exceeds end_date")


@dataclass(frozen=True, slots=True)
class ResearchContinuation:
    request_id: str
    status: str
    snapshot_id: str | None = None
    successor_cutoff: datetime | None = None
    error_kind: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "successor_cutoff": None
            if self.successor_cutoff is None
            else self.successor_cutoff.isoformat(),
            "error_kind": self.error_kind,
        }


class OnDemandResearch:
    def __init__(
        self,
        *,
        store: LocalDataSnapshotStore,
        parent_budget: ModelBudget,
        episode_deadline: datetime,
        run_id: str,
        episode_id: str | None = None,
        cutoff: datetime,
        pit_lane: DataPITLane,
        templates: tuple[ResearchSourceTemplate, ...],
        frozen_input: FrozenDataSnapshotInput | None = None,
        historical_inputs: HistoricalAShareInputs | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        require_aware(episode_deadline, "episode deadline")
        require_aware(cutoff, "research cutoff")
        if cutoff.utcoffset() != UTC.utcoffset(cutoff):
            raise ValueError("research cutoff must use UTC")
        if (
            parent_budget.journal.path != store.index_path
            or parent_budget.journal.harness_authority_id != store.harness_authority_id
        ):
            raise ValueError("research acquisition must share the parent Harness authority")
        if len({template.tool_name for template in templates}) != len(templates):
            raise ValueError("duplicate semantic research tool")
        self.store = store
        self.budget = parent_budget
        self.deadline = episode_deadline
        self.episode_id = parent_budget.owner_run_id if episode_id is None else episode_id
        if not self.episode_id or self.episode_id != self.episode_id.strip():
            raise ValueError("research episode_id must be nonempty trimmed text")
        self.run_id = run_id
        self.cutoff = cutoff
        self.pit_lane = pit_lane
        self.templates = {item.template_id: item for item in templates}
        self.clock = clock
        self.historical_inputs = historical_inputs
        if historical_inputs is not None and (
            pit_lane is not DataPITLane.MODELED
            or historical_inputs.store.index_path != store.index_path
        ):
            raise PermissionError("modeled acquisition requires same-root historical authority")
        self.snapshots = tuple(
            store.get(item)
            for item in sorted(() if frozen_input is None else frozen_input.authorized_snapshot_ids)
        )
        if historical_inputs is None and any(
            snapshot.query.as_of > cutoff or snapshot.query.pit_lane != pit_lane
            for snapshot in self.snapshots
        ):
            raise ValueError("research frozen inputs exceed cutoff or PIT lane")
        self.binding: dict[str, object] = {
            "schema_version": "market-impact.on-demand-research.v1",
            "parent_run_id": parent_budget.owner_run_id,
            "parent_budget": parent_budget.binding,
            "episode_deadline": episode_deadline.isoformat(),
            "run_id": run_id,
            "cutoff": cutoff.isoformat(),
            "pit_lane": pit_lane.value,
            "template_ids": sorted(self.templates),
            "snapshot_ids": [item.snapshot_id for item in self.snapshots],
        }
        if historical_inputs is not None:
            self.binding["historical_policy"] = {
                "policy": {
                    key: str(value) for key, value in historical_inputs.policy.to_dict().items()
                },
                "base_snapshot_ids": list(historical_inputs.snapshot_ids),
                "rule_artifact_hashes": list(historical_inputs.rule_artifact_hashes),
                **(
                    {"fund_halt_artifact_hashes": list(historical_inputs.fund_halt_artifact_hashes)}
                    if historical_inputs.fund_halt_artifact_hashes
                    else {}
                ),
            }
        if episode_id is not None:
            self.binding["episode_id"] = self.episode_id
        self._append(
            "research.episode-binding"
            if episode_id is None
            else f"research.episode-binding.{canonical_hash(self.episode_id)}",
            "research.episode.binding",
            {
                "parent_budget": parent_budget.binding,
                "episode_deadline": episode_deadline.isoformat(),
            },
        )
        # Reopening this active Run cannot change its deadline, allowance, or inputs.
        self._append(f"binding.{canonical_hash(run_id)}", "research.binding", self.binding)

    def snapshot_projection(self, snapshot: DataSnapshot) -> dict[str, object]:
        """A bounded view; the content-identified source remains in the snapshot store."""
        template = next(
            (
                item
                for item in self.templates.values()
                if snapshot.query.sources == (item.source,)
                and snapshot.query.source_policy_id == item.template_id
            ),
            None,
        )
        return {
            "projection_version": _PROJECTION_VERSION,
            "modeled_cutoff": self.cutoff.isoformat() if self.historical_inputs else None,
            "strict_pit_accepted": False if self.historical_inputs else None,
            "snapshot_id": snapshot.snapshot_id,
            "query": snapshot.query.to_dict(),
            "completed_at": snapshot.completed_at.isoformat(),
            "coverage_complete": snapshot.coverage_complete,
            "attempts": [item.to_dict() for item in snapshot.attempts],
            "observation_count": len(snapshot.observations),
            "read_tool": None
            if template is None
            else {
                "name": template.tool_name,
                "arguments": dict(snapshot.query.parameters),
                "pagination": {"offset": 0, "limit": 20, "maximum_limit": 100},
            },
        }

    def _modeled_page(
        self,
        snapshot: DataSnapshot,
        template: ResearchSourceTemplate,
        arguments: dict[str, object],
        offset: int,
        limit: int,
    ) -> dict[str, object]:
        assert self.historical_inputs is not None
        market = self.historical_inputs.with_snapshots((snapshot.snapshot_id,))
        projection = market.research_series(str(arguments["ts_code"]), self.cutoff, limit=252)
        observations = {item.raw_content_hash: item for item in snapshot.observations}
        start, end = str(arguments["start_date"]), str(arguments["end_date"])
        rows = [
            row
            for row in cast(list[dict[str, object]], projection["rows"])
            if start <= str(row["trade_date"]).replace("-", "") <= end
            and row["source_record_hash"] in observations
        ]
        facts: list[dict[str, object]] = []
        for row in rows[offset : offset + limit]:
            original = observations[str(row["source_record_hash"])]
            facts.append(
                {
                    "observation_id": original.observation_id,
                    "raw_content_hash": original.raw_content_hash,
                    "times": original.times.to_dict(),
                    "modeled_fact": row,
                }
            )
        return {
            "status": "available" if snapshot.coverage_complete and rows else "data_gap",
            "projection_version": "modeled-completed-raw-prices-v1",
            "snapshot_id": snapshot.snapshot_id,
            "source_api": template.api_name,
            "actual_receipt_query": snapshot.query.to_dict(),
            "modeled_cutoff": self.cutoff.isoformat(),
            "policy_id": market.policy.policy_id,
            "strict_pit_accepted": False,
            "gaps": projection["gaps"],
            "observations": facts,
            "page": {
                "offset": offset,
                "limit": limit,
                "total": len(rows),
                "next_offset": offset + limit if offset + limit < len(rows) else None,
            },
        }

    def descriptors(self) -> tuple[ToolDescriptor, ...]:
        result: list[ToolDescriptor] = []
        for template in self.templates.values():

            async def handler(
                arguments: dict[str, object], template: ResearchSourceTemplate = template
            ) -> object:
                try:
                    return self._lookup(template, arguments, origin="agent_tool")
                except ResearchQueryValidationError as exc:
                    return {
                        "status": "validation_error",
                        "error_kind": "invalid_query_arguments",
                        "tool": template.tool_name,
                        "message": str(exc),
                        "retryable": True,
                        "allowed_parameters": [*template.parameters, "offset", "limit"],
                        "required_parameters": list(template.required_parameters),
                    }

            result.append(
                ToolDescriptor(
                    name=template.tool_name,
                    version=canonical_hash(
                        {
                            "binding": self.binding,
                            "template": template.binding,
                            "projection_version": _PROJECTION_VERSION,
                            "query_validation_version": "recoverable-domain-errors-v1",
                        }
                    ),
                    description=template.description
                    + ". Frozen evidence only; a miss requests an outside-Run continuation.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            **{
                                name: {"type": "string", "minLength": 1}
                                for name in template.parameters
                            },
                            "offset": {"type": "integer", "minimum": 0},
                            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                        },
                        "required": list(template.required_parameters),
                        "additionalProperties": False,
                    },
                    required_capabilities=frozenset({"read_market_context"}),
                    side_effect=ToolSideEffect.READ_ONLY,
                    timeout_seconds=5.0,
                    max_result_bytes=100_000,
                    handler=handler,
                )
            )
        configured = {template.api_name for template in self.templates.values()}
        for name, routes in _UNAVAILABLE.items():
            if configured & routes:
                continue

            async def unavailable(arguments: dict[str, object], name: str = name) -> object:
                del arguments
                return {
                    "status": "data_gap",
                    "tool": name,
                    "error_kind": "source_route_unconfigured",
                }

            result.append(
                ToolDescriptor(
                    name=name,
                    version=canonical_hash(self.binding),
                    description="This information aspect has no accepted source bound.",
                    input_schema={
                        "type": "object",
                        "properties": {"subject": {"type": "string"}},
                        "required": ["subject"],
                        "additionalProperties": False,
                    },
                    required_capabilities=frozenset({"read_market_context"}),
                    side_effect=ToolSideEffect.READ_ONLY,
                    timeout_seconds=5.0,
                    max_result_bytes=2000,
                    handler=unavailable,
                )
            )
        return tuple(result)

    async def request(
        self,
        tool_name: str,
        arguments: dict[str, object],
        *,
        origin: str = "harness_preparation",
    ) -> dict[str, object]:
        """Queue an authorized zero-model preparation without inventing an Agent call."""
        if origin != "harness_preparation":
            raise ValueError("public preparation request requires harness_preparation origin")
        template = next(
            (item for item in self.templates.values() if item.tool_name == tool_name), None
        )
        if template is None:
            return {
                "status": "data_gap",
                "error_kind": "source_route_unconfigured",
                "tool": tool_name,
            }
        return self._lookup(template, arguments, origin=origin)

    def _lookup(
        self,
        template: ResearchSourceTemplate,
        arguments: dict[str, object],
        *,
        origin: str,
    ) -> dict[str, object]:
        offset, limit = arguments.get("offset", 0), arguments.get("limit", 20)
        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 100:
            raise ResearchQueryValidationError(
                "research pagination requires nonnegative offset and limit 1..100"
            )
        arguments = {
            key: value for key, value in arguments.items() if key not in {"offset", "limit"}
        }
        template.validate(arguments)
        if self.historical_inputs is not None:
            gap = self.historical_inputs.research_query_gap(
                template.api_name, arguments, self.cutoff
            )
            if gap is not None:
                return {"status": "data_gap", "error_kind": gap}
        if template.api_name in {
            "daily",
            "fund_daily",
            "fund_adj",
            "adj_factor",
            "suspend_d",
            "stk_limit",
        }:
            window_end = arguments.get("end_date", arguments.get("trade_date"))
            if (
                window_end is not None
                and _parse_domain_date(cast(str, window_end), "%Y%m%d").date() > self.cutoff.date()
            ):
                return {"status": "data_gap", "error_kind": "query_window_after_cutoff"}
        for snapshot in sorted(
            self.snapshots,
            key=lambda item: (item.query.as_of, item.completed_at, item.snapshot_id),
            reverse=True,
        ):
            if (
                snapshot.query.sources == (template.source,)
                and snapshot.query.capability == template.capability
                and snapshot.query.source_policy_id == template.template_id
                and snapshot.query.parameters == arguments
            ):
                if self.historical_inputs is not None:
                    return self._modeled_page(snapshot, template, arguments, offset, limit)
                return {
                    "status": "available" if snapshot.coverage_complete else "data_gap",
                    "snapshot_id": snapshot.snapshot_id,
                    "query_id": snapshot.query.query_id,
                    "as_of": snapshot.query.as_of.isoformat(),
                    "pit_lane": self.pit_lane.value,
                    "modeled_cutoff": self.cutoff.isoformat() if self.historical_inputs else None,
                    "strict_pit_accepted": False if self.historical_inputs else None,
                    "coverage_complete": snapshot.coverage_complete,
                    "attempts": [item.to_dict() for item in snapshot.attempts],
                    "projection_version": _PROJECTION_VERSION,
                    "query": snapshot.query.to_dict(),
                    "source_api": template.api_name,
                    "source_semantics": {
                        key: value
                        for key, value in (
                            snapshot.observations[0].normalized_payload.items()
                            if snapshot.observations
                            else ()
                        )
                        if key != "record"
                    },
                    "observations": [
                        {
                            "observation_id": item.observation_id,
                            "raw_content_hash": item.raw_content_hash,
                            "times": item.times.to_dict(),
                            "authority_at": None
                            if item.authority_at is None
                            else item.authority_at.isoformat(),
                            "authority_kind": item.authority_kind,
                            "record": item.normalized_payload["record"],
                        }
                        for item in sorted(
                            snapshot.observations, key=lambda item: item.observation_id
                        )[offset : offset + limit]
                    ],
                    "page": {
                        "offset": offset,
                        "limit": limit,
                        "total": len(snapshot.observations),
                        "next_offset": offset + limit
                        if offset + limit < len(snapshot.observations)
                        else None,
                    },
                }
        if self.pit_lane is not DataPITLane.PROSPECTIVE and self.historical_inputs is None:
            return {
                "status": "data_gap",
                "error_kind": "planned_external_historical_acquisition",
                "template_id": template.template_id,
                "parameters": arguments,
                "cutoff": self.cutoff.isoformat(),
                "pit_lane": self.pit_lane.value,
            }
        payload: dict[str, object] = {
            "binding": self.binding,
            "origin": origin,
            "template_id": template.template_id,
            "parameters": arguments,
        }
        request_id = f"research-request-{canonical_hash(payload)}"
        self._append(request_id + ".requested", "research.data.requested", payload)
        return {
            "status": "continuation_required",
            "request_id": request_id,
            "cutoff": self.cutoff.isoformat(),
        }

    async def fulfill_pending(self) -> tuple[ResearchContinuation, ...]:
        """Call only after the active model Run has yielded; never from a tool handler."""
        results: list[ResearchContinuation] = []
        for event in self.budget.journal.events(self.budget.owner_run_id):
            if (
                event.event_type != "research.data.requested"
                or event.payload["binding"] != self.binding
            ):
                continue
            request_id = event.event_id.removeprefix(self.budget.owner_run_id + ".").removesuffix(
                ".requested"
            )
            results.append(await self._fulfill(request_id, event.payload))
        return tuple(results)

    async def _fulfill(self, request_id: str, payload: dict[str, object]) -> ResearchContinuation:
        journal = self.budget.journal
        prefix = f"{self.budget.owner_run_id}.{request_id}"
        completed = journal.event(prefix + ".completed")
        if completed is not None:
            return _continuation(completed.payload)
        claim = journal.try_claim_run(prefix)
        if claim is None:
            return ResearchContinuation(request_id, "pending", error_kind="acquisition_owned")
        attempt_suffix: str | None = None
        try:
            completed = journal.event(prefix + ".completed")
            if completed is not None:
                return _continuation(completed.payload)
            self._check_parent()
            template = self.templates[cast(str, payload["template_id"])]
            template.assert_binding()
            arguments = cast(dict[str, object], payload["parameters"])
            staged = journal.event(prefix + ".received")
            attempts = [
                event
                for event in journal.events(self.budget.owner_run_id)
                if event.event_type == "research.data.started"
                and event.payload.get("request_id") == request_id
            ]
            started = attempts[-1] if attempts else None
            if staged is None:
                if started is not None and journal.event(started.event_id + ".deferred") is None:
                    return ResearchContinuation(
                        request_id, "uncertain", error_kind="receipt_not_durable"
                    )
                attempt_suffix = request_id + f".started.{len(attempts)}"
                self._append(attempt_suffix, "research.data.started", {"request_id": request_id})
                query = self._query(
                    template, arguments, self.clock() if self.historical_inputs else self.cutoff
                )
                provider = (
                    TushareDailyRangeCache(template.provider, self.store)
                    if template.api_name in {"daily", "fund_daily"}
                    else template.provider
                )
                response = await asyncio.wait_for(
                    self._acquire(template, provider, query), timeout=self._remaining()
                )
                if response.retrieved_at > self.clock():
                    raise ValueError("provider receipt is in the Harness future")
                artifact_hash = await asyncio.to_thread(_store_response, self.store, response)
                self._append(
                    request_id + ".received",
                    "research.data.received",
                    {"artifact_hash": artifact_hash},
                )
            else:
                response = _load_response(self.store, cast(str, staged.payload["artifact_hash"]))
            self._check_parent()
            successor_cutoff = max(self.cutoff, response.retrieved_at)
            harness = DataInputHarness(self.store)
            harness.register(_ReceivedProvider(template.provider, response))
            snapshot = await harness.execute(
                self._query(template, arguments, successor_cutoff),
                mode=DataQueryMode.DURABLE_FETCH_IF_MISSING,
            )
            result = ResearchContinuation(
                request_id,
                "fulfilled" if snapshot.coverage_complete else "data_gap",
                snapshot.snapshot_id,
                self.cutoff if self.historical_inputs else successor_cutoff,
                None if snapshot.coverage_complete else "source_coverage_incomplete",
            )
            self._append(request_id + ".completed", "research.data.completed", result.to_dict())
            return result
        except AcquisitionPending:
            if attempt_suffix is not None:
                self._append(
                    attempt_suffix + ".deferred",
                    "research.data.deferred",
                    {"request_id": request_id, "reason": "acquisition_owned"},
                )
            return ResearchContinuation(request_id, "pending", error_kind="acquisition_owned")
        except AcquisitionUncertain:
            return ResearchContinuation(request_id, "uncertain", error_kind="acquisition_uncertain")
        finally:
            claim.release()

    async def _acquire(
        self,
        template: ResearchSourceTemplate,
        provider: TushareDailyRangeCache | TushareObservationProvider,
        query: DataQuery,
    ) -> ProviderDataResponse:
        if isinstance(provider, TushareDailyRangeCache):
            return await provider.fetch(query=query, source=template.source)
        harness = DataInputHarness(self.store)
        harness.register(provider)
        initial = await harness.execute(query, mode=DataQueryMode.DURABLE_FETCH_IF_MISSING)
        attempt = initial.attempts[0]
        if attempt.raw_response_hash is None:
            return provider.failed_response(
                source=template.source,
                retrieved_at=attempt.retrieved_at,
                status=attempt.status,
                error_kind=attempt.error_kind or "source_failure",
            )
        artifact = self.store.artifacts.get(
            attempt.raw_response_hash, media_type="application/octet-stream"
        )
        if not attempt.status.completed:
            return provider.failed_response(
                source=template.source,
                retrieved_at=attempt.retrieved_at,
                status=attempt.status,
                error_kind=attempt.error_kind or "source_failure",
                raw_payload=artifact.path.read_bytes(),
            )
        config = tushare_observation_source_from_dict(
            dict(provider.public_source_config(template.source.upstream_source))
        )
        capture = load_tushare_observation_capture_bundle(
            artifact.path.read_bytes(),
            config=config,
            parameters=query.parameters,
            retrieved_at=attempt.retrieved_at,
        )
        return provider.response_from_capture(query=query, source=template.source, capture=capture)

    def successor_input(
        self, results: tuple[ResearchContinuation, ...]
    ) -> tuple[datetime, FrozenDataSnapshotInput]:
        """Build the caller's next Run input; this does not mutate these active tools."""
        snapshots = {item.snapshot_id for item in self.snapshots}
        cutoff = self.cutoff
        for result in results:
            if result.snapshot_id is not None and result.successor_cutoff is not None:
                event = self.budget.journal.event(
                    f"{self.budget.owner_run_id}.{result.request_id}.completed"
                )
                requested = self.budget.journal.event(
                    f"{self.budget.owner_run_id}.{result.request_id}.requested"
                )
                if (
                    event is None
                    or event.payload != result.to_dict()
                    or requested is None
                    or requested.payload["binding"] != self.binding
                ):
                    raise ValueError("successor input requires this parent durable completion")
                snapshots.add(result.snapshot_id)
                cutoff = max(cutoff, result.successor_cutoff)
        return cutoff, FrozenDataSnapshotInput(frozenset(snapshots))

    def _query(
        self, template: ResearchSourceTemplate, arguments: dict[str, object], cutoff: datetime
    ) -> DataQuery:
        return DataQuery.build(
            capability=template.capability,
            pit_lane=DataPITLane.PROSPECTIVE if self.historical_inputs else self.pit_lane,
            as_of=cutoff,
            window_start=None,
            source_policy_id=template.template_id,
            parameters=arguments,
            sources=(template.source,),
            minimum_data_sources=1,
        )

    def _remaining(self) -> float:
        remaining = (self.deadline - self.clock()).total_seconds()
        if remaining <= 0:
            raise TimeoutError("parent episode deadline exceeded")
        return remaining

    def _check_parent(self) -> None:
        self.budget.check_cancel()
        self._remaining()
        state = self.budget.summary()
        if state["physical_requests"] >= self.budget.max_requests or (
            self.budget.max_cost_microusd is not None
            and state["known_cost_microusd"] + state["reserved_microusd"]
            >= self.budget.max_cost_microusd
        ):
            raise PermissionError("parent episode model budget cannot fund a successor")

    def _append(self, suffix: str, kind: str, payload: dict[str, object]) -> None:
        if self.budget.append is not None:
            self.budget.append(suffix, kind, payload)
            return
        self.budget.journal.append(
            run_id=self.budget.owner_run_id,
            event_id=f"{self.budget.owner_run_id}.{suffix}",
            event_type=kind,
            observed_at=self.clock(),
            payload=payload,
        )


@dataclass
class _ReceivedProvider:
    provider: TushareObservationProvider
    response: ProviderDataResponse

    @property
    def manifest(self) -> ObservationProviderManifest:
        return self.provider.manifest

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]:
        return self.provider.public_source_config(upstream_source)

    async def fetch(self, *, query: DataQuery, source: DataSourceBinding) -> ProviderDataResponse:
        del query, source
        return self.response


def _store_response(store: LocalDataSnapshotStore, response: ProviderDataResponse) -> str:
    raw = response.raw_payload
    return store.artifacts.put_json(
        {
            "status": response.status.value,
            "provider_id": response.provider_id,
            "provider_version": response.provider_version,
            "upstream_source": response.upstream_source,
            "retrieved_at": response.retrieved_at.isoformat(),
            "error_kind": response.error_kind,
            "raw_payload": None if raw is None else base64.b64encode(raw).decode(),
            "observations": [item.to_dict() for item in response.observations],
            "raw_records": [
                [key, base64.b64encode(raw).decode()] for key, raw in response.raw_records
            ],
        }
    ).content_hash


def _load_response(store: LocalDataSnapshotStore, artifact_hash: str) -> ProviderDataResponse:
    value = cast(dict[str, object], store.artifacts.read_json(artifact_hash))
    raw = value["raw_payload"]
    return ProviderDataResponse(
        status=DataFetchStatus(cast(str, value["status"])),
        provider_id=cast(str, value["provider_id"]),
        provider_version=cast(str, value["provider_version"]),
        upstream_source=cast(str, value["upstream_source"]),
        retrieved_at=datetime.fromisoformat(cast(str, value["retrieved_at"])),
        error_kind=cast(str | None, value["error_kind"]),
        raw_payload=None if raw is None else base64.b64decode(cast(str, raw)),
        observations=tuple(
            source_observation_from_dict(item) for item in cast(list[object], value["observations"])
        ),
        raw_records=tuple(
            (key, base64.b64decode(raw)) for key, raw in cast(list[list[str]], value["raw_records"])
        ),
    )


def _continuation(value: dict[str, object]) -> ResearchContinuation:
    cutoff = value["successor_cutoff"]
    return ResearchContinuation(
        cast(str, value["request_id"]),
        cast(str, value["status"]),
        cast(str | None, value["snapshot_id"]),
        None if cutoff is None else datetime.fromisoformat(cast(str, cutoff)),
        cast(str | None, value["error_kind"]),
    )


def _parse_domain_date(value: str, date_format: str) -> datetime:
    try:
        parsed = datetime.strptime(value, date_format)
    except ValueError as exc:
        raise ResearchQueryValidationError(
            f"research dates must use their declared exact format: {date_format}"
        ) from exc
    if parsed.strftime(date_format) != value:
        raise ResearchQueryValidationError("research dates must use their declared exact format")
    return parsed
