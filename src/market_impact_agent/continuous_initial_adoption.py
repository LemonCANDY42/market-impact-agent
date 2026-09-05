"""Registered T0 reuse receipts; original native runs remain the only model provenance."""

from __future__ import annotations

import hmac
from dataclasses import asdict, replace
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, cast

from market_impact_agent.account_state import opaque_account_reference_hash
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    _PrivilegedEventSink,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.continuous_decision import ContinuousDecision, ReviewFrame
from market_impact_agent.continuous_study_runner import (
    continuous_study_scope,
    load_prepared_continuous_registration,
    study_budget,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import PortfolioOrderIntent

if TYPE_CHECKING:
    from market_impact_agent.continuous_portfolio_runtime import ContinuousPortfolioRuntime

# These are policy fields, explicitly enumerated so account/mandate IDs are never
# mistaken for economic equivalence and new risk fields require a deliberate review.
_POLICY_FIELDS = (
    "environment",
    "approval_mode",
    "valid_from",
    "valid_until",
    "allowed_instruments",
    "allowed_instrument_classes",
    "allowed_sides",
    "currency",
    "gross_exposure_limit",
    "minimum_net_exposure",
    "maximum_net_exposure",
    "maximum_position_count",
    "maximum_single_position_fraction",
    "daily_turnover_limit",
    "daily_submission_limit",
    "daily_loss_kill_threshold",
    "strategy_peak_drawdown_kill_threshold",
    "kill_on_unknown_ack",
    "kill_on_stale_account_snapshot",
    "kill_on_incomplete_order_coverage",
    "kill_on_reconciliation_difference",
    "kill_on_provider_loss",
    "execution_scope",
)


def initial_economic_contract(
    runtime: ContinuousPortfolioRuntime, frame: ReviewFrame
) -> dict[str, object]:
    """Exact, prior-close bootstrap contract; no post-T0 account branch is eligible."""
    prefix = runtime._account_prefix(frame)  # pyright: ignore[reportPrivateUsage]
    inputs = runtime._portfolio_inputs(frame)  # pyright: ignore[reportPrivateUsage]
    if len(prefix) != 1:
        raise PermissionError("initial adoption requires exactly the original bootstrap prefix")
    first = prefix[0]
    state = first.account_state
    if (
        len(first.fills) != 1
        or first.fills[0].order_id != "historical-opening-510300"
        or first.no_fills
        or state.open_orders != ()
        or not state.complete
        or state.missing_sections
        or state.reconciliation_gaps
        or first.fills[0].filled_at.date() >= frame.cutoff.date()
    ):
        raise PermissionError("initial adoption requires fully settled prior-session bootstrap")
    # Prefix fill quantities/dates prove identical T+1 eligibility, not just headline NAV.
    exposure = inputs.exposure_view
    mandate = inputs.mandate.to_dict()
    if set(mandate) != set(_POLICY_FIELDS) | {
        "schema_version",
        "mandate_id",
        "account_id",
        "harness_authority_id",
        "universe_binding_hash",
    }:
        raise PermissionError("initial adoption requires review of new mandate policy fields")
    _, market = runtime._frame_sources(frame)  # pyright: ignore[reportPrivateUsage]
    specs = {}
    for symbol in sorted(inputs.price_bases):
        spec = market.instrument_spec(symbol, frame.cutoff)
        if spec is None:
            raise PermissionError("initial adoption lacks effective execution rules")
        if symbol in runtime.account.specs and runtime.account.specs[symbol] != spec:
            raise PermissionError("initial adoption execution rules differ from frozen rules")
        specs[symbol] = asdict(spec)
    bootstrap = first.fills[0]
    seed_spec = market.instrument_spec(bootstrap.target_id, frame.cutoff)
    if (
        seed_spec is None
        or bootstrap.target_id != "510300.SH"
        or bootstrap.side.value != "buy"
        or bootstrap.quantity
        != (runtime.account.initial_cash / 2 / bootstrap.price // seed_spec.lot_size)
        * seed_spec.lot_size
    ):
        raise PermissionError("initial adoption is not the registered half-HS300 bootstrap")
    return cast(
        dict[str, object],
        _json(
            {
                "frame": frame.to_dict(),
                "initial_cash": runtime.account.initial_cash,
                "bootstrap_cash": first.cash,
                "bootstrap_nav": first.nav,
                "as_of": state.as_of,
                "reconciled_at": state.reconciled_at,
                "environment": state.environment.value,
                "provider_id": state.provider_id,
                "provider_version": state.provider_version,
                "provider_manifest_hash": state.provider_manifest_hash,
                "cash": [item.to_dict() for item in state.cash or ()],
                "positions": [item.to_dict() for item in state.positions or ()],
                "open_orders": [],
                "missing_sections": [],
                "reconciliation_gaps": [],
                "bootstrap_fills": [
                    {
                        "target_id": item.target_id,
                        "side": item.side.value,
                        "quantity": item.quantity,
                        "price": item.price,
                        "commission": item.commission,
                        "filled_at": item.filled_at,
                    }
                    for item in first.fills
                ],
                "recent_fills_since": state.recent_fills_since,
                "recent_fills": [
                    {
                        "order_reference": item.order_reference,
                        "target_id": item.target_id,
                        "venue": item.venue,
                        "instrument_class": item.instrument_class,
                        "side": item.side.value,
                        "quantity": item.quantity,
                        "filled_at": item.filled_at,
                    }
                    for item in state.recent_fills or ()
                ],
                "position_max_age_seconds": inputs.position_snapshot.max_age_seconds,
                "position_observation_gaps": list(inputs.position_snapshot.observation_gaps),
                "daily_turnover_used": exposure.daily_turnover_used,
                "daily_submissions_used": exposure.daily_submissions_used,
                "active_kill_reasons": list(exposure.active_kill_reasons),
                "policy": {name: mandate[name] for name in _POLICY_FIELDS},
                "prices": {key: value.to_dict() for key, value in inputs.price_bases.items()},
                "rule_set": inputs.rule_set.to_dict(),
                "execution_specs": specs,
                "profile": runtime.provider.profile.to_dict(),
                "runtime": runtime.provider.runtime_identity,
            }
        ),
    )


class InitialAdoptionAuthority:
    """Harness composition only; no Agent-facing constructor or authorization tool."""

    def __init__(
        self,
        *,
        study_root: Path,
        source_runtime: ContinuousPortfolioRuntime,
        coverage_window_id: str,
        profile_arm: str,
        cadence: str,
    ) -> None:
        if cadence == "coverage":
            raise ValueError("adoption destination must be a registered cadence arm")
        self.study_root = study_root
        self.source_runtime = source_runtime
        self.window_id, self.profile_arm, self.cadence = coverage_window_id, profile_arm, cadence
        self.registration = load_prepared_continuous_registration(study_root)
        self.budget = study_budget(study_root, "rolling")
        self.store = LocalDataSnapshotStore(self.budget.journal.path.parent)
        if self.store.index_path != self.budget.journal.path:
            raise PermissionError("study owner is outside its existing Harness store")
        key = (self.store.root / ".harness-event-hmac.key").read_bytes()
        self.events = _PrivilegedEventSink(
            journal=self.budget.journal,
            authority_id=self.store.harness_authority_id,
            signer=lambda value: hmac.new(key, value, sha256).hexdigest(),
        )

    def _registered(
        self, destination: ContinuousPortfolioRuntime, frame: ReviewFrame
    ) -> dict[str, object]:
        if load_prepared_continuous_registration(self.study_root) != self.registration:
            raise PermissionError("initial adoption registration changed")
        budget = study_budget(self.study_root, "rolling")
        if (
            budget.owner_run_id != self.budget.owner_run_id
            or budget.journal.path != self.budget.journal.path
        ):
            raise PermissionError("initial adoption study owner changed")
        registration_id = str(self.registration["registration_id"])
        windows = cast(list[dict[str, object]], self.registration["coverage_windows"])
        cells = cast(list[dict[str, object]], self.registration["deep_cells"])
        profiles = cast(list[dict[str, object]], self.registration["model_profiles"])
        matches = [item for item in windows if item["window_id"] == self.window_id]
        models = [item for item in profiles if item["arm"] == self.profile_arm]
        if (
            len(matches) != 1
            or len(models) != 1
            or not any(item["coverage_window_id"] == self.window_id for item in cells)
            or matches[0]["decision_session"] != frame.cutoff.date().isoformat()
        ):
            raise PermissionError("initial adoption is outside a registered deep-cell T0")
        source = self.source_runtime
        source_engine = cast(object, source.account.engine)  # pyright: ignore[reportUnknownMemberType]
        destination_engine = cast(
            object,
            destination.account.engine,  # pyright: ignore[reportUnknownMemberType]
        )
        if (
            source.account is destination.account
            or source_engine is destination_engine
            or source.account.account_id == destination.account.account_id
            or source.account.journal_path.resolve() == destination.account.journal_path.resolve()
        ):
            raise PermissionError(
                "initial adoption requires distinct source/destination account writers"
            )
        for runtime, cadence in ((source, "coverage"), (destination, self.cadence)):
            expected = continuous_study_scope(
                registration_id, self.window_id, self.profile_arm, cadence
            )
            provider_budget = runtime.provider.budget
            if (
                (runtime.experiment_id, runtime.arm_id) != expected
                or runtime.account.account_reference != expected[1]
                or not hmac.compare_digest(
                    runtime.account.account_reference_key, source.account.account_reference_key
                )
                or runtime.account.account_id
                != opaque_account_reference_hash(
                    expected[1], key=source.account.account_reference_key
                )
                or runtime.provider.profile.profile_hash != models[0]["provider_profile_hash"]
                or provider_budget is None
                or provider_budget.owner_run_id != budget.owner_run_id
                or provider_budget.journal.path != budget.journal.path
            ):
                raise PermissionError("initial adoption scope/profile/shared study budget mismatch")
        if source.store.root.resolve() != destination.store.root.resolve():
            raise PermissionError("initial adoption source must retain same-root native provenance")
        return {
            "registration_id": registration_id,
            "registration_hash": canonical_hash(self.registration),
            "coverage_window_id": self.window_id,
            "profile_arm": self.profile_arm,
            "cadence": self.cadence,
            "experiment_id": destination.experiment_id,
            "arm_id": destination.arm_id,
            "account_scope": destination.account.account_id,
            "episode_id": canonical_hash({"window": self.window_id, "frame": frame.to_dict()}),
            "frame": frame.to_dict(),
        }

    def _source_refs(self, decision: ContinuousDecision, frame: ReviewFrame) -> dict[str, object]:
        source = self.source_runtime
        if decision.initial_adoption_ref is not None:
            raise PermissionError("initial adoption cannot chain an already adopted decision")
        source.validate_decision(decision, frame)
        research = source.journal.get_run(decision.research_run_id)
        portfolio = source.journal.get_run(decision.portfolio_run_id)
        for record in (research, portfolio):
            binding = _object(source.store.artifacts.read_json(record.config_hash))
            if (
                binding.get("profile") != source.provider.profile.to_dict()
                or binding.get("runtime") != source.provider.runtime_identity
            ):
                raise PermissionError("initial adoption native source profile/runtime changed")
        return {
            "decision": decision.to_dict(),
            "research_binding_hash": research.config_hash,
            "research_terminal_hash": research.terminal_artifact_id,
            "portfolio_binding_hash": portfolio.config_hash,
            "portfolio_terminal_hash": portfolio.terminal_artifact_id,
            "source_account": source.account.account_id,
            "source_arm": source.arm_id,
        }

    def _signed_object(self, reference: str, suffix: str) -> dict[str, object]:
        value = _object(self.store.artifacts.read_json(reference))
        event = self.budget.journal.event("initial-adoption-" + reference + "." + suffix)
        if (
            event is None
            or event.run_id != self.budget.owner_run_id
            or event.event_type != "continuous.initial-adoption." + suffix
            or event.payload != {"artifact_hash": reference}
        ):
            raise PermissionError("initial adoption lacks registered owner signature")
        return value

    def _persist(self, value: dict[str, object], suffix: str, frame: ReviewFrame) -> str:
        ref = self.store.artifacts.put_json(value).content_hash
        self.events.append(
            run_id=self.budget.owner_run_id,
            event_id="initial-adoption-" + ref + "." + suffix,
            event_type="continuous.initial-adoption." + suffix,
            observed_at=frame.cutoff,
            payload={"artifact_hash": ref},
        )
        return ref

    def _contexts(
        self,
        destination: ContinuousPortfolioRuntime,
        source_decision: ContinuousDecision,
        frame: ReviewFrame,
    ) -> tuple[ContinuousPortfolioRuntime, ContinuousPortfolioRuntime, ReviewFrame]:
        source, effective = self.source_runtime.resolve_decision_context(source_decision, frame)
        if source_decision.research_successor_ref is None:
            return source, destination, effective
        repository, market = source._frame_sources(effective)  # pyright: ignore[reportPrivateUsage]
        rebound = destination._with_sources(  # pyright: ignore[reportPrivateUsage]
            repository, market, source.symbol_source(effective)
        )
        return source, rebound, effective

    def adopt(
        self,
        *,
        destination: ContinuousPortfolioRuntime,
        source_decision: ContinuousDecision,
        frame: ReviewFrame,
    ) -> ContinuousDecision:
        scope = self._registered(destination, frame)
        source_refs = self._source_refs(source_decision, frame)
        source_context, destination_context, effective = self._contexts(
            destination, source_decision, frame
        )
        economic = initial_economic_contract(source_context, effective)
        if initial_economic_contract(destination_context, effective) != economic:
            raise PermissionError("initial adoption account economics or policy differ")
        permission: dict[str, object] = {
            "schema_version": "market-impact.continuous-initial-adoption-permission.v1",
            "scope": scope,
            "source": source_refs,
            "economic_hash": canonical_hash(economic),
        }
        permission_ref = self._persist(permission, "authorized", frame)
        receipt, _ = self._derive(destination, source_decision, frame, permission_ref)
        ref = self._persist(receipt, "validated", frame)
        return replace(source_decision, initial_adoption_ref=ref)

    def _derive(
        self,
        destination: ContinuousPortfolioRuntime,
        source_decision: ContinuousDecision,
        frame: ReviewFrame,
        permission_ref: str,
    ) -> tuple[dict[str, object], PortfolioOrderIntent | None]:
        permission = self._signed_object(permission_ref, "authorized")
        scope = self._registered(destination, frame)
        source_refs = self._source_refs(source_decision, frame)
        source_context, destination_context, effective = self._contexts(
            destination, source_decision, frame
        )
        economic = initial_economic_contract(destination_context, effective)
        if (
            permission["scope"] != scope
            or permission["source"] != source_refs
            or permission["economic_hash"] != canonical_hash(economic)
            or initial_economic_contract(source_context, effective) != economic
        ):
            raise PermissionError(
                "initial adoption permission differs from exact source/destination"
            )
        authority = destination_context._portfolio_authority(effective)  # pyright: ignore[reportPrivateUsage]
        inputs = authority.input_source().to_dict()
        binding_hash = canonical_hash({"permission_ref": permission_ref, "inputs": inputs})
        original = source_context._portfolio_authority(effective).replay(  # pyright: ignore[reportPrivateUsage]
            source_decision.portfolio_run_id
        )
        decision, sizing, order = authority.derive_initial_adoption(
            parsed_proposal=original["parsed_proposal"],
            binding_hash=binding_hash,
            research_run_id=source_decision.research_run_id,
            source_portfolio_run_id=source_decision.portfolio_run_id,
        )
        source_orders = self.source_runtime.admitted_intents(source_decision, frame)
        if (order is None) != (not source_orders):
            raise PermissionError("adoption changes the original economic action")
        if order is not None:
            if len(source_orders) != 1 or _order_economics(order) != _order_economics(
                source_orders[0]
            ):
                raise PermissionError(
                    "adoption changes the original signed instrument/side/quantity"
                )
            destination_context._assert_opening_buy_bounds(order, effective)  # pyright: ignore[reportPrivateUsage]
        return {
            "schema_version": "market-impact.continuous-initial-adoption-receipt.v1",
            "permission_ref": permission_ref,
            "binding_hash": binding_hash,
            "inputs": inputs,
            "source_decision": source_decision.to_dict(),
            "frame": frame.to_dict(),
            "decision": decision.to_dict(),
            "sizing": None if sizing is None else sizing.to_dict(),
            "order": None if order is None else order.to_dict(),
        }, order

    def rotation_order(
        self, destination: ContinuousPortfolioRuntime, source_run_id: str
    ) -> PortfolioOrderIntent:
        """Resolve only this destination's signed adopted initial order."""
        matches: list[PortfolioOrderIntent] = []
        for event in self.budget.journal.events(self.budget.owner_run_id):
            if event.event_type != "continuous.initial-adoption.validated":
                continue
            reference = str(event.payload["artifact_hash"])
            receipt = self._signed_object(reference, "validated")
            permission = self._signed_object(str(receipt["permission_ref"]), "authorized")
            scope = _object(permission["scope"])
            if (
                scope.get("experiment_id") != destination.experiment_id
                or scope.get("arm_id") != destination.arm_id
                or scope.get("account_scope") != destination.account.account_id
                or _object(receipt["source_decision"]).get("portfolio_run_id") != source_run_id
            ):
                continue
            _, order = self.reopen(reference, destination)
            if order is not None:
                matches.append(order)
        if len(matches) != 1:
            raise PermissionError("rotation source has no unique adopted destination order")
        return matches[0]

    def recall_source(
        self, destination: ContinuousPortfolioRuntime, cutoff: datetime
    ) -> str | None:
        """Recover only this destination's registered initial source after restart."""
        candidates: set[str] = set()
        for event in self.budget.journal.events(self.budget.owner_run_id):
            if event.event_type != "continuous.initial-adoption.validated":
                continue
            reference = str(event.payload["artifact_hash"])
            receipt = self._signed_object(reference, "validated")
            permission = self._signed_object(str(receipt["permission_ref"]), "authorized")
            scope = _object(permission["scope"])
            if (
                scope.get("experiment_id") != destination.experiment_id
                or scope.get("arm_id") != destination.arm_id
                or scope.get("account_scope") != destination.account.account_id
            ):
                continue
            initial_frame = _frame(_object(receipt["frame"]))
            if initial_frame.cutoff >= cutoff:
                continue
            reopened, _ = self.reopen(reference, destination)
            candidates.add(str(_object(reopened["source_decision"])["research_run_id"]))
        if len(candidates) > 1:
            raise PermissionError("initial adoption has ambiguous source research permissions")
        return next(iter(candidates), None)

    def reopen(
        self,
        reference: str,
        destination: ContinuousPortfolioRuntime,
        *,
        frame: ReviewFrame | None = None,
    ) -> tuple[dict[str, object], PortfolioOrderIntent | None]:
        receipt = self._signed_object(reference, "validated")
        initial_frame = _frame(_object(receipt["frame"]))
        if frame is not None and frame != initial_frame:
            raise PermissionError("initial adoption receipt is valid only at registered T0")
        source = ContinuousDecision.from_dict(_object(receipt["source_decision"]))
        derived, order = self._derive(
            destination, source, initial_frame, str(receipt["permission_ref"])
        )
        if receipt != derived:
            raise PermissionError("initial adoption receipt differs from deterministic replay")
        return receipt, order


def _order_economics(order: object) -> object:
    from market_impact_agent.domain import ExecutableOrder

    value = cast(ExecutableOrder, order)
    return value.instrument_id, value.side, value.quantity, value.order_kind, value.expires_at


def _frame(value: dict[str, object]) -> ReviewFrame:
    return ReviewFrame(
        datetime.fromisoformat(str(value["cutoff"])),
        tuple(cast(list[str], value["snapshot_ids"])),
        str(value["input_hash"]),
        tuple(cast(list[str], value["new_fact_ids"])),
        tuple(cast(list[str], value["gaps"])),
    )


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("initial adoption requires frozen object")
    return cast(dict[str, object], value)


def _json(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in cast(dict[object, object], value).items()}
    if isinstance(value, (tuple, list)):
        return [_json(item) for item in cast(tuple[object, ...], value)]
    return value
