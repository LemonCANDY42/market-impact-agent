"""Fixed-denominator, zero-model continuous-study baseline measurements.

The registered CSI 300 panel supplies the calendar only.  Executable baseline
fills come exclusively from source-backed raw 510300.SH bars reopened through
``HistoricalAShareInputs`` and are settled by ``HistoricalStreamingAccount``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from typing import Literal, cast

from market_impact_agent.account_state import opaque_account_reference_hash
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.continuous_metrics import measure_continuous_account
from market_impact_agent.continuous_study import ContinuousStudyRegistration, ContinuousStudyWindow
from market_impact_agent.domain import OrderIntent, OrderKind, Side, TradingEnvironment
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    HistoricalSessionInputs,
)
from market_impact_agent.market_regimes import RegimePanel, RegimeSeries, ValidatedRegimePanel
from market_impact_agent.streaming_nautilus_account import (
    HistoricalInstrumentSpec,
    HistoricalSessionResult,
    HistoricalStreamingAccount,
)

CONTINUOUS_BASELINE_REPORT_SCHEMA = "market-impact.continuous-baseline-report.v1"
_RAW_BROAD_ETF_SYMBOL = "510300.SH"
_INITIAL_CASH = Decimal("100000")
_MOMENTUM_BASELINE_ID = "phase2_adjusted_close_momentum_510300"
CONTINUOUS_EXECUTABLE_BASELINE_IDS = (
    "cash_no_action",
    "same_initial_account_hold",
    "broad_etf_hold",
    _MOMENTUM_BASELINE_ID,
)
_BASELINE_IDS = CONTINUOUS_EXECUTABLE_BASELINE_IDS
_MOMENTUM_BINDING_CORE = {
    "binding_version": "continuous-phase2-adjusted-close-momentum-510300.v1",
    "phase2_calculation_ref": "market_impact_agent.phase2_study._momentum_action",
    "phase2_strategy_ref": "three-session-adjusted-close-momentum-long-or-abstain.v1",
    "signal_input": "four source-bound adjusted closes ending before each session open",
    "direction_rule": (
        "buy when latest adjusted close is strictly greater than the close three open "
        "sessions earlier"
    ),
    "nonpositive_phase2_action": "abstain",
    "continuous_abstain_mapping": "target_cash_by_selling_existing_510300_at_next_eligible_open",
    "positive_mapping": "target_maximum_affordable_510300_whole_lots_at_next_eligible_open",
    "buy_sizing_basis": "cutoff_known_raw_reference_and_effective_upper_limit_with_fee_ceiling",
    "execution_price_basis": "raw_unadjusted",
    "outcomes_visible_to_rule": False,
}
_MOMENTUM_BINDING = {
    **_MOMENTUM_BINDING_CORE,
    "binding_id": "continuous-momentum-binding-" + canonical_hash(_MOMENTUM_BINDING_CORE),
}


@dataclass(frozen=True, slots=True)
class ContinuousBaselineAccountSeed:
    """Caller-owned pseudonymous account seed; its secret key is never reported."""

    account_reference: str
    account_reference_key: bytes
    initial_cash: Decimal = _INITIAL_CASH

    def __post_init__(self) -> None:
        if not self.account_reference or self.account_reference != self.account_reference.strip():
            raise ValueError("baseline account reference must be non-empty trimmed text")
        if len(self.account_reference_key) < 32:
            raise ValueError("baseline account reference key must contain at least 32 bytes")
        if self.initial_cash != _INITIAL_CASH:
            raise ValueError("continuous baseline initial cash is fixed at CNY 100000")


@dataclass(frozen=True, slots=True)
class ContinuousBaselineWindow:
    """One registered window and its complete, panel-derived trading calendar."""

    window: ContinuousStudyWindow
    sessions: tuple[date, ...]

    def __post_init__(self) -> None:
        if not self.sessions:
            raise ValueError("registered baseline window needs at least one calendar session")
        if self.sessions[0] != self.window.decision_session:
            raise ValueError("baseline calendar must begin on the registered decision session")
        if self.sessions[-1] != self.window.outcome_window_end:
            raise ValueError("baseline calendar must end on the registered outcome session")
        if tuple(sorted(set(self.sessions))) != self.sessions:
            raise ValueError("baseline calendar sessions must be unique and chronological")


@dataclass(frozen=True, slots=True)
class _MomentumSignal:
    """A source-bound Phase 2 direction calculation for one executable open."""

    session: date
    cutoff: str
    action: Literal["buy", "cash"]
    adjusted_closes: tuple[str, ...]
    source_record_hashes: tuple[str, ...]
    factor_record_hashes: tuple[str, ...]

    def report(self) -> dict[str, object]:
        return {
            "session": self.session.isoformat(),
            "cutoff": self.cutoff,
            "phase2_action": "buy" if self.action == "buy" else "abstain",
            "continuous_target": "maximum_affordable_long" if self.action == "buy" else "cash",
            "adjusted_closes": list(self.adjusted_closes),
            "source_record_hashes": list(self.source_record_hashes),
            "factor_record_hashes": list(self.factor_record_hashes),
        }


@dataclass(frozen=True, slots=True)
class _PreopenBuySizing:
    """The conservative buy ceiling visible before an executable session open."""

    session: date
    cutoff: str
    raw_reference_price: Decimal
    upper_limit: Decimal
    source_record_hashes: tuple[str, ...]

    def report(self) -> dict[str, object]:
        return {
            "session": self.session.isoformat(),
            "cutoff": self.cutoff,
            "raw_reference_price": str(self.raw_reference_price),
            "maximum_buy_price": str(self.upper_limit),
            "source_record_hashes": list(self.source_record_hashes),
        }


@dataclass(frozen=True, slots=True)
class _BaselineDefinition:
    baseline_id: str
    description: str


_DEFINITIONS = (
    _BaselineDefinition(
        "cash_no_action",
        "Sell the identical overnight half-510300 seed at the first registered session "
        "and hold cash.",
    ),
    _BaselineDefinition(
        "same_initial_account_hold",
        "Retain the identical overnight half-510300 seed and make no later order.",
    ),
    _BaselineDefinition(
        "broad_etf_hold",
        "Buy additional source-backed 510300 at the first registered session to the "
        "maximum affordable whole-lot long allocation.",
    ),
    _BaselineDefinition(
        _MOMENTUM_BASELINE_ID,
        "Apply the Phase 2 four-adjusted-close direction rule before each session; buy targets "
        "maximum affordable 510300 long exposure and abstain targets cash in this new binding.",
    ),
)


def registered_baseline_windows(
    registration: ContinuousStudyRegistration,
    selection_panel: ValidatedRegimePanel | RegimePanel,
) -> tuple[ContinuousBaselineWindow, ...]:
    """Derive every registered full daily calendar without using panel prices as fills."""

    panel = _resolve_selection_panel(registration, selection_panel)
    calendar = _primary_calendar(panel)
    result: list[ContinuousBaselineWindow] = []
    for window in registration.coverage_windows:
        try:
            start = calendar.index(window.decision_session)
            end = calendar.index(window.outcome_window_end)
        except ValueError as exc:
            raise ValueError(
                f"selection panel is missing registered baseline endpoint for {window.window_id}"
            ) from exc
        if end < start:
            raise ValueError(f"registered baseline window {window.window_id} has reversed calendar")
        result.append(ContinuousBaselineWindow(window, calendar[start : end + 1]))
    return tuple(result)


def evaluate_continuous_baselines(
    registration: ContinuousStudyRegistration,
    selection_panel: ValidatedRegimePanel | RegimePanel,
    *,
    historical_inputs: HistoricalAShareInputs,
    account_seed: ContinuousBaselineAccountSeed,
    state_root: Path,
) -> dict[str, object]:
    """Measure the four executable fixed baselines over all 18 registered windows.

    This function makes no model, network, provider, or broker call.  A missing
    daily input remains in its registered window and produces typed incompleteness.
    """

    windows = registered_baseline_windows(registration, selection_panel)
    source_binding_hash = _source_binding_hash(historical_inputs)
    required_sessions = {
        session
        for item in windows
        for session in (item.window.observation_through_session, *item.sessions)
    }
    source_sessions = {
        session: historical_inputs.session(_RAW_BROAD_ETF_SYMBOL, session)
        for session in required_sessions
    }
    output: list[dict[str, object]] = []
    for definition in _DEFINITIONS:
        output.append(
            {
                "baseline_id": definition.baseline_id,
                "description": definition.description,
                "execution_eligible": True,
                "instrument": _RAW_BROAD_ETF_SYMBOL,
                "binding": _MOMENTUM_BINDING
                if definition.baseline_id == _MOMENTUM_BASELINE_ID
                else None,
                "windows": [
                    evaluate_continuous_baseline_window(
                        registration_id=registration.registration_id,
                        baseline_id=definition.baseline_id,
                        registered_window=item,
                        historical_inputs=historical_inputs,
                        account_seed=account_seed,
                        state_root=state_root,
                        source_binding_hash=source_binding_hash,
                        source_sessions=source_sessions,
                    )
                    for item in windows
                ],
            }
        )
    output.extend(_unsupported_rule_baselines())
    executable = output[: len(_DEFINITIONS)]
    return {
        "schema_version": CONTINUOUS_BASELINE_REPORT_SCHEMA,
        "registration_id": registration.registration_id,
        "coverage_denominator": len(windows),
        "baseline_ids": list(_BASELINE_IDS),
        "calendar_source": {
            "role": "registered_daily_calendar_only",
            "selection_panel_id": registration.selection_panel_id,
            "index_prices_used_as_execution_prices": False,
        },
        "execution_source": {
            "symbol": _RAW_BROAD_ETF_SYMBOL,
            "price_basis": "raw_unadjusted",
            "modeled_policy_id": historical_inputs.policy.policy_id,
            "source_binding_hash": source_binding_hash,
        },
        "initial_account": {
            "currency": "CNY",
            "initial_cash": str(account_seed.initial_cash),
            "opening_allocation": (
                "approximately_half_510300_overnight_via_historical_streaming_account"
            ),
            "fees_and_corporate_actions": "HistoricalStreamingAccount",
            "account_reference_reported": False,
        },
        "baselines": output,
        "executable_baselines_complete": all(
            window["status"] == "complete"
            for baseline in executable
            for window in _objects(baseline["windows"], "baseline windows")
        ),
        "model_or_network_invocation": False,
        "broker_access": False,
        "investment_effectiveness_accepted": False,
    }


def evaluate_continuous_baseline_window(
    *,
    registration_id: str,
    baseline_id: str,
    registered_window: ContinuousBaselineWindow,
    historical_inputs: HistoricalAShareInputs,
    account_seed: ContinuousBaselineAccountSeed,
    state_root: Path,
    source_binding_hash: str | None = None,
    source_sessions: Mapping[date, HistoricalSessionInputs] | None = None,
) -> dict[str, object]:
    """Run one baseline window without dropping any calendar session after a gap."""

    if baseline_id not in _BASELINE_IDS:
        raise ValueError("unregistered continuous baseline id")
    if not registration_id or registration_id != registration_id.strip():
        raise ValueError("baseline registration id must be non-empty trimmed text")
    actual_source_hash = _source_binding_hash(historical_inputs)
    if source_binding_hash is not None and source_binding_hash != actual_source_hash:
        raise ValueError("baseline source identity differs from its exact frozen evidence")
    source_binding_hash = actual_source_hash
    if len(source_binding_hash) != 64:
        raise ValueError("baseline source binding must be a sha256 hash")

    def reopen(session: date) -> HistoricalSessionInputs:
        if source_sessions is None:
            return historical_inputs.session(_RAW_BROAD_ETF_SYMBOL, session)
        try:
            return source_sessions[session]
        except KeyError as exc:
            raise ValueError("baseline source-session cache misses a registered session") from exc

    inputs_by_day = {session: reopen(session) for session in registered_window.sessions}
    seed_inputs = reopen(registered_window.window.observation_through_session)
    input_gaps = _input_gaps(
        registered_window, seed_inputs=seed_inputs, inputs_by_day=inputs_by_day
    )
    momentum_signals: Mapping[date, _MomentumSignal] = {}
    if baseline_id == _MOMENTUM_BASELINE_ID:
        momentum_signals, momentum_gaps = _momentum_signals(
            historical_inputs=historical_inputs,
            inputs_by_day=inputs_by_day,
        )
        input_gaps.extend(momentum_gaps)
    buy_sizing_sessions = (
        tuple(session for session, signal in momentum_signals.items() if signal.action == "buy")
        if baseline_id == _MOMENTUM_BASELINE_ID
        else (registered_window.sessions[0],)
        if baseline_id == "broad_etf_hold"
        else ()
    )
    preopen_buy_sizing, sizing_gaps = _preopen_buy_sizing(
        historical_inputs=historical_inputs,
        inputs_by_day=inputs_by_day,
        sessions=buy_sizing_sessions,
    )
    input_gaps.extend(sizing_gaps)
    initial_account_hash = _initial_account_hash(account_seed, seed_inputs)
    common = _window_common(
        registered_window,
        baseline_id=baseline_id,
        source_binding_hash=source_binding_hash,
        input_gaps=input_gaps,
    )
    if not _seed_ready(seed_inputs):
        return {
            **common,
            "status": "incomplete_source_inputs",
            "initial_account_hash": initial_account_hash,
            "metrics": None,
            "execution_target": _execution_target(baseline_id),
            **_momentum_report_fields(baseline_id, momentum_signals),
            **_buy_sizing_report_fields(preopen_buy_sizing),
        }

    assert seed_inputs.spec is not None and seed_inputs.bar is not None
    account = HistoricalStreamingAccount(
        specs=(seed_inputs.spec,),
        journal_path=_journal_path(
            state_root,
            registration_id=registration_id,
            baseline_id=baseline_id,
            window_id=registered_window.window.window_id,
            source_binding_hash=source_binding_hash,
        ),
        account_reference=account_seed.account_reference,
        account_reference_key=account_seed.account_reference_key,
        initial_cash=account_seed.initial_cash,
    )
    measurement_initial_nav = account.initial_cash
    try:
        if not account.results:
            account.bootstrap_half_hs300(seed_inputs.bar)
        seeded = account.results[0]
        measurement_initial_nav = seeded.nav
        initial_account_hash = canonical_hash(
            {
                "account_id": account.account_id,
                "seed_result_hash": seeded.result_hash,
                "initial_cash": str(account_seed.initial_cash),
                "seed_symbol": _RAW_BROAD_ETF_SYMBOL,
            }
        )
        session_results, execution_gaps = _advance_registered_window(
            account=account,
            baseline_id=baseline_id,
            registered_window=registered_window,
            inputs_by_day=inputs_by_day,
            momentum_signals=momentum_signals,
            preopen_buy_sizing=preopen_buy_sizing,
        )
        # The opening seed is prior-session state, not an outcome observation.
        measured = tuple(session_results)
        metrics = measure_continuous_account(
            initial_nav=measurement_initial_nav,
            sessions=measured,
            expected_sessions=len(registered_window.sessions),
            execution_policy_hash=_execution_policy_hash(baseline_id),
            initial_account_hash=initial_account_hash,
            model_cost_microusd=0,
        )
        source_incomplete = bool(input_gaps)
        execution_incomplete = bool(execution_gaps) or not _target_reached(
            baseline_id,
            account.results[-1],
            momentum_signal=momentum_signals.get(registered_window.sessions[-1]),
        )
        status = (
            "incomplete_source_inputs"
            if source_incomplete
            else "incomplete_execution"
            if execution_incomplete
            else "complete"
        )
        return {
            **common,
            "status": status,
            "initial_account_hash": initial_account_hash,
            "metrics": metrics,
            "execution_target": _execution_target(baseline_id),
            "execution_gaps": execution_gaps,
            **_momentum_report_fields(baseline_id, momentum_signals),
            **_buy_sizing_report_fields(preopen_buy_sizing),
        }
    except (RuntimeError, ValueError) as exc:
        # A failed engine prefix is unusable.  Preserve the full registered denominator
        # and let a fresh, separately repaired source binding run in another journal.
        observed = _window_results(account, registered_window)
        metrics = measure_continuous_account(
            initial_nav=measurement_initial_nav,
            sessions=observed,
            expected_sessions=len(registered_window.sessions),
            execution_policy_hash=_execution_policy_hash(baseline_id),
            initial_account_hash=initial_account_hash,
            model_cost_microusd=0,
        )
        return {
            **common,
            "status": "incomplete_execution",
            "initial_account_hash": initial_account_hash,
            "metrics": metrics,
            "execution_target": _execution_target(baseline_id),
            "execution_gaps": [
                {"gap_id": "historical_streaming_account_error", "detail": str(exc)}
            ],
            **_momentum_report_fields(baseline_id, momentum_signals),
            **_buy_sizing_report_fields(preopen_buy_sizing),
        }
    finally:
        account.close()


def evaluate_raw_index_research_baseline(
    registration: ContinuousStudyRegistration,
    selection_panel: ValidatedRegimePanel | RegimePanel,
) -> dict[str, object]:
    """Return a raw-panel long price diagnostic, expressly outside execution evidence."""

    panel = _resolve_selection_panel(registration, selection_panel)
    primary = _primary_series(panel)
    rows = {_row_date(row): row for row in primary.rows}
    windows = registered_baseline_windows(registration, panel)
    result: list[dict[str, object]] = []
    for item in windows:
        window_rows = [rows[day] for day in item.sessions if day in rows]
        if len(window_rows) != len(item.sessions):
            result.append(
                {
                    "window_id": item.window.window_id,
                    "status": "incomplete_price_panel",
                    "expected_sessions": len(item.sessions),
                    "observed_sessions": len(window_rows),
                    "long_raw_index_return": None,
                }
            )
            continue
        try:
            first_close = _positive_decimal(window_rows[0], "close")
            last_close = _positive_decimal(window_rows[-1], "close")
        except ValueError:
            result.append(
                {
                    "window_id": item.window.window_id,
                    "status": "invalid_price_panel",
                    "expected_sessions": len(item.sessions),
                    "observed_sessions": len(window_rows),
                    "long_raw_index_return": None,
                }
            )
            continue
        result.append(
            {
                "window_id": item.window.window_id,
                "status": "research_only_complete",
                "expected_sessions": len(item.sessions),
                "observed_sessions": len(window_rows),
                "long_raw_index_return": str(last_close / first_close - 1),
            }
        )
    return {
        "schema_version": "market-impact.continuous-raw-index-research-baseline.v1",
        "registration_id": registration.registration_id,
        "baseline_id": "raw_index_long_price_research",
        "series_id": primary.series_id,
        "tushare_code": primary.tushare_code,
        "return_basis": primary.return_basis,
        "execution_eligible": False,
        "non_executable_reason": "regime_panel_index_prices_are_not_execution_prices",
        "coverage_denominator": len(windows),
        "windows": result,
        "investment_effectiveness_accepted": False,
    }


def _advance_registered_window(
    *,
    account: HistoricalStreamingAccount,
    baseline_id: str,
    registered_window: ContinuousBaselineWindow,
    inputs_by_day: Mapping[date, HistoricalSessionInputs],
    momentum_signals: Mapping[date, _MomentumSignal],
    preopen_buy_sizing: Mapping[date, _PreopenBuySizing],
) -> tuple[tuple[HistoricalSessionResult, ...], list[dict[str, object]]]:
    results: list[HistoricalSessionResult] = []
    initial_spec = account.specs[_RAW_BROAD_ETF_SYMBOL]
    prior_results = _window_results(account, registered_window)
    gaps = _persisted_execution_gaps(baseline_id, prior_results)
    results.extend(prior_results)
    next_index = len(results)
    for session in registered_window.sessions[next_index:]:
        inputs = inputs_by_day[session]
        if not _session_ready(inputs, initial_spec):
            gaps.append(
                {
                    "gap_id": "daily_execution_input_unavailable",
                    "session": session.isoformat(),
                    "gaps": _gaps(inputs, initial_spec),
                }
            )
            break
        assert inputs.bar is not None
        if baseline_id == _MOMENTUM_BASELINE_ID:
            signal = momentum_signals.get(session)
            if signal is None:
                gaps.append(
                    {
                        "gap_id": "continuous_momentum_signal_unavailable",
                        "session": session.isoformat(),
                    }
                )
                break
            sizing = preopen_buy_sizing.get(session) if signal.action == "buy" else None
            if signal.action == "buy" and sizing is None:
                gaps.append(
                    {
                        "gap_id": "continuous_momentum_buy_sizing_unavailable",
                        "session": session.isoformat(),
                    }
                )
                break
            intents = _momentum_intents(
                account,
                inputs,
                signal=signal,
                buy_sizing=sizing,
                window_id=registered_window.window.window_id,
            )
        else:
            intents = _first_session_intents(
                baseline_id,
                account,
                inputs,
                buy_sizing=preopen_buy_sizing.get(session),
                is_first_window_session=session == registered_window.sessions[0],
                window_id=registered_window.window.window_id,
            )
        result = account.advance_session(
            {_RAW_BROAD_ETF_SYMBOL: inputs.bar},
            intents=intents,
            corporate_actions=inputs.corporate_actions,
        )
        gaps.extend(_unfilled_intent_gaps(baseline_id, session, intents, result))
        if (
            not intents
            and baseline_id == "broad_etf_hold"
            and session == registered_window.sessions[0]
        ):
            gaps.append(
                {
                    "gap_id": "broad_etf_additional_lot_unaffordable",
                    "session": session.isoformat(),
                }
            )
        results.append(result)
    return tuple(results), gaps


def _persisted_execution_gaps(
    baseline_id: str, results: Sequence[HistoricalSessionResult]
) -> list[dict[str, object]]:
    """Keep a durable account-prefix failure from becoming acceptable on replay."""

    gap_id = (
        "continuous_momentum_target_unfilled_or_partial"
        if baseline_id == _MOMENTUM_BASELINE_ID
        else "first_session_baseline_target_unfilled_or_partial"
    )
    gaps: list[dict[str, object]] = []
    for result in results:
        for no_fill in result.no_fills:
            if not no_fill.order_id.startswith("continuous-baseline-"):
                continue
            gaps.append(
                {
                    "gap_id": gap_id,
                    "session": result.account_state.as_of.date().isoformat(),
                    "baseline_order_id": no_fill.order_id,
                    "no_fill_reasons": [no_fill.reason],
                    "reconstructed_from_persisted_result": True,
                }
            )
    return gaps


def _unfilled_intent_gaps(
    baseline_id: str,
    session: date,
    intents: Sequence[OrderIntent],
    result: HistoricalSessionResult,
) -> list[dict[str, object]]:
    gap_id = (
        "continuous_momentum_target_unfilled_or_partial"
        if baseline_id == _MOMENTUM_BASELINE_ID
        else "first_session_baseline_target_unfilled_or_partial"
    )
    gaps: list[dict[str, object]] = []
    for requested in intents:
        filled = sum(
            (item.quantity for item in result.fills if item.order_id == requested.client_order_id),
            start=Decimal(0),
        )
        if filled != requested.quantity:
            gaps.append(
                {
                    "gap_id": gap_id,
                    "session": session.isoformat(),
                    "baseline_order_id": requested.client_order_id,
                    "requested_quantity": str(requested.quantity),
                    "filled_quantity": str(filled),
                    "no_fill_reasons": [item.reason for item in result.no_fills],
                }
            )
    return gaps


def _first_session_intents(
    baseline_id: str,
    account: HistoricalStreamingAccount,
    inputs: HistoricalSessionInputs,
    *,
    buy_sizing: _PreopenBuySizing | None,
    is_first_window_session: bool,
    window_id: str,
) -> tuple[OrderIntent, ...]:
    if not is_first_window_session or len(account.results) > 1:
        return ()
    assert inputs.bar is not None
    quantity = account.results[0].positions.get(_RAW_BROAD_ETF_SYMBOL, Decimal(0))
    if baseline_id == "cash_no_action":
        return (_intent(account, window_id, inputs, Side.SELL, quantity, "cash-exit"),)
    if baseline_id == "broad_etf_hold":
        if buy_sizing is None:
            raise ValueError("broad ETF buy has no cutoff-known sizing proof")
        purchase = _maximum_affordable_lot(account, inputs, buy_sizing)
        return (
            ()
            if purchase == 0
            else (_intent(account, window_id, inputs, Side.BUY, purchase, "full-buy"),)
        )
    return ()


def _momentum_intents(
    account: HistoricalStreamingAccount,
    inputs: HistoricalSessionInputs,
    *,
    signal: _MomentumSignal,
    buy_sizing: _PreopenBuySizing | None,
    window_id: str,
) -> tuple[OrderIntent, ...]:
    """Map the existing Phase 2 action into this binding's account-state target."""

    if signal.action == "cash":
        quantity = account.results[-1].positions.get(_RAW_BROAD_ETF_SYMBOL, Decimal(0))
        return (
            ()
            if quantity == 0
            else (
                _intent(
                    account,
                    window_id,
                    inputs,
                    Side.SELL,
                    quantity,
                    f"momentum-cash-{signal.session:%Y%m%d}",
                ),
            )
        )
    if buy_sizing is None:
        raise ValueError("momentum buy has no cutoff-known sizing proof")
    purchase = _maximum_affordable_lot(account, inputs, buy_sizing)
    return (
        ()
        if purchase == 0
        else (
            _intent(
                account,
                window_id,
                inputs,
                Side.BUY,
                purchase,
                f"momentum-buy-{signal.session:%Y%m%d}",
            ),
        )
    )


def _intent(
    account: HistoricalStreamingAccount,
    window_id: str,
    inputs: HistoricalSessionInputs,
    side: Side,
    quantity: Decimal,
    action: str,
) -> OrderIntent:
    if quantity <= 0:
        raise ValueError("baseline order needs a positive source-backed quantity")
    assert inputs.bar is not None
    return OrderIntent(
        client_order_id=f"continuous-baseline-{window_id}-{action}",
        signal_id=f"continuous-baseline-{action}",
        account_id=account.account_id,
        environment=TradingEnvironment.BACKTEST,
        instrument_id=_RAW_BROAD_ETF_SYMBOL,
        side=side,
        quantity=quantity,
        order_kind=OrderKind.MARKET,
        created_at=inputs.bar.session_open_at - timedelta(microseconds=1),
        expires_at=inputs.bar.session_close_at,
    )


def _maximum_affordable_lot(
    account: HistoricalStreamingAccount,
    inputs: HistoricalSessionInputs,
    sizing: _PreopenBuySizing,
) -> Decimal:
    """Size at the cutoff-known upper limit, never the future executable open."""

    assert inputs.spec is not None
    spec = inputs.spec
    cash = account.results[-1].cash
    price = sizing.upper_limit
    lots = int(cash / price // spec.lot_size)
    while lots > 0:
        quantity = Decimal(lots * spec.lot_size)
        notional = quantity * price
        commission = max(spec.minimum_commission, notional * spec.commission_rate).quantize(
            Decimal("0.01"), rounding=ROUND_CEILING
        )
        if notional + commission <= cash:
            return quantity
        lots -= 1
    return Decimal(0)


def _preopen_buy_sizing(
    *,
    historical_inputs: HistoricalAShareInputs,
    inputs_by_day: Mapping[date, HistoricalSessionInputs],
    sessions: Sequence[date],
) -> tuple[Mapping[date, _PreopenBuySizing], list[dict[str, object]]]:
    sizing: dict[date, _PreopenBuySizing] = {}
    gaps: list[dict[str, object]] = []
    for session in sessions:
        inputs = inputs_by_day[session]
        if inputs.bar is None:
            gaps.append(
                {
                    "gap_id": "continuous_preopen_buy_sizing_unavailable",
                    "session": session.isoformat(),
                    "detail": "session open is unavailable for the cutoff-bound source read",
                }
            )
            continue
        cutoff = inputs.bar.session_open_at - timedelta(microseconds=1)
        try:
            evidence = historical_inputs.reopen_security(_RAW_BROAD_ETF_SYMBOL, cutoff)
            if (
                evidence is None
                or evidence.gaps
                or evidence.raw_price is None
                or evidence.raw_price_observed_at is None
                or evidence.raw_price_observed_at > cutoff
                or evidence.upper_limit is None
                or not evidence.source_record_hashes
            ):
                raise ValueError(
                    "effective raw reference, upper limit, or source proof is unavailable"
                )
            sizing[session] = _PreopenBuySizing(
                session=session,
                cutoff=cutoff.isoformat(),
                raw_reference_price=evidence.raw_price,
                upper_limit=evidence.upper_limit,
                source_record_hashes=evidence.source_record_hashes,
            )
        except (KeyError, TypeError, ValueError) as exc:
            gaps.append(
                {
                    "gap_id": "continuous_preopen_buy_sizing_unavailable",
                    "session": session.isoformat(),
                    "cutoff": cutoff.isoformat(),
                    "detail": str(exc),
                }
            )
    return sizing, gaps


def _buy_sizing_report_fields(
    preopen_buy_sizing: Mapping[date, _PreopenBuySizing],
) -> dict[str, object]:
    return {"preopen_buy_sizing": [item.report() for _, item in sorted(preopen_buy_sizing.items())]}


def _target_reached(
    baseline_id: str,
    result: HistoricalSessionResult,
    *,
    momentum_signal: _MomentumSignal | None,
) -> bool:
    position = result.positions.get(_RAW_BROAD_ETF_SYMBOL, Decimal(0))
    if baseline_id == "cash_no_action":
        return position == 0
    if baseline_id == "same_initial_account_hold":
        return position > 0
    if baseline_id == _MOMENTUM_BASELINE_ID:
        return momentum_signal is not None and (
            position > 0 if momentum_signal.action == "buy" else position == 0
        )
    return position > 0


def _momentum_calendar_continuity_gap(
    historical_inputs: HistoricalAShareInputs,
    *,
    session_open_at: datetime,
    current_session: date,
    prior_sessions: Sequence[date],
) -> dict[str, object] | None:
    """Prove the adjusted rows are consecutive through source ``pretrade_date`` links."""

    if len(prior_sessions) != 4:
        raise ValueError("momentum calendar continuity requires four prior sessions")
    current_sessions = (*prior_sessions[1:], current_session)
    for current, expected_prior in zip(current_sessions, prior_sessions, strict=True):
        cutoff = session_open_at.replace(
            year=current.year,
            month=current.month,
            day=current.day,
        ) - timedelta(microseconds=1)
        evidence = historical_inputs.reopen_security(_RAW_BROAD_ETF_SYMBOL, cutoff)
        if evidence is None:
            return {
                "transition_session": current.isoformat(),
                "expected_prior_session": expected_prior.isoformat(),
                "detail": "authoritative trading-calendar proof is unavailable",
            }
        observed_prior = (
            None
            if evidence.raw_price_observed_at is None
            else evidence.raw_price_observed_at.date()
        )
        calendar_gaps = {
            "prior_completed_raw_close_missing",
            "trading_calendar_session_unverified",
            "prior_close_not_previous_trading_session",
        }.intersection(evidence.gaps)
        if observed_prior != expected_prior or calendar_gaps:
            return {
                "transition_session": current.isoformat(),
                "expected_prior_session": expected_prior.isoformat(),
                "observed_prior_session": None
                if observed_prior is None
                else observed_prior.isoformat(),
                "calendar_gaps": sorted(calendar_gaps),
            }
    return None


def _momentum_signals(
    *,
    historical_inputs: HistoricalAShareInputs,
    inputs_by_day: Mapping[date, HistoricalSessionInputs],
) -> tuple[Mapping[date, _MomentumSignal], list[dict[str, object]]]:
    """Reopen the exact four-close Phase 2 calculation before each executable open."""

    signals: dict[date, _MomentumSignal] = {}
    gaps: list[dict[str, object]] = []
    for session, inputs in sorted(inputs_by_day.items()):
        if inputs.bar is None:
            gaps.append(
                {
                    "gap_id": "continuous_momentum_signal_input_unavailable",
                    "session": session.isoformat(),
                    "detail": "session raw bar is required to establish the pre-open cutoff",
                }
            )
            continue
        cutoff = inputs.bar.session_open_at - timedelta(microseconds=1)
        try:
            series = historical_inputs.research_series(_RAW_BROAD_ETF_SYMBOL, cutoff, limit=4)
            rows = _objects(series.get("rows"), "continuous momentum source rows")
            raw_series_gaps = series.get("gaps")
            if not isinstance(raw_series_gaps, list):
                raise ValueError("continuous momentum source gaps are invalid")
            series_gaps = cast(list[object], raw_series_gaps)
            if not all(isinstance(item, str) for item in series_gaps):
                raise ValueError("continuous momentum source gaps are invalid")
            typed_series_gaps = cast(list[str], series_gaps)
            if typed_series_gaps or len(rows) != 4:
                gaps.append(
                    {
                        "gap_id": "continuous_momentum_adjusted_history_unavailable",
                        "session": session.isoformat(),
                        "cutoff": cutoff.isoformat(),
                        "required_completed_sessions": 4,
                        "observed_completed_sessions": len(rows),
                        "source_gaps": typed_series_gaps,
                    }
                )
                continue
            adjusted: list[Decimal] = []
            close_strings: list[str] = []
            raw_hashes: list[str] = []
            factor_hashes: list[str] = []
            prior_days: list[date] = []
            for row in rows:
                day_text = row.get("trade_date")
                raw_hash = row.get("source_record_hash")
                factor_hash = row.get("factor_record_hash")
                if (
                    not isinstance(day_text, str)
                    or not isinstance(raw_hash, str)
                    or not isinstance(factor_hash, str)
                ):
                    raise ValueError("continuous momentum source row lacks bound record hashes")
                day = date.fromisoformat(day_text)
                close = _finite_positive_decimal(row.get("cutoff_adjusted_close"))
                if day >= session:
                    raise ValueError("continuous momentum input includes an uncompleted session")
                prior_days.append(day)
                adjusted.append(close)
                close_strings.append(str(close))
                raw_hashes.append(raw_hash)
                factor_hashes.append(factor_hash)
            if tuple(sorted(set(prior_days))) != tuple(prior_days):
                raise ValueError(
                    "continuous momentum source rows are not chronological unique sessions"
                )
            continuity_gap = _momentum_calendar_continuity_gap(
                historical_inputs,
                session_open_at=inputs.bar.session_open_at,
                current_session=session,
                prior_sessions=prior_days,
            )
            if continuity_gap is not None:
                gaps.append(
                    {
                        "gap_id": "continuous_momentum_calendar_contiguity_unverified",
                        "session": session.isoformat(),
                        "cutoff": cutoff.isoformat(),
                        **continuity_gap,
                    }
                )
                continue
            signals[session] = _MomentumSignal(
                session=session,
                cutoff=cutoff.isoformat(),
                action="buy" if adjusted[-1] > adjusted[0] else "cash",
                adjusted_closes=tuple(close_strings),
                source_record_hashes=tuple(raw_hashes),
                factor_record_hashes=tuple(factor_hashes),
            )
        except (KeyError, TypeError, ValueError) as exc:
            gaps.append(
                {
                    "gap_id": "continuous_momentum_adjusted_history_unavailable",
                    "session": session.isoformat(),
                    "cutoff": cutoff.isoformat(),
                    "detail": str(exc),
                }
            )
    return signals, gaps


def _finite_positive_decimal(value: object) -> Decimal:
    try:
        result = Decimal(str(value))
    except ArithmeticError as exc:
        raise ValueError("continuous momentum adjusted close is invalid") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError("continuous momentum adjusted close must be positive")
    return result


def _momentum_report_fields(
    baseline_id: str, momentum_signals: Mapping[date, _MomentumSignal]
) -> dict[str, object]:
    if baseline_id != _MOMENTUM_BASELINE_ID:
        return {}
    return {
        "momentum_binding": _MOMENTUM_BINDING,
        "momentum_signals": [item.report() for _, item in sorted(momentum_signals.items())],
    }


def _input_gaps(
    registered_window: ContinuousBaselineWindow,
    *,
    seed_inputs: HistoricalSessionInputs,
    inputs_by_day: Mapping[date, HistoricalSessionInputs],
) -> list[dict[str, object]]:
    gaps: list[dict[str, object]] = []
    if not _seed_ready(seed_inputs):
        gaps.append(
            {
                "gap_id": "overnight_seed_input_unavailable",
                "session": registered_window.window.observation_through_session.isoformat(),
                "gaps": _gaps(seed_inputs, None),
            }
        )
    expected_spec = seed_inputs.spec if _seed_ready(seed_inputs) else None
    for session in registered_window.sessions:
        inputs = inputs_by_day[session]
        if not _session_ready(inputs, expected_spec):
            gaps.append(
                {
                    "gap_id": "daily_execution_input_unavailable",
                    "session": session.isoformat(),
                    "gaps": _gaps(inputs, expected_spec),
                }
            )
    return gaps


def _seed_ready(inputs: HistoricalSessionInputs) -> bool:
    return (
        _session_ready(inputs, None)
        and inputs.spec is not None
        and inputs.spec.instrument_class == "exchange_traded_fund"
    )


def _session_ready(
    inputs: HistoricalSessionInputs, expected_spec: HistoricalInstrumentSpec | None
) -> bool:
    return (
        inputs.execution_ready
        and inputs.price_basis == "raw_unadjusted"
        and bool(inputs.source_record_hashes)
        and (expected_spec is None or inputs.spec == expected_spec)
    )


def _gaps(
    inputs: HistoricalSessionInputs, expected_spec: HistoricalInstrumentSpec | None
) -> list[str]:
    values = list(inputs.gaps)
    if inputs.price_basis != "raw_unadjusted":
        values.append("execution_price_basis_not_raw_unadjusted")
    if not inputs.source_record_hashes:
        values.append("source_record_hashes_missing")
    if expected_spec is not None and inputs.spec != expected_spec:
        values.append("historical_instrument_rule_changed_within_window")
    if inputs.spec is None:
        values.append("historical_instrument_spec_missing")
    if inputs.bar is None:
        values.append("raw_daily_bar_missing")
    return sorted(set(values))


def _window_results(
    account: HistoricalStreamingAccount, window: ContinuousBaselineWindow
) -> tuple[HistoricalSessionResult, ...]:
    by_day = {
        result.account_state.as_of.date(): result
        for result in account.results[1:]
        if result.account_state.as_of.date() in set(window.sessions)
    }
    return tuple(by_day[session] for session in window.sessions if session in by_day)


def _execution_policy_hash(baseline_id: str) -> str:
    value: dict[str, object] = {
        "baseline_id": baseline_id,
        "symbol": _RAW_BROAD_ETF_SYMBOL,
        "price_basis": "raw_unadjusted",
        "opening_allocation": "bootstrap_half_hs300",
        "execution_engine": "HistoricalStreamingAccount",
    }
    if baseline_id == _MOMENTUM_BASELINE_ID:
        value["momentum_binding_id"] = _MOMENTUM_BINDING["binding_id"]
    return canonical_hash(value)


def _initial_account_hash(
    account_seed: ContinuousBaselineAccountSeed, seed_inputs: HistoricalSessionInputs
) -> str:
    return canonical_hash(
        {
            "account_id": opaque_account_reference_hash(
                account_seed.account_reference, key=account_seed.account_reference_key
            ),
            "initial_cash": str(account_seed.initial_cash),
            "seed_symbol": _RAW_BROAD_ETF_SYMBOL,
            "seed_source_records": list(seed_inputs.source_record_hashes),
        }
    )


def _source_binding_hash(historical_inputs: HistoricalAShareInputs) -> str:
    return canonical_hash(
        {
            "snapshot_ids": sorted(historical_inputs.snapshot_ids),
            "rule_artifact_hashes": sorted(historical_inputs.rule_artifact_hashes),
            **(
                {"fund_halt_artifact_hashes": sorted(historical_inputs.fund_halt_artifact_hashes)}
                if historical_inputs.fund_halt_artifact_hashes
                else {}
            ),
            "policy": {
                "policy_id": historical_inputs.policy.policy_id,
                "daily_open_volume_fraction": str(
                    historical_inputs.policy.daily_open_volume_fraction
                ),
                "lane": historical_inputs.policy.lane,
                "opening_tick_validity_microseconds": (
                    historical_inputs.policy.opening_tick_validity_microseconds
                ),
            },
        }
    )


def _journal_path(
    state_root: Path,
    *,
    registration_id: str,
    baseline_id: str,
    window_id: str,
    source_binding_hash: str,
) -> Path:
    return (
        state_root
        / "continuous-baselines"
        / registration_id
        / f"source-{source_binding_hash}"
        / baseline_id
        / window_id
        / "account.jsonl"
    )


def _window_common(
    registered_window: ContinuousBaselineWindow,
    *,
    baseline_id: str,
    source_binding_hash: str,
    input_gaps: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "window_id": registered_window.window.window_id,
        "baseline_id": baseline_id,
        "decision_session": registered_window.window.decision_session.isoformat(),
        "outcome_window_end": registered_window.window.outcome_window_end.isoformat(),
        "registered_daily_calendar": [item.isoformat() for item in registered_window.sessions],
        "expected_sessions": len(registered_window.sessions),
        "source_binding_hash": source_binding_hash,
        "input_gaps": input_gaps,
    }


def _execution_target(baseline_id: str) -> dict[str, object]:
    if baseline_id == "cash_no_action":
        return {
            "target": "no_510300_position_after_first_registered_session",
            "comparable_seed": True,
        }
    if baseline_id == "same_initial_account_hold":
        return {"target": "retain_overnight_half_510300_seed", "comparable_seed": True}
    if baseline_id == _MOMENTUM_BASELINE_ID:
        return {
            "target": "daily_phase2_buy_maximum_affordable_long_or_abstain_cash",
            "comparable_seed": True,
            "binding_id": _MOMENTUM_BINDING["binding_id"],
        }
    return {
        "target": "maximum_affordable_510300_whole_lots_after_first_registered_session",
        "comparable_seed": True,
    }


def _unsupported_rule_baselines() -> list[dict[str, object]]:
    """Retain known diagnostic policies without silently turning proxies into fills."""

    return [
        {
            "baseline_id": "lagged_volatility_rule",
            "execution_eligible": False,
            "status": "unsupported_policy_or_source",
            "registered_policy_ref": "continuous_study.OrdinarySelectionPolicy",
            "gap_id": "continuous_volatility_rebalance_policy_missing",
            "reason": (
                "the existing realized-volatility calculation selects ordinary study windows; "
                "it does not define a trading or rebalance rule"
            ),
        },
        {
            "baseline_id": "equal_sector_buy_and_hold",
            "execution_eligible": False,
            "status": "unsupported_policy_or_source",
            "registered_policy_ref": "regime_study._equal_sector_buy_and_hold_path",
            "gap_id": "historical_tradable_sector_membership_missing",
            "reason": (
                "the registered descriptive equal-sector path uses SW2021 index proxies; no "
                "historical tradable sector membership, executable symbols, or source-backed "
                "corporate-action paths are bound"
            ),
        },
        {
            "baseline_id": "lagged_sector_momentum",
            "execution_eligible": False,
            "status": "unsupported_policy_or_source",
            "registered_policy_ref": "regime_study._lagged_sector_momentum_path",
            "gap_id": "historical_tradable_sector_membership_missing",
            "reason": (
                "the registered 20-session monthly top-three sector rule uses SW2021 index "
                "proxies; no historical tradable sector membership, executable symbols, or "
                "source-backed corporate-action paths are bound"
            ),
        },
    ]


def _resolve_selection_panel(
    registration: ContinuousStudyRegistration,
    supplied: ValidatedRegimePanel | RegimePanel,
) -> RegimePanel:
    if isinstance(supplied, ValidatedRegimePanel):
        if supplied.panel_id != registration.selection_panel_id:
            raise ValueError("baseline selection panel does not match the frozen registration")
        panel = supplied.panel
    else:
        panel = supplied
    if (
        panel.dataset_id != registration.dataset_id
        or panel.dataset_hash != registration.dataset_hash
    ):
        raise ValueError("baseline selection panel does not bind the frozen registration dataset")
    return panel


def _primary_calendar(panel: RegimePanel) -> tuple[date, ...]:
    primary = _primary_series(panel)
    dates = tuple(_row_date(row) for row in primary.rows)
    if not dates or tuple(sorted(set(dates))) != dates:
        raise ValueError("baseline primary calendar must be non-empty, unique, and chronological")
    return dates


def _primary_series(panel: RegimePanel) -> RegimeSeries:
    match = next((item for item in panel.series if item.series_id == "000300.SH"), None)
    if match is None:
        raise ValueError("baseline selection panel has no CSI 300 calendar series")
    return match


def _row_date(row: Mapping[str, object]) -> date:
    value = row.get("trade_date")
    if not isinstance(value, str):
        raise ValueError("baseline panel row has no trade_date")
    if len(value) == 8 and value.isdigit():
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    return date.fromisoformat(value)


def _positive_decimal(row: Mapping[str, object], key: str) -> Decimal:
    try:
        value = Decimal(str(row[key]))
    except (KeyError, ArithmeticError) as exc:
        raise ValueError(f"baseline panel {key} is invalid") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(f"baseline panel {key} must be positive")
    return value


def _objects(value: object, label: str) -> Sequence[Mapping[str, object]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be object list")
    result: list[Mapping[str, object]] = []
    for item in cast(list[object], value):
        if not isinstance(item, dict):
            raise ValueError(f"{label} must be object list")
        result.append(cast(Mapping[str, object], item))
    return result
