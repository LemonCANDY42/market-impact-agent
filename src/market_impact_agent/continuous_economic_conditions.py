"""Source-reopened economic conditions, separate from strategy and run identities."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount


def _value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            key: _value(item)
            for key, item in cast(dict[str, Any], value).items()
            if key != "source_ref"
        }
    if isinstance(value, (tuple, list)):
        return [_value(item) for item in cast(Sequence[Any], value)]
    return value


def reopen_common_economic_conditions(
    *,
    account: HistoricalStreamingAccount,
    historical_inputs: HistoricalAShareInputs,
    sessions: Sequence[date],
    symbols: Sequence[str] | None = None,
) -> dict[str, object]:
    """Verify persisted inputs against source data and derive economic equality.

    The caller supplies a real, reconstructed account, never a report's seed hash.
    No journal or historical report is changed. Required path inputs fail closed;
    optional candidates retain source missingness as an economic condition.
    """
    if not account.results or not sessions or list(sessions) != sorted(set(sessions)):
        raise ValueError("economic comparison requires seed and full ordered calendar")
    records = [json.loads(line) for line in account.journal_path.read_text().splitlines()]
    batches = [record for record in records[1:] if "bars" in record]
    if [canonical_hash(record) for record in batches] != [r.input_hash for r in account.results]:
        raise ValueError("economic comparison journal differs from reconstructed account")
    seed = account.results[0]
    seed_date = seed.account_state.as_of.date()
    observed = [result.account_state.as_of.date() for result in account.results[1:]]
    if observed != list(sessions[: len(observed)]) or seed_date >= sessions[0]:
        raise ValueError("economic comparison account differs from registered calendar")
    targets = sorted(set(symbols or account.specs))
    if not set(account.specs) <= set(targets):
        raise ValueError("economic comparison must cover all account instruments")
    rows: list[dict[str, object]] = []
    source_hashes: set[str] = set()
    source_by_day: dict[date, dict[str, Any]] = {}
    for day in (seed_date, *sessions):
        day_inputs: dict[str, Any] = {}
        for symbol in targets:
            source = historical_inputs.session(symbol, day)
            day_inputs[symbol] = source
            source_hashes.update(source.source_record_hashes)
            rows.append(
                {
                    "session": day.isoformat(),
                    "symbol": symbol,
                    "spec": None if source.spec is None else _value(asdict(source.spec)),
                    "bar": None if source.bar is None else _value(asdict(source.bar)),
                    "execution_ready": source.execution_ready,
                    "gaps": sorted(set(source.gaps)),
                    "actions": _value([asdict(action) for action in source.corporate_actions]),
                    "price_basis": source.price_basis,
                    "liquidity_basis": source.liquidity_basis,
                }
            )
        source_by_day[day] = day_inputs
    for record, result in zip(batches, account.results, strict=True):
        day_inputs = source_by_day[result.account_state.as_of.date()]
        expected_actions: list[Any] = []
        for symbol, bar in record["bars"].items():
            source = day_inputs[symbol]
            if not source.execution_ready or source.spec is None or source.bar is None:
                raise ValueError("economic comparison requires complete reopened source inputs")
            if not account.specs[symbol].execution_rules_compatible(source.spec):
                raise ValueError("economic comparison journal fees/spec differ from source")
            # Journal decimals retain textual precision; compare canonical numeric values.
            expected_bar = json.loads(
                json.dumps(
                    asdict(source.bar),
                    default=lambda value: (
                        value.isoformat() if isinstance(value, datetime) else str(value)
                    ),
                )
            )
            if bar != expected_bar:
                raise ValueError("economic comparison journal raw prices differ from source")
            expected_actions.extend(asdict(action) for action in source.corporate_actions)
        if record["actions"] != json.loads(
            json.dumps(
                expected_actions,
                default=lambda value: (
                    value.isoformat() if isinstance(value, datetime) else str(value)
                ),
            )
        ):
            raise ValueError("economic comparison journal corporate actions differ from source")
    policy = historical_inputs.policy
    economics: dict[str, object] = {
        "seed": _value(
            {
                "as_of": seed.account_state.as_of,
                "initial_cash": account.initial_cash,
                "nav": seed.nav,
                "cash": seed.cash,
                "positions": {key: value for key, value in seed.positions.items() if value},
                "cash_only_inception_at": account.cash_only_inception_at,
            }
        ),
        "currency": "CNY",
        "sessions": [day.isoformat() for day in sessions],
        "market_conditions": rows,
        "fill_assumptions": {
            "engine": "historical-streaming-account.v1",
            "prob_slippage": "0",
            "random_seed": 0,
            "daily_open_volume_fraction": _value(policy.daily_open_volume_fraction),
            "opening_tick_validity_microseconds": policy.opening_tick_validity_microseconds,
            "limit_basis": policy.limit_basis,
        },
    }
    return {
        "schema_version": "market-impact.common-economic-conditions.v1",
        "economic_hash": canonical_hash(economics),
        "economics": economics,
        "source_evidence": {
            "journal_hash": canonical_hash(records),
            "seed_result_hash": seed.result_hash,
            "source_record_hashes": sorted(source_hashes),
        },
    }


def compare_reopened_continuous_accounts(
    reviewed: dict[str, object],
    control: dict[str, object],
    *,
    reviewed_account: HistoricalStreamingAccount,
    control_account: HistoricalStreamingAccount,
    reviewed_inputs: HistoricalAShareInputs,
    control_inputs: HistoricalAShareInputs,
    sessions: Sequence[date],
    symbols: Sequence[str] | None = None,
) -> dict[str, object]:
    """Offline or live reconciliation; a report-carried dictionary is not authority."""
    from market_impact_agent.continuous_metrics import (
        compare_continuous_accounts,
        measure_continuous_account,
    )

    if reviewed["complete"] is not True or control["complete"] is not True:
        return {"status": "incomplete_pair", "performance_difference": None}
    targets = symbols or sorted(set(reviewed_account.specs) | set(control_account.specs))
    conditions: list[dict[str, object]] = []
    for measurement, account, inputs in (
        (reviewed, reviewed_account, reviewed_inputs),
        (control, control_account, control_inputs),
    ):
        reopened = reopen_common_economic_conditions(
            account=account, historical_inputs=inputs, sessions=sessions, symbols=targets
        )
        rebuilt = measure_continuous_account(
            initial_nav=account.results[0].nav,
            sessions=account.results[1:],
            expected_sessions=len(sessions),
            execution_policy_hash=str(measurement["execution_policy_hash"]),
            initial_account_hash=str(measurement["initial_account_hash"]),
            model_cost_microusd=int(str(measurement["model_cost_microusd"])),
        )
        for key, value in rebuilt.items():
            if measurement.get(key) != value:
                raise ValueError("economic comparison measurement differs from account replay")
        conditions.append(reopened)
    if conditions[0]["economics"] != conditions[1]["economics"]:
        raise ValueError("economic comparison requires the same reopened economic conditions")
    comparison = compare_continuous_accounts(
        reviewed,
        {
            **control,
            "execution_policy_hash": reviewed["execution_policy_hash"],
            "initial_account_hash": reviewed["initial_account_hash"],
        },
    )
    return {
        **comparison,
        "common_economic_hash": conditions[0]["economic_hash"],
        "reopened_source_evidence": [item["source_evidence"] for item in conditions],
        "account_identities": [
            {key: item[key] for key in ("execution_policy_hash", "initial_account_hash")}
            for item in (reviewed, control)
        ],
    }
