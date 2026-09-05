"""Production CNY Mock account and immutable current portfolio composition.

The Mock provider remains the account owner. This module only projects its durable
facts and source-qualified prices into the existing portfolio review authority.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from hashlib import sha256
from typing import cast
from zoneinfo import ZoneInfo

from market_impact_agent.account_state import (
    AccountPosition,
    AccountStateSnapshot,
    CashBalance,
    account_state_snapshot_from_dict,
    opaque_account_reference_hash,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRule,
    ExchangeInstrumentRuleSet,
)
from market_impact_agent.data_inputs import FrozenDataSnapshotInput, LocalDataSnapshotStore
from market_impact_agent.domain import ApprovalMode, Side, TradingEnvironment, TradingMandateV3
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission, SecurityAdmission
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.portfolio_decision import (
    PortfolioExposureViewV2,
    RawMarkedPositionV2,
    RegisteredPortfolioExposureViewAuthorityV2,
)
from market_impact_agent.portfolio_review import PortfolioReviewAuthority, PortfolioReviewInputs
from market_impact_agent.prospective_ashare_inputs import ProspectiveAShareInputs
from market_impact_agent.providers import MockExecutionProvider, ReconciliationSnapshot
from market_impact_agent.research_thesis_runtime import ResearchThesisRunInputs
from market_impact_agent.runtime_store import RunJournal


class ProspectiveMockComposition:
    def __init__(
        self,
        *,
        store: LocalDataSnapshotStore,
        profile_id: str,
        study_registration_id: str,
        opening_authority_ref: str,
        parent_run_id: str,
        market_factory: Callable[[FrozenDataSnapshotInput], ProspectiveAShareInputs],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.store, self.clock, self.market_factory = store, clock, market_factory
        self.parent_run_id = parent_run_id
        self.opening_authority_ref = opening_authority_ref
        self.seed = "prospective-cny-" + canonical_hash([study_registration_id, profile_id])
        self.provider = MockExecutionProvider(
            store.root / "prospective-mock" / self.seed / "account.sqlite3", clock=clock
        )
        self.account_scope = opaque_account_reference_hash(
            "simulated:" + self.seed, key=sha256(("synthetic-only:" + self.seed).encode()).digest()
        )
        self.inputs: PortfolioReviewInputs | None = None
        self.portfolio: PortfolioReviewAuthority | None = None
        self._context: tuple[AccountStateSnapshot, datetime] | None = None
        self._ledger: tuple[list[dict[str, object]], list[dict[str, object]]] | None = None
        self.frozen_snapshot_input: FrozenDataSnapshotInput | None = None
        self.research_inputs: ResearchThesisRunInputs | None = None

    def _configure(self, frozen: FrozenDataSnapshotInput) -> None:
        store, clock, market_factory = self.store, self.clock, self.market_factory
        opening_authority_ref = self.opening_authority_ref
        opening_frozen = frozen
        with self.provider._connect() as connection:  # pyright: ignore[reportPrivateUsage]
            configured = connection.execute(
                "SELECT payload_json FROM mock_account_configuration"
            ).fetchone()
        if configured is None:
            opening_market = market_factory(opening_frozen)
            opened_at = clock()
            qualification = opening_market.qualification("510300.SH", opened_at)
            seed = opening_market.reopen_security("510300.SH", opened_at)
            spec = qualification.spec
            if (
                not qualification.qualified
                or spec is None
                or seed.raw_price is None
                or seed.raw_price_observed_at is None
                or seed.raw_price_observed_at > opened_at
            ):
                raise PermissionError("half_hs300_opening_source_missing")
            assert seed.buy_lot_size is not None
            quantity = (Decimal(50000) / seed.raw_price / seed.buy_lot_size).to_integral_value(
                rounding=ROUND_FLOOR
            ) * seed.buy_lot_size
            if quantity <= 0:
                raise PermissionError("half_hs300_opening_lot_unaffordable")
            cash = Decimal(100000) - quantity * seed.raw_price
            source = store.artifacts.put_json(
                {
                    "registration": opening_authority_ref,
                    "security": seed.to_dict(),
                    "qualification": qualification.qualification_artifact_hash,
                    "policy": "100k-half-hs300-overnight-v1",
                }
            )
            self.provider.configure_simulated_account(
                seed=self.seed,
                cash=(CashBalance("CNY", cash, cash),),
                positions=(
                    AccountPosition(
                        "510300.SH",
                        spec.venue,
                        spec.instrument_class,
                        Side.BUY,
                        quantity,
                        quantity * seed.raw_price / Decimal(100000),
                        None,
                    ),
                ),
                instruments={"510300.SH": (spec.venue, spec.instrument_class)},
                opened_at=opened_at,
                opening_authority={
                    "version": "cny-local-mock.v1",
                    "source_reference": source.content_hash,
                    "opening_inventory": "overnight_sellable",
                },
            )

    def admission_source(
        self, inputs: ResearchThesisRunInputs, frozen: FrozenDataSnapshotInput
    ) -> ProspectiveAShareInputs:
        return self.market_factory(frozen)

    def account_source(self) -> AccountStateSnapshot:
        if self._context is None:
            raise PermissionError("current portfolio account has not been captured")
        return self._context[0]

    def _symbols(self) -> tuple[str, ...]:
        with self.provider._connect() as connection:  # pyright: ignore[reportPrivateUsage]
            opening = json.loads(
                connection.execute(
                    "SELECT payload_json FROM mock_account_configuration"
                ).fetchone()[0]
            )
            quantities = {
                item["target_id"]: Decimal(item["quantity"]) for item in opening["positions"]
            }
            for row in connection.execute(
                "SELECT f.quantity, r.order_json FROM mock_execution_fills f "
                "JOIN mock_execution_receipts r USING(client_order_id)"
            ):
                order = json.loads(row["order_json"])
                sign = 1 if order["side"] == "buy" else -1
                symbol = order["instrument_id"]
                quantities[symbol] = quantities.get(symbol, Decimal(0)) + sign * Decimal(
                    row["quantity"]
                )
        return tuple(sorted(symbol for symbol, quantity in quantities.items() if quantity))

    @staticmethod
    def _prices(securities: tuple[SecurityAdmission, ...]) -> dict[str, PriceBasis]:
        result: dict[str, PriceBasis] = {}
        for admission in securities:
            item = admission.evidence
            if not admission.execution_ready or item is None:
                raise PermissionError("security_evidence_incomplete:" + ",".join(admission.gaps))
            assert item.raw_price is not None and item.raw_price_observed_at is not None
            result[item.symbol] = PriceBasis(
                item.symbol,
                "CNY",
                "per_share",
                "raw_reference_quote",
                item.raw_price,
                "source-qualified-prospective-quote",
                canonical_hash(item.to_dict()),
                item.raw_price_observed_at.astimezone(UTC),
                item.effective_until.astimezone(UTC),
            )
        return result

    def capture_context(
        self, inputs: ResearchThesisRunInputs, frozen: FrozenDataSnapshotInput
    ) -> tuple[AccountStateSnapshot, datetime]:
        self._configure(frozen)
        context_id = "prospective.mock.capture." + canonical_hash(
            [self.seed, inputs.identity_dict(), sorted(frozen.authorized_snapshot_ids)]
        )
        journal = RunJournal.authoritative(self.store)
        existing = journal.event(context_id)
        if existing is not None:
            saved = cast(
                dict[str, object],
                self.store.artifacts.read_json(str(existing.payload["artifact_hash"])),
            )
            self._ledger = (
                cast(list[dict[str, object]], saved["fills"]),
                cast(list[dict[str, object]], saved["orders"]),
            )
            self._context = (
                account_state_snapshot_from_dict(saved["account"]),
                datetime.fromisoformat(str(saved["cutoff"])),
            )
            return self._context
        market = self.market_factory(frozen)
        symbols = tuple(sorted({inputs.target_id, *self._symbols()}))
        prices = self._prices(DynamicAShareAdmission(market).discover(symbols, self.clock()))
        account = self.provider.simulated_account_snapshot(price_bases=prices)
        # Freeze only after capturing actual account facts; never reuse the thesis cutoff.
        cutoff = self.clock()
        prices = self._prices(DynamicAShareAdmission(market).discover(symbols, cutoff))
        if any(price.observed_at > account.as_of for price in prices.values()):
            raise PermissionError("account_capture_precedes_revalidated_mark")
        self._ledger = self._execution_ledger()
        artifact = self.store.artifacts.put_json(
            {
                "account": account.to_dict(),
                "cutoff": cutoff.isoformat(),
                "fills": self._ledger[0],
                "orders": self._ledger[1],
            }
        )
        journal.append(
            run_id=self.parent_run_id,
            event_id=context_id,
            event_type="prospective.mock.account.captured",
            observed_at=cutoff,
            payload={"artifact_hash": artifact.content_hash},
        )
        self._context = account, cutoff
        return self._context

    def portfolio_authority(
        self,
        inputs: ResearchThesisRunInputs,
        frozen: FrozenDataSnapshotInput,
        account: AccountStateSnapshot,
        security: SecurityAdmission,
    ) -> PortfolioReviewAuthority:
        self.frozen_snapshot_input, self.research_inputs = frozen, inputs
        if self._context is None or self._context[0] != account:
            raise PermissionError("portfolio account differs from captured context")
        cutoff = self._context[1]
        market = self.market_factory(frozen)
        symbols = tuple(
            sorted({inputs.target_id, *(item.target_id for item in account.positions or ())})
        )
        template = TradingMandateV3(
            mandate_id="prospective-template-" + canonical_hash([self.seed, cutoff.isoformat()]),
            account_id=self.account_scope,
            harness_authority_id=self.store.harness_authority_id,
            environment=TradingEnvironment.PAPER,
            approval_mode=ApprovalMode.AUTONOMOUS,
            valid_from=cutoff,
            valid_until=cutoff + timedelta(minutes=5),
            allowed_instruments=frozenset(symbols),
            allowed_instrument_classes=frozenset({"cash_equity", "unlevered_exchange_traded_fund"}),
            allowed_sides=frozenset({Side.BUY, Side.SELL}),
            currency="CNY",
            gross_exposure_limit=Decimal(100000),
            minimum_net_exposure=Decimal(0),
            maximum_net_exposure=Decimal(100000),
            maximum_position_count=5,
            maximum_single_position_fraction=Decimal(1),
            daily_turnover_limit=Decimal(100000),
            daily_submission_limit=10,
            daily_loss_kill_threshold=Decimal(10000),
            strategy_peak_drawdown_kill_threshold=Decimal(20000),
            universe_binding_hash=canonical_hash(symbols),
            execution_scope="local_mock",
        )
        binding = DynamicAShareAdmission(market).bind(symbols, cutoff, template)
        prices = self._prices(binding.securities)
        if next(item for item in binding.securities if item.symbol == security.symbol) != security:
            raise PermissionError("portfolio security differs from current admission")
        rules: dict[tuple[str, str], ExchangeInstrumentRule] = {}
        for admission in binding.securities:
            item = admission.evidence
            assert (
                item is not None and item.buy_lot_size is not None and item.price_tick is not None
            )
            pair = item.venue, item.instrument_class
            rule = ExchangeInstrumentRule(
                "prospective-" + "-".join(pair),
                *pair,
                item.buy_lot_size,
                float(item.price_tick),
                "CNY",
                "ordinary_auction_buy_and_sell_order",
                (),
            )
            if pair in rules and rules[pair] != rule:
                raise PermissionError("venue_class_rule_variants_require_exact_sizing_support")
            rules[pair] = rule
            qualification = market.qualification(item.symbol, cutoff)
            self.provider.register_simulated_instrument(
                target_id=item.symbol,
                venue=item.venue,
                instrument_class=item.instrument_class,
                qualification_hash=qualification.qualification_artifact_hash,
            )
        expiry = min(binding.mandate.valid_until, *(price.valid_until for price in prices.values()))
        position = account.project_positions(
            evaluated_at=account.reconciled_at, max_age=timedelta(minutes=5)
        )
        view = AuthorizedDecisionView.build(
            cutoff=cutoff,
            frozen_at=cutoff,
            data_snapshot_ids=tuple(sorted(frozen.authorized_snapshot_ids)),
            decision_input_ids=(),
            position_snapshot=position,
        )
        today = cutoff.astimezone(ZoneInfo("Asia/Shanghai")).date()
        fills, orders = self._ledger if self._ledger is not None else self._execution_ledger()
        turnover = sum(
            (
                Decimal(str(row["quantity"])) * Decimal(str(row["price"]))
                for row in fills
                if datetime.fromisoformat(str(row["observed_at"]))
                .astimezone(ZoneInfo("Asia/Shanghai"))
                .date()
                == today
            ),
            Decimal(0),
        )
        submissions = sum(
            1
            for row in orders
            if row["order_json"] is not None
            and datetime.fromisoformat(json.loads(str(row["order_json"]))["created_at"])
            .astimezone(ZoneInfo("Asia/Shanghai"))
            .date()
            == today
        )
        nav = sum((cash.settled for cash in account.cash or ()), Decimal(0)) + sum(
            (item.quantity * prices[item.target_id].price for item in account.positions or ()),
            Decimal(0),
        )
        journal = RunJournal.authoritative(self.store)
        history = [
            cast(
                dict[str, object],
                self.store.artifacts.read_json(str(event.payload["artifact_hash"])),
            )
            for event in journal.events(self.parent_run_id)
            if event.event_type == "prospective.mock.context.frozen"
            and event.payload.get("account_scope") == self.account_scope
            and event.observed_at <= cutoff
        ]
        peak = max([Decimal(100000), nav, *(Decimal(str(item["nav"])) for item in history)])
        prior_days = [
            item
            for item in history
            if datetime.fromisoformat(str(item["cutoff"]))
            .astimezone(ZoneInfo("Asia/Shanghai"))
            .date()
            < today
        ]
        start_nav = Decimal(str(prior_days[-1]["nav"])) if prior_days else Decimal(100000)
        kills = tuple(
            name
            for active, name in (
                (peak - nav >= 20000, "strategy_peak_drawdown_threshold_exceeded"),
                (start_nav - nav >= 10000, "daily_loss_threshold_exceeded"),
            )
            if active
        )
        exposure = PortfolioExposureViewV2.build(
            authorized_view=view,
            position_snapshot=position,
            raw_mark_set_hash=canonical_hash(
                {key: value.to_dict() for key, value in prices.items()}
            ),
            execution_ledger_snapshot_hash=canonical_hash(orders),
            reconciliation_ledger_snapshot_hash=canonical_hash(account.to_dict()),
            currency="CNY",
            marked_positions=tuple(
                RawMarkedPositionV2(
                    item.target_id,
                    item.venue,
                    item.instrument_class,
                    item.side,
                    item.quantity,
                    prices[item.target_id].price,
                    canonical_hash(prices[item.target_id].to_dict()),
                )
                for item in account.positions or ()
            ),
            daily_turnover_used=turnover,
            daily_submissions_used=submissions,
            active_kill_reasons=kills,
            observed_at=cutoff,
            valid_until=expiry,
        )
        sources: tuple[dict[str, object], ...] = tuple(
            {
                "symbol": item.symbol,
                "source_record_hashes": list(item.evidence.source_record_hashes),
            }
            for item in binding.securities
            if item.evidence is not None
        )
        rule_set = ExchangeInstrumentRuleSet(
            "exchange-instrument-rule-set-"
            + canonical_hash([sources, [rule.to_dict() for rule in rules.values()]]),
            cutoff.date(),
            sources,
            tuple(rules.values()),
        )
        current = PortfolioReviewInputs(
            account, position, view, exposure, binding.mandate, prices, rule_set, cutoff, expiry
        )
        artifact = self.store.artifacts.put_json(
            {
                "inputs": current.to_dict(),
                "universe": binding.to_dict(),
                "nav": str(nav),
                "cutoff": cutoff.isoformat(),
            }
        )
        journal.append(
            run_id=self.parent_run_id,
            event_id="prospective.mock.context." + artifact.content_hash,
            event_type="prospective.mock.context.frozen",
            observed_at=cutoff,
            payload={"artifact_hash": artifact.content_hash, "account_scope": self.account_scope},
        )
        self.inputs = current
        self.portfolio = PortfolioReviewAuthority(
            self.store,
            input_source=lambda: current,
            exposure_authority=RegisteredPortfolioExposureViewAuthorityV2(
                {exposure.exposure_view_id: exposure}
            ),
            clock=self.clock,
            proposal_version="v5",
        )
        current.assert_complete(
            self.store.harness_authority_id, self.portfolio.exposure_authority, cutoff
        )
        return self.portfolio

    def _execution_ledger(self) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        with self.provider._connect() as connection:  # pyright: ignore[reportPrivateUsage]
            connection.execute("BEGIN")
            fills = [
                dict(row)
                for row in connection.execute("SELECT * FROM mock_execution_fills ORDER BY fill_id")
            ]
            orders = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM mock_execution_receipts ORDER BY client_order_id"
                )
            ]
        return fills, orders

    def refresh_execution_inputs(
        self,
        reconciliation_snapshot: ReconciliationSnapshot | None = None,
        *,
        frozen: FrozenDataSnapshotInput | None = None,
    ) -> PortfolioReviewInputs:
        """Project changed provider facts for the existing execution reconciliation owner."""
        original = self.inputs
        research = self.research_inputs
        frozen = frozen or self.frozen_snapshot_input
        if original is None or research is None or frozen is None:
            raise PermissionError("execution refresh requires the original portfolio context")
        market = self.market_factory(frozen)
        symbols = tuple(sorted(set(original.mandate.allowed_instruments) | set(self._symbols())))
        prices = self._prices(DynamicAShareAdmission(market).discover(symbols, self.clock()))
        account = self.provider.simulated_account_snapshot(
            price_bases=prices, reconciliation_snapshot=reconciliation_snapshot
        )
        cutoff = self.clock()
        security = DynamicAShareAdmission(market).discover((research.target_id,), cutoff)[0]
        original_context, original_ledger = self._context, self._ledger
        original_portfolio, original_frozen = self.portfolio, self.frozen_snapshot_input
        self._context, self._ledger = (account, cutoff), None
        try:
            current = self.portfolio_authority(research, frozen, account, security).input_source()
        finally:
            self._context, self._ledger = original_context, original_ledger
            self.inputs, self.portfolio = original, original_portfolio
            self.frozen_snapshot_input = original_frozen
        if reconciliation_snapshot is not None:
            reconciliation_hash = canonical_hash(reconciliation_snapshot.to_dict())
            core = {
                **current.exposure_view.core_dict(),
                "reconciliation_ledger_snapshot_hash": reconciliation_hash,
            }
            current = replace(
                current,
                exposure_view=replace(
                    current.exposure_view,
                    exposure_view_id="portfolio-exposure-view-" + canonical_hash(core),
                    reconciliation_ledger_snapshot_hash=reconciliation_hash,
                ),
            )
        refreshed = replace(
            current,
            mandate=original.mandate,
            expires_at=min(current.expires_at, original.expires_at),
        )
        self.inputs, self.portfolio = original, original_portfolio
        return refreshed
