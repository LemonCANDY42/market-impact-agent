"""Historical discovery eligibility and exact concrete mandate identity."""

from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.domain import TradingEnvironment, TradingMandateV3
from market_impact_agent.dynamic_ashare_admission import (
    DynamicAShareAdmission,
    HistoricalSecurityEvidence,
)

from .test_autonomous_paper import AT, _fixture  # pyright: ignore[reportPrivateUsage]


class SourceAuthority:
    def __init__(self, evidence: HistoricalSecurityEvidence) -> None:
        self.evidence = evidence

    def reopen_security(self, symbol: str, cutoff: object) -> HistoricalSecurityEvidence | None:
        return self.evidence if symbol == self.evidence.symbol else None


def _evidence() -> HistoricalSecurityEvidence:
    return HistoricalSecurityEvidence(
        "600519.SH",
        "XSHG",
        "equity",
        AT,
        AT,
        AT + timedelta(hours=1),
        Decimal(100),
        False,
        Decimal(90),
        Decimal(110),
        Decimal(1000000),
        "none",
        100,
        Decimal("0.01"),
        ("a" * 64,),
    )


def _mandate(root: Path) -> TradingMandateV3:
    base = _fixture(root).mandate
    values = {field.name: getattr(base, field.name) for field in fields(base)}
    values.update(
        currency="CNY",
        environment=TradingEnvironment.BACKTEST,
        allowed_instruments=frozenset({"510300.SH"}),
        minimum_net_exposure=Decimal(0),
    )
    return TradingMandateV3(**values, universe_binding_hash="0" * 64)


def test_discovery_does_not_require_existing_pair_or_grant_missing_authority(
    tmp_path: Path,
) -> None:
    authority = SourceAuthority(_evidence())
    admission = DynamicAShareAdmission(authority)
    found = admission.discover(("600519.SH", "159915.SZ"), AT)
    assert found[0].gaps == ("historical_security_evidence_missing",)
    assert found[1].execution_ready
    binding = admission.bind(("600519.SH", "159915.SZ"), AT, _mandate(tmp_path))
    assert binding.mandate.allowed_instruments == frozenset({"600519.SH"})
    assert validate_agent_contract(binding.mandate.to_dict(), "trading-mandate.schema.json") == ()
    assert validate_agent_contract(binding.to_dict(), "dynamic-ashare-universe.schema.json") == ()
    admission.assert_current(binding, AT)
    for mutation in (
        replace(binding.mandate, account_id="other-account"),
        replace(binding.mandate, daily_submission_limit=999),
    ):
        with pytest.raises(PermissionError, match="binding changed"):
            admission.assert_current(replace(binding, mandate=mutation), AT)
    authority.evidence = replace(authority.evidence, source_record_hashes=("b" * 64,))
    with pytest.raises(PermissionError, match="binding changed"):
        admission.assert_current(binding, AT)


@pytest.mark.parametrize(
    "changes,gap",
    [
        ({"halted": None}, "halt_status_unverified"),
        ({"halted": True}, "halted"),
        ({"corporate_action_status": "split"}, "corporate_action_semantics_unverified"),
        ({"turnover": None}, "liquidity_unverified"),
        ({"raw_price": Decimal("NaN")}, "raw_price_unverified"),
        ({"cutoff": AT - timedelta(days=1)}, "historical_evidence_not_current"),
    ],
)
def test_historical_gaps_remain_research_eligible(changes: dict[str, object], gap: str) -> None:
    evidence = replace(_evidence(), **changes)
    result = DynamicAShareAdmission(SourceAuthority(evidence)).discover((evidence.symbol,), AT)[0]
    assert gap in result.gaps
    assert result.to_dict()["research_eligible"] is True
    assert not result.execution_ready


def test_currency_scope_and_wildcards_are_not_relaxed(tmp_path: Path) -> None:
    mandate = _mandate(tmp_path)
    for changes in (
        {"currency": "USD"},
        {"environment": TradingEnvironment.LIVE},
        {"allowed_instruments": frozenset({"*"})},
        {"execution_scope": "ibkr"},
    ):
        with pytest.raises(ValueError):
            replace(mandate, **changes)
    with pytest.raises(PermissionError, match="stale"):
        DynamicAShareAdmission(SourceAuthority(_evidence())).bind(
            ("600519.SH",), AT + timedelta(days=2), mandate
        )
