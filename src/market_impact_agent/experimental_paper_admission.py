from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from market_impact_agent.agent_contracts import EvidencePack, canonical_hash
from market_impact_agent.domain import (
    OrderIntent,
    SignalIntent,
    TradingEnvironment,
    require_aware,
)
from market_impact_agent.prospective_query_gate import ProspectiveQueryGateResult

EXPERIMENTAL_PAPER_ADMISSION_SCHEMA = "market-impact.experimental-paper-admission.v1"
EXPERIMENTAL_PAPER_CLAIM_SCOPE = "execution_diagnostic_only_no_alpha_or_live_claim"


@dataclass(frozen=True, slots=True)
class ExperimentalPaperAdmission:
    admission_id: str
    query_gate_result_id: str
    query_gate_result_hash: str
    evidence_pack_id: str
    evidence_pack_hash: str
    signal_id: str
    signal_intent_hash: str
    order_intent_hash: str
    created_at: datetime
    strategy_admission_id: None = None
    claim_scope: str = EXPERIMENTAL_PAPER_CLAIM_SCOPE
    alpha_claim: bool = False
    live_capability: bool = False
    execution_authority: bool = False
    schema_version: str = EXPERIMENTAL_PAPER_ADMISSION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EXPERIMENTAL_PAPER_ADMISSION_SCHEMA:
            raise ValueError("unsupported experimental paper admission schema")
        if not self.query_gate_result_id.startswith("prospective-query-gate-"):
            raise ValueError("experimental paper admission Query Gate identity is invalid")
        _sha256(self.query_gate_result_hash, "experimental paper Query Gate hash")
        if not self.evidence_pack_id.startswith("evidence-pack-"):
            raise ValueError("experimental paper admission Evidence Pack identity is invalid")
        _sha256(self.evidence_pack_hash, "experimental paper Evidence Pack hash")
        if not self.signal_id.startswith("signal-"):
            raise ValueError("experimental paper admission Signal identity is invalid")
        _sha256(self.signal_intent_hash, "experimental paper Signal Intent hash")
        _sha256(self.order_intent_hash, "experimental paper Order Intent hash")
        _strict_utc(self.created_at, "experimental paper admission created_at")
        if self.strategy_admission_id is not None:
            raise ValueError("experimental paper admission cannot claim strategy promotion")
        if self.claim_scope != EXPERIMENTAL_PAPER_CLAIM_SCOPE:
            raise ValueError("experimental paper admission claim scope is invalid")
        if self.alpha_claim or self.live_capability or self.execution_authority:
            raise ValueError("experimental paper admission cannot grant alpha, live, or execution")
        if self.admission_id != self.expected_admission_id:
            raise ValueError("experimental paper admission_id does not match content")

    @property
    def expected_admission_id(self) -> str:
        return f"experimental-paper-admission-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "query_gate_result_id": self.query_gate_result_id,
            "query_gate_result_hash": self.query_gate_result_hash,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "signal_id": self.signal_id,
            "signal_intent_hash": self.signal_intent_hash,
            "order_intent_hash": self.order_intent_hash,
            "created_at": _timestamp(self.created_at),
            "strategy_admission_id": self.strategy_admission_id,
            "claim_scope": self.claim_scope,
            "alpha_claim": self.alpha_claim,
            "live_capability": self.live_capability,
            "execution_authority": self.execution_authority,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "admission_id": self.admission_id}

    def assert_matches(
        self,
        *,
        query_gate: ProspectiveQueryGateResult,
        evidence_pack: EvidencePack,
        signal: SignalIntent,
        order: OrderIntent,
    ) -> None:
        if not query_gate.model_run_eligible:
            raise PermissionError("experimental paper requires an eligible prospective Query Gate")
        if query_gate.execution_capability:
            raise ValueError("prospective Query Gate must remain execution-free")
        if self.query_gate_result_id != query_gate.result_id:
            raise ValueError("experimental paper admission binds a different Query Gate")
        if self.query_gate_result_hash != canonical_hash(query_gate.to_dict()):
            raise ValueError("experimental paper admission binds different Query Gate content")
        _assert_evidence_pack_binding(
            query_gate=query_gate,
            evidence_pack=evidence_pack,
            signal=signal,
        )
        if self.evidence_pack_id != evidence_pack.pack_id:
            raise ValueError("experimental paper admission binds a different Evidence Pack")
        if self.evidence_pack_hash != canonical_hash(evidence_pack.to_dict()):
            raise ValueError("experimental paper admission binds different Evidence Pack content")
        if self.signal_id != order.signal_id:
            raise ValueError("experimental paper admission binds a different Signal")
        if self.signal_id != signal.signal_id:
            raise ValueError("experimental paper admission binds a different Signal")
        if self.signal_intent_hash != canonical_hash(signal.to_dict()):
            raise ValueError("experimental paper admission binds different Signal content")
        if self.order_intent_hash != canonical_hash(order.to_dict()):
            raise ValueError("experimental paper admission binds a different Order Intent")
        if order.environment is not TradingEnvironment.PAPER:
            raise PermissionError("experimental paper admission is paper-only")


def prepare_experimental_paper_admission(
    *,
    query_gate: ProspectiveQueryGateResult,
    evidence_pack: EvidencePack,
    signal: SignalIntent,
    order: OrderIntent,
    created_at: datetime,
) -> ExperimentalPaperAdmission:
    if not query_gate.model_run_eligible:
        raise PermissionError("experimental paper requires an eligible prospective Query Gate")
    if query_gate.execution_capability:
        raise ValueError("prospective Query Gate must remain execution-free")
    if order.environment is not TradingEnvironment.PAPER:
        raise PermissionError("experimental Agent admission is paper-only")
    _assert_evidence_pack_binding(
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
    )
    if order.signal_id != signal.signal_id:
        raise ValueError("Order Intent does not bind the supplied Signal")
    if order.instrument_id != signal.instrument_id or order.side is not signal.side:
        raise ValueError("Order Intent instrument or side differs from the supplied Signal")
    if not signal.valid_from <= order.created_at < signal.expires_at:
        raise ValueError("Order Intent was not created while the Signal was valid")
    if order.expires_at > signal.expires_at:
        raise ValueError("Order Intent cannot outlive the Signal")
    _strict_utc(created_at, "experimental paper admission created_at")
    if created_at < order.created_at:
        raise ValueError("experimental paper admission cannot predate the Order Intent")

    query_gate_hash = canonical_hash(query_gate.to_dict())
    evidence_pack_hash = canonical_hash(evidence_pack.to_dict())
    signal_hash = canonical_hash(signal.to_dict())
    order_hash = canonical_hash(order.to_dict())
    core = {
        "schema_version": EXPERIMENTAL_PAPER_ADMISSION_SCHEMA,
        "query_gate_result_id": query_gate.result_id,
        "query_gate_result_hash": query_gate_hash,
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": evidence_pack_hash,
        "signal_id": signal.signal_id,
        "signal_intent_hash": signal_hash,
        "order_intent_hash": order_hash,
        "created_at": _timestamp(created_at),
        "strategy_admission_id": None,
        "claim_scope": EXPERIMENTAL_PAPER_CLAIM_SCOPE,
        "alpha_claim": False,
        "live_capability": False,
        "execution_authority": False,
    }
    return ExperimentalPaperAdmission(
        admission_id=f"experimental-paper-admission-{canonical_hash(core)}",
        query_gate_result_id=query_gate.result_id,
        query_gate_result_hash=query_gate_hash,
        evidence_pack_id=evidence_pack.pack_id,
        evidence_pack_hash=evidence_pack_hash,
        signal_id=signal.signal_id,
        signal_intent_hash=signal_hash,
        order_intent_hash=order_hash,
        created_at=created_at,
    )


def _assert_evidence_pack_binding(
    *,
    query_gate: ProspectiveQueryGateResult,
    evidence_pack: EvidencePack,
    signal: SignalIntent,
) -> None:
    if query_gate.evidence_pack_id != evidence_pack.pack_id:
        raise ValueError("Query Gate binds a different Evidence Pack")
    if evidence_pack.as_of != query_gate.barrier_at:
        raise ValueError("Evidence Pack cutoff differs from the Query Gate barrier")
    if signal.event_id != evidence_pack.event_id:
        raise ValueError("Signal event differs from the Evidence Pack")
    evidence_ids = frozenset(item.evidence_id for item in evidence_pack.evidence)
    if not set(signal.evidence_refs) <= evidence_ids:
        raise ValueError("Signal evidence_refs are not all in the Evidence Pack")
    if signal.instrument_id not in evidence_pack.allowed_targets:
        raise ValueError("Signal instrument is not an allowed Evidence Pack target")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be lowercase SHA-256 text")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
