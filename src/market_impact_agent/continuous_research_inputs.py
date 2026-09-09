"""Daily, outcome-blind research projections for the registered historical study."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import cast

from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import research_evidence_metadata

_QUESTION = (
    "Using only information visible at the cutoff, explain the incremental economic impact, "
    "affected industries and companies, what is already priced in, and counterevidence. "
    "Choose a defensible direction and allowed thesis horizon. Research may discover A-share "
    "stocks or equity ETFs beyond the initial seeds; missing information is an explicit gap. "
    "Account suitability and execution eligibility are assessed separately."
)
_CONTEXT = frozenset(
    {"official-policy-context", "macro-vintage", "timestamped-news-corpus", "positioning-flow"}
)


async def continuous_event_facts(
    repository: FrozenResearchRepository, *, version: str = "source-v2"
) -> tuple[str, ...]:
    """Stable source fact IDs and a frozen 3% completed-session price threshold.

    Context envelope/cutoff changes alone are not new facts. The coordinator
    consumes these identities across thesis renewals and combines same-cutoff causes.
    """
    if version not in {"legacy-v1", "source-v2"}:
        raise ValueError("unknown continuous fact projection")
    facts: set[str] = set()
    for reference in repository.evidence_pack.evidence:
        loaded = cast(
            dict[str, object],
            await repository.read_evidence({"evidence_id": reference.evidence_id}),
        )
        value = loaded["document"]
        if not isinstance(value, dict):
            continue
        document = cast(dict[str, object], value)
        if version == "source-v2" or reference.evidence_id in _CONTEXT:
            for key in ("articles", "records", "rows"):
                for raw in cast(list[object], document.get(key, [])):
                    if not isinstance(raw, dict):
                        continue
                    item = cast(dict[str, object], raw)
                    identity = item.get("evidence_record_id")
                    available = item.get("available_at")
                    if not isinstance(identity, str) or not isinstance(available, str):
                        continue
                    at = datetime.fromisoformat(available.replace("Z", "+00:00"))
                    if at.tzinfo is not None and at <= repository.evidence_pack.as_of:
                        facts.add("qualified-fact:" + canonical_hash(identity))
        price_history = (
            research_evidence_metadata(reference, document, repository.evidence_pack.as_of)[
                "category"
            ]
            == "price_session_history"
            if version == "source-v2"
            else reference.evidence_id.startswith("price-")
        )
        if price_history:
            raw_rows = cast(list[object], document.get("rows", []))
            rows = [
                cast(list[object], row)
                for row in raw_rows
                if isinstance(row, list) and len(cast(list[object], row)) >= 3
            ]
            if len(rows) >= 2 and rows[-1][2] is not None and rows[-2][2] is not None:
                previous, current = Decimal(str(rows[-2][2])), Decimal(str(rows[-1][2]))
                if previous > 0 and abs(current / previous - 1) >= Decimal("0.03"):
                    facts.add(
                        "completed-price-move-3pct:"
                        + canonical_hash({"symbol": document["symbol"], "date": rows[-1][0]})
                    )
    return tuple(sorted(facts))


async def continuous_research_repository(
    *,
    market: HistoricalAShareInputs,
    cutoff: datetime,
    event_scope: str,
    symbols: tuple[str, ...],
    previously_qualified_context: tuple[FrozenResearchRepository, ...] = (),
) -> FrozenResearchRepository:
    """Reuse only previously qualified contextual records, never case labels/outcomes.

    Sparse old context is carried with its original availability and a typed
    freshness gap. Current industry membership is never projected into history.
    All newly captured prices remain explicitly modeled PIT, not original receipt.
    """
    if not event_scope or not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("continuous research requires one scope and unique research symbols")
    references: list[EvidenceReference] = []
    documents: dict[str, object] = {}
    gaps = [
        "Opened historical development inputs use an explicit modeled-PIT policy; strict PIT "
        "and unseen investment effectiveness are not established.",
        "Historical industry membership and business exposure require separately qualified "
        "records; current constituents are not historical evidence.",
    ]
    for symbol in symbols:
        projection = market.research_series(symbol, cutoff)
        rows = cast(list[dict[str, object]], projection["rows"])
        evidence_id = "price-" + canonical_hash(symbol)[:12]
        # Full source/version proofs stay in CAS, while the model receives the
        # economic observations rather than dozens of repeated opaque hashes.
        proof = market.store.artifacts.put_json(projection)
        document: dict[str, object] = {
            "symbol": symbol,
            "policy_id": market.policy.policy_id,
            "pit_lane": market.policy.lane,
            "strict_pit_accepted": False,
            "fields": ["trade_date", "raw_close", "cutoff_adjusted_close", "volume_lots"],
            "field_semantics": {
                "trade_date": "completed exchange-session date",
                "raw_close": "actual quoted session close; do not mix it with adjusted closes",
                "cutoff_adjusted_close": (
                    "research-only close normalized with factors through this cutoff; compare "
                    "returns only on this consistent adjusted-close basis"
                ),
                "volume_lots": "source-reported session volume in lots",
            },
            "rows": [
                [
                    row[key]
                    for key in ("trade_date", "raw_close", "cutoff_adjusted_close", "volume_lots")
                ]
                for row in rows
            ],
            "source_projection_hash": proof.content_hash,
            "source_projection_proof": (
                "source_projection_hash identifies the complete source and factor proof "
                "retained in CAS"
            ),
            "gaps": projection["gaps"],
        }
        documents[evidence_id] = document
        references.append(
            EvidenceReference(
                evidence_id,
                "completed-session-price-history",
                "sha256:" + proof.content_hash,
                EvidenceTier.REGULATED,
                cutoff,
                canonical_hash(document),
                "Completed-session prices: raw_close is the actual quote and "
                "cutoff_adjusted_close is the consistent return-comparison basis. Prices do not "
                "establish execution eligibility or original historical receipt.",
            )
        )
        gaps.extend(f"{symbol}: {gap}" for gap in cast(list[str], projection["gaps"]))
    eligible = [item for item in previously_qualified_context if item.evidence_pack.as_of <= cutoff]
    if eligible:
        prior = max(eligible, key=lambda item: item.evidence_pack.as_of)
        for reference in prior.evidence_pack.evidence:
            if reference.evidence_id not in _CONTEXT:
                continue
            loaded = cast(
                Mapping[str, object],
                await prior.read_evidence({"evidence_id": reference.evidence_id}),
            )
            context_document = loaded["document"]
            source_proof = market.store.artifacts.put_json(
                {
                    "source_pack": prior.evidence_pack.to_dict(),
                    "source_reference": reference.to_dict(),
                    "document_hash": canonical_hash(context_document),
                }
            )
            documents[reference.evidence_id] = context_document
            references.append(
                EvidenceReference(
                    reference.evidence_id,
                    reference.claim_id,
                    "sha256:" + source_proof.content_hash,
                    reference.source_tier,
                    reference.available_at,
                    reference.content_hash,
                    reference.summary,
                )
            )
        if prior.evidence_pack.as_of < cutoff:
            gaps.append(
                "Context is carried from the latest qualified earlier checkpoint at "
                + prior.evidence_pack.as_of.isoformat()
                + "; absence of newer news has not been established."
            )
    if not any(item.evidence_id in _CONTEXT for item in references):
        gaps.append("Bounded news, policy and company disclosure coverage is missing.")
    pack = EvidencePack.build(
        event_id="continuous-research-" + canonical_hash(event_scope),
        as_of=cutoff,
        research_question=_QUESTION,
        evidence=tuple(references),
        pattern_packs=(),
        allowed_targets=("A-share-equity-universe", *symbols),
        data_gaps=tuple(dict.fromkeys(gaps)),
    )
    return FrozenResearchRepository(
        evidence_pack=pack, evidence_documents=documents, pattern_packs={}
    )
