import asyncio
import copy
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import (
    EvidencePack,
    PatternEntry,
    PatternPack,
    PatternPackReference,
    canonical_hash,
    evidence_pack_from_dict,
    pattern_pack_from_dict,
)
from market_impact_agent.agent_runtime import SkillRegistry
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.method_benchmark import (
    BenchmarkSplit,
    HistoricalEvidenceManifest,
    HistoricalEvidenceVersion,
    IdentityMaskingPolicy,
    LatencyCalibration,
    MaskedAgentInputManifest,
    MethodQualityEvaluationSpecification,
    ProvenanceTrustStatus,
    SourceVersionReceipt,
    load_historical_evidence_manifest,
    load_masked_agent_input_manifest,
    load_method_quality_benchmark,
    load_method_quality_evaluation_specification,
)
from market_impact_agent.method_evaluation import (
    MarketSnapshot,
    market_snapshot_from_dict,
    validate_case_value,
    validate_outcome_result,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.observations import AvailabilityBasis
from market_impact_agent.research_methods import (
    MethodArm,
    ResearchContext,
    ResearchMethodRouter,
    load_research_method_catalog,
)

ROOT = Path(__file__).parents[1]


def at(hour: int) -> datetime:
    return datetime(2026, 1, 15, hour, tzinfo=UTC)


def evaluation_snapshot() -> MarketSnapshot:
    evaluation = evaluation_specification()
    evaluation_hash = evaluation.specification_hash
    source_hash = "f" * 64
    price_rows: list[dict[str, object]] = []
    benchmark_rows: list[dict[str, object]] = []
    for session, instrument_open, instrument_close, benchmark_open, benchmark_close in (
        ("2026-01-16", "10", "10.5", "100", "101"),
        ("2026-01-17", "11", "12", "101", "102"),
    ):
        common = {
            "session_date": session,
            "high": instrument_close,
            "low": instrument_open,
            "volume": "10000",
            "adjustment_factor": "1",
            "trade_status": "open",
            "limit_up": "20",
            "limit_down": "5",
            "source_version_id": "market-v1",
        }
        price_rows.append(
            {
                "target_id": "target-1",
                **common,
                "open": instrument_open,
                "close": instrument_close,
            }
        )
        benchmark_rows.append(
            {
                "session_date": session,
                "open": benchmark_open,
                "high": benchmark_close,
                "low": benchmark_open,
                "close": benchmark_close,
                "volume": "10000",
                "adjustment_factor": "1",
                "trade_status": "open",
                "limit_up": None,
                "limit_down": None,
                "source_version_id": "market-v1",
            }
        )
    core = {
        "schema_version": "market-impact.method-quality-market-snapshot.v1",
        "case_alias": "case-1",
        "case_cutoff_session": "2026-01-15",
        "case_as_of": "2026-01-15T00:00:00Z",
        "evaluation_specification_id": evaluation.specification_id,
        "evaluation_specification_hash": evaluation_hash,
        "created_at": "2026-01-15T00:00:00Z",
        "sealed_before_agent_runs": True,
        "source_vintage_id": f"source-vintage-{source_hash}",
        "source_vintage_hash": source_hash,
        "venue": "TEST",
        "timezone": "UTC",
        "currency": "CNY",
        "calendar_id": "test-calendar-v1",
        "calendar_sessions": ["2026-01-15", "2026-01-16", "2026-01-17"],
        "corporate_actions": [],
        "instrument_prices": price_rows,
        "benchmark_id": "benchmark-1",
        "benchmark_prices": benchmark_rows,
        "fee_schedule": [
            {
                "fee_id": "entry-fee",
                "effective_from": "2026-01-01",
                "effective_through": None,
                "side": "entry",
                "component": "commission",
                "rate": "0",
                "minimum_amount": "0",
                "rounding_quantum": "0.01",
                "rounding_mode": "half_up",
            },
            {
                "fee_id": "exit-fee",
                "effective_from": "2026-01-01",
                "effective_through": None,
                "side": "exit",
                "component": "commission",
                "rate": "0",
                "minimum_amount": "0",
                "rounding_quantum": "0.01",
                "rounding_mode": "half_up",
            },
        ],
        "venue_rules": [
            {
                "rule_id": "venue-v1",
                "effective_from": "2026-01-01",
                "effective_through": None,
                "board_lot_size": 100,
                "price_tick": "0.01",
                "price_limit_basis": "snapshot_limits",
                "suspension_fill_policy": "no_fill",
                "missing_bar_policy": "missing_zero_value_in_denominator",
            }
        ],
        "execution_capability": "none",
    }
    return market_snapshot_from_dict(
        {
            **core,
            "snapshot_id": f"method-quality-market-snapshot-{canonical_hash(core)}",
        }
    )


def filled_result() -> dict[str, object]:
    core: dict[str, object] = {
        "case_alias": "case-1",
        "replicate": 1,
        "arm": "general_methods",
        "artifact_id": "judgment-" + "a" * 64,
        "artifact_hash": "b" * 64,
        "target_id": "target-1",
        "horizon_sessions": 1,
        "direction": "up",
        "fill_status": "filled",
        "entry_session": "2026-01-16",
        "entry_price": "10",
        "quantity": "100000",
        "entry_reference_value": "1000000",
        "exit_session": "2026-01-17",
        "exit_price": "12",
        "exit_reference_value": "1200000",
        "cost_components": [
            {
                "fee_id": "entry-fee",
                "side": "entry",
                "component": "commission",
                "reference_value": "1000000",
                "amount": "0.00",
            },
            {
                "fee_id": "exit-fee",
                "side": "exit",
                "component": "commission",
                "reference_value": "1200000",
                "amount": "0.00",
            },
        ],
        "total_cost_proxy_amount": "0.00",
        "price_move_ratio": "0.2",
        "directional_score": "0.2",
        "cost_proxy": "0.00",
        "benchmark_move_ratio": "0.02",
        "benchmark_adjusted_directional_score": "0.18",
    }
    return {**core, "result_id": f"method-quality-result-{canonical_hash(core)}"}


def rematerialize_snapshot(core: dict[str, object]) -> MarketSnapshot:
    return market_snapshot_from_dict(
        {
            **core,
            "snapshot_id": f"method-quality-market-snapshot-{canonical_hash(core)}",
        }
    )


def rematerialize_result(result: dict[str, object]) -> dict[str, object]:
    core = {key: value for key, value in result.items() if key != "result_id"}
    return {**core, "result_id": f"method-quality-result-{canonical_hash(core)}"}


def zero_result(status: str = "no_fill") -> dict[str, object]:
    result = filled_result()
    result.update(
        {
            "fill_status": status,
            "entry_session": None,
            "entry_price": None,
            "quantity": "0",
            "entry_reference_value": None,
            "exit_session": None,
            "exit_price": None,
            "exit_reference_value": None,
            "cost_components": [],
            "total_cost_proxy_amount": "0",
            "price_move_ratio": "0",
            "directional_score": "0",
            "cost_proxy": "0",
            "benchmark_move_ratio": "0",
            "benchmark_adjusted_directional_score": "0",
        }
    )
    return rematerialize_result(result)


def version(
    evidence_id: str = "evidence-1",
    *,
    claim_id: str = "claim-1",
    availability_basis: AvailabilityBasis = AvailabilityBasis.SOURCE_REPORTED,
    supersedes_id: str | None = None,
    available_hour: int = 3,
    retrieved_hour: int = 4,
    source_updated_hour: int | None = None,
) -> HistoricalEvidenceVersion:
    calibration = None
    if availability_basis is AvailabilityBasis.MODELED_LATENCY:
        calibration_core = {
            "schema_version": "market-impact.latency-calibration.v1",
            "source_class": "test",
            "provider_id": "test-provider",
            "archive_id": "test-archive",
            "calibration_version": "v1",
            "sample_hash": "c" * 64,
            "sample_count": 10,
            "calibrated_at": at(0).isoformat().replace("+00:00", "Z"),
            "availability_offset_seconds": 3600,
            "trust_status": "synthetic_contract_only",
        }
        calibration = LatencyCalibration(
            calibration_id=f"latency-calibration-{canonical_hash(calibration_core)}",
            source_class="test",
            provider_id="test-provider",
            archive_id="test-archive",
            calibration_version="v1",
            sample_hash="c" * 64,
            sample_count=10,
            calibrated_at=at(0),
            availability_offset_seconds=3600,
            trust_status=ProvenanceTrustStatus.SYNTHETIC_CONTRACT_ONLY,
        )
    receipt_core = {
        "schema_version": "market-impact.source-version-receipt.v1",
        "source_ref": f"synthetic://{evidence_id}",
        "provider_id": "test-provider",
        "archive_id": "test-archive",
        "archive_version": "v1",
        "source_version_id": f"source-{evidence_id}",
        "raw_content_hash": "a" * 64,
        "extracted_content_hash": "a" * 64,
        "published_at": at(2).isoformat().replace("+00:00", "Z"),
        "source_updated_at": (
            None
            if source_updated_hour is None
            else at(source_updated_hour).isoformat().replace("+00:00", "Z")
        ),
        "retrieved_at": at(retrieved_hour).isoformat().replace("+00:00", "Z"),
        "available_at": at(available_hour).isoformat().replace("+00:00", "Z"),
        "availability_basis": availability_basis.value,
        "latency_calibration": None if calibration is None else calibration.to_dict(),
        "trust_status": "synthetic_contract_only",
    }
    receipt = SourceVersionReceipt(
        receipt_id=f"source-version-receipt-{canonical_hash(receipt_core)}",
        source_ref=f"synthetic://{evidence_id}",
        provider_id="test-provider",
        archive_id="test-archive",
        archive_version="v1",
        source_version_id=f"source-{evidence_id}",
        raw_content_hash="a" * 64,
        extracted_content_hash="a" * 64,
        published_at=at(2),
        source_updated_at=None if source_updated_hour is None else at(source_updated_hour),
        retrieved_at=at(retrieved_hour),
        available_at=at(available_hour),
        availability_basis=availability_basis,
        latency_calibration=calibration,
        trust_status=ProvenanceTrustStatus.SYNTHETIC_CONTRACT_ONLY,
    )
    return HistoricalEvidenceVersion(
        evidence_id=evidence_id,
        claim_id=claim_id,
        source_version_id=f"source-{evidence_id}",
        occurred_at=at(1),
        published_at=at(2),
        source_updated_at=None if source_updated_hour is None else at(source_updated_hour),
        available_at=at(available_hour),
        retrieved_at=at(retrieved_hour),
        availability_basis=availability_basis,
        source_version_receipt=receipt,
        supersedes_id=supersedes_id,
        content_hash="a" * 64,
    )


def test_historical_example_binds_exact_point_in_time_evidence_pack() -> None:
    manifest = load_historical_evidence_manifest(
        ROOT / "examples/research/synthetic-energy-historical-evidence-v1.json"
    )
    pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/evidence-pack.json").read_text(encoding="utf-8")
        )
    )

    manifest.validate_against(pack)

    assert manifest.split is BenchmarkSplit.DEVELOPMENT
    assert len(manifest.evidence_versions) == 4
    assert not manifest.outcomes_opened


def evaluation_specification() -> MethodQualityEvaluationSpecification:
    return load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v1.json"
    )


def test_outcome_result_rejects_inconsistent_directional_score() -> None:
    result = filled_result()
    result["directional_score"] = "0.21"
    result_core = {key: value for key, value in result.items() if key != "result_id"}
    result["result_id"] = f"method-quality-result-{canonical_hash(result_core)}"

    with pytest.raises(ValueError, match="directional score or cost proxy equation"):
        validate_outcome_result(
            result,
            snapshot=evaluation_snapshot(),
            specification=evaluation_specification(),
        )


def test_market_snapshot_contract_example_matches_public_schema() -> None:
    errors = validate_agent_contract(
        evaluation_snapshot().to_dict(),
        "method-quality-market-snapshot.schema.json",
    )

    assert errors == ()

    empty_fees = evaluation_snapshot().to_dict()
    empty_fees["fee_schedule"] = []
    assert validate_agent_contract(
        empty_fees,
        "method-quality-market-snapshot.schema.json",
    )

    empty_rules = evaluation_snapshot().to_dict()
    empty_rules["venue_rules"] = []
    assert validate_agent_contract(
        empty_rules,
        "method-quality-market-snapshot.schema.json",
    )


def test_outcome_result_requires_the_snapshot_bound_evaluation_specification() -> None:
    core = evaluation_snapshot().core_dict()
    other_hash = "d" * 64
    core["evaluation_specification_hash"] = other_hash
    core["evaluation_specification_id"] = f"method-quality-evaluation-{other_hash}"
    with pytest.raises(ValueError, match="does not bind the supplied evaluation specification"):
        validate_outcome_result(
            filled_result(),
            snapshot=rematerialize_snapshot(core),
            specification=evaluation_specification(),
        )


def test_outcome_result_rejects_arbitrary_no_fill_and_suspended_fill() -> None:
    with pytest.raises(ValueError, match="fill status does not match"):
        validate_outcome_result(
            zero_result(),
            snapshot=evaluation_snapshot(),
            specification=evaluation_specification(),
        )

    core = evaluation_snapshot().core_dict()
    prices = copy.deepcopy(core["instrument_prices"])
    assert isinstance(prices, list)
    assert isinstance(prices[0], dict)
    prices[0]["trade_status"] = "suspended"
    core["instrument_prices"] = prices
    suspended = rematerialize_snapshot(core)
    with pytest.raises(ValueError, match="fill status does not match"):
        validate_outcome_result(
            filled_result(),
            snapshot=suspended,
            specification=evaluation_specification(),
        )

    core = evaluation_snapshot().core_dict()
    prices = cast(list[dict[str, object]], copy.deepcopy(core["instrument_prices"]))
    for row in prices:
        row["trade_status"] = "missing"
    core["instrument_prices"] = prices
    missing = rematerialize_snapshot(core)
    validate_outcome_result(
        zero_result("missing_market_data"),
        snapshot=missing,
        specification=evaluation_specification(),
    )
    with pytest.raises(ValueError, match="fill status does not match"):
        validate_outcome_result(
            zero_result("no_fill"),
            snapshot=missing,
            specification=evaluation_specification(),
        )


def test_outcome_result_rejects_nonmaximal_board_lot_quantity() -> None:
    result = filled_result()
    result["quantity"] = "99900"
    result["entry_reference_value"] = "999000"
    result["exit_reference_value"] = "1198800"
    costs = copy.deepcopy(result["cost_components"])
    assert isinstance(costs, list)
    assert isinstance(costs[0], dict) and isinstance(costs[1], dict)
    costs[0]["reference_value"] = "999000"
    costs[1]["reference_value"] = "1198800"
    result["cost_components"] = costs
    result = rematerialize_result(result)

    with pytest.raises(ValueError, match="largest affordable"):
        validate_outcome_result(
            result,
            snapshot=evaluation_snapshot(),
            specification=evaluation_specification(),
        )


def test_outcome_result_rejects_later_favorable_entry() -> None:
    core = evaluation_snapshot().core_dict()
    sessions = cast(list[object], core["calendar_sessions"])
    instrument = cast(list[dict[str, object]], core["instrument_prices"])
    benchmark = cast(list[dict[str, object]], core["benchmark_prices"])
    sessions.append("2026-01-18")
    instrument.append(
        {
            "target_id": "target-1",
            "session_date": "2026-01-18",
            "open": "12",
            "high": "13",
            "low": "12",
            "close": "13",
            "volume": "10000",
            "adjustment_factor": "1",
            "trade_status": "open",
            "limit_up": "20",
            "limit_down": "5",
            "source_version_id": "market-v1",
        }
    )
    benchmark.append(
        {
            "session_date": "2026-01-18",
            "open": "102",
            "high": "103",
            "low": "102",
            "close": "103",
            "volume": "10000",
            "adjustment_factor": "1",
            "trade_status": "open",
            "limit_up": None,
            "limit_down": None,
            "source_version_id": "market-v1",
        }
    )
    snapshot = rematerialize_snapshot(core)
    result = filled_result()
    result.update(
        {
            "entry_session": "2026-01-17",
            "entry_price": "11",
            "quantity": "90900",
            "entry_reference_value": "999900",
            "exit_session": "2026-01-18",
            "exit_price": "13",
            "exit_reference_value": "1181700",
        }
    )
    result = rematerialize_result(result)

    with pytest.raises(ValueError, match="derived entry"):
        validate_outcome_result(
            result,
            snapshot=snapshot,
            specification=evaluation_specification(),
        )

    wrong_exit = filled_result()
    wrong_exit.update(
        {
            "exit_session": "2026-01-18",
            "exit_price": "13",
            "exit_reference_value": "1300000",
        }
    )
    with pytest.raises(ValueError, match="exact-horizon exit"):
        validate_outcome_result(
            rematerialize_result(wrong_exit),
            snapshot=snapshot,
            specification=evaluation_specification(),
        )


def test_market_snapshot_rejects_tick_limit_and_overlapping_effective_rules() -> None:
    core = evaluation_snapshot().core_dict()
    prices = copy.deepcopy(core["instrument_prices"])
    assert isinstance(prices, list) and isinstance(prices[0], dict)
    prices[0]["open"] = "10.005"
    core["instrument_prices"] = prices
    with pytest.raises(ValueError, match="tick or price limits"):
        rematerialize_snapshot(core)

    core = evaluation_snapshot().core_dict()
    prices = copy.deepcopy(core["instrument_prices"])
    assert isinstance(prices, list) and isinstance(prices[0], dict)
    prices[0]["open"] = "21"
    prices[0]["high"] = "21"
    core["instrument_prices"] = prices
    with pytest.raises(ValueError, match="tick or price limits"):
        rematerialize_snapshot(core)

    core = evaluation_snapshot().core_dict()
    rules = cast(list[dict[str, object]], copy.deepcopy(core["venue_rules"]))
    rules.append({**rules[0], "rule_id": "venue-overlap"})
    core["venue_rules"] = rules
    with pytest.raises(ValueError, match="effective ranges overlap"):
        rematerialize_snapshot(core)

    core = evaluation_snapshot().core_dict()
    fees = cast(list[dict[str, object]], copy.deepcopy(core["fee_schedule"]))
    fees.append({**fees[0], "fee_id": "entry-overlap"})
    core["fee_schedule"] = fees
    with pytest.raises(ValueError, match="effective ranges overlap"):
        rematerialize_snapshot(core)


def test_market_snapshot_rejects_no_effective_fee_or_venue_rule() -> None:
    core = evaluation_snapshot().core_dict()
    fees = cast(list[dict[str, object]], copy.deepcopy(core["fee_schedule"]))
    for fee in fees:
        fee["effective_from"] = "2027-01-01"
    core["fee_schedule"] = fees
    with pytest.raises(ValueError, match="requires effective entry and exit fees"):
        rematerialize_snapshot(core)

    core = evaluation_snapshot().core_dict()
    rules = cast(list[dict[str, object]], copy.deepcopy(core["venue_rules"]))
    rules[0]["effective_through"] = "2026-01-15"
    core["venue_rules"] = rules
    with pytest.raises(ValueError, match="exactly one effective venue rule"):
        rematerialize_snapshot(core)


def test_entry_search_does_not_fill_on_fourth_post_cutoff_session() -> None:
    core = evaluation_snapshot().core_dict()
    sessions = cast(list[object], core["calendar_sessions"])
    instrument = cast(list[dict[str, object]], core["instrument_prices"])
    benchmark = cast(list[dict[str, object]], core["benchmark_prices"])
    sessions.extend(["2026-01-18", "2026-01-19", "2026-01-20"])
    for row in instrument:
        row["trade_status"] = "suspended"
    for session, open_price, close_price in (
        ("2026-01-18", "12", "12"),
        ("2026-01-19", "13", "13.5"),
        ("2026-01-20", "14", "15"),
    ):
        instrument.append(
            {
                "target_id": "target-1",
                "session_date": session,
                "open": open_price,
                "high": close_price,
                "low": open_price,
                "close": close_price,
                "volume": "10000",
                "adjustment_factor": "1",
                "trade_status": "suspended" if session == "2026-01-18" else "open",
                "limit_up": "20",
                "limit_down": "5",
                "source_version_id": "market-v1",
            }
        )
        benchmark.append(
            {
                "session_date": session,
                "open": "103",
                "high": "104",
                "low": "103",
                "close": "104",
                "volume": "10000",
                "adjustment_factor": "1",
                "trade_status": "open",
                "limit_up": None,
                "limit_down": None,
                "source_version_id": "market-v1",
            }
        )
    snapshot = rematerialize_snapshot(core)
    result = filled_result()
    result.update(
        {
            "entry_session": "2026-01-19",
            "entry_price": "13",
            "quantity": "76900",
            "entry_reference_value": "999700",
            "exit_session": "2026-01-20",
            "exit_price": "15",
            "exit_reference_value": "1153500",
        }
    )
    result = rematerialize_result(result)

    with pytest.raises(ValueError, match="fill status does not match"):
        validate_outcome_result(
            result,
            snapshot=snapshot,
            specification=evaluation_specification(),
        )


def test_down_direction_is_only_a_directional_score_multiplier() -> None:
    result = filled_result()
    result.update(
        {
            "direction": "down",
            "directional_score": "-0.2",
            "benchmark_adjusted_directional_score": "-0.18",
        }
    )
    validate_outcome_result(
        rematerialize_result(result),
        snapshot=evaluation_snapshot(),
        specification=evaluation_specification(),
    )


def test_case_value_rejects_favorable_target_or_horizon_selection() -> None:
    first = filled_result()
    second = {**first, "result_id": "method-quality-result-" + "c" * 64}
    second["benchmark_adjusted_directional_score"] = "0.02"
    favorable_only = {
        "case_alias": "case-1",
        "replicate": 1,
        "arm": "general_methods",
        "status": "valued",
        "component_result_ids": [first["result_id"], second["result_id"]],
        "value": "0.18",
    }

    with pytest.raises(ValueError, match="equal-weight mean"):
        validate_case_value(
            favorable_only,
            component_results=(first, second),
        )


def test_historical_modeled_availability_requires_content_identified_calibration() -> None:
    item = version(availability_basis=AvailabilityBasis.MODELED_LATENCY)
    assert item.source_version_receipt.latency_calibration is not None


def test_synthetic_chronology_cannot_enter_retrospective_holdout() -> None:
    manifest = load_historical_evidence_manifest(
        ROOT / "examples/research/synthetic-energy-historical-evidence-v1.json"
    )

    with pytest.raises(ValueError, match="admission is unavailable in v1"):
        replace(manifest, split=BenchmarkSplit.RETROSPECTIVE_HOLDOUT)


def test_retrospective_holdout_requires_consistent_identity_aliases() -> None:
    item = version()
    core = {
        "schema_version": "market-impact.historical-evidence-manifest.v1",
        "case_alias": "case-1",
        "split": "retrospective_holdout",
        "evidence_pack_id": "evidence-pack-1",
        "evidence_pack_hash": "b" * 64,
        "as_of": at(5).isoformat().replace("+00:00", "Z"),
        "identity_masking": "none",
        "masked_agent_input_manifest_id": None,
        "masked_agent_input_manifest_hash": None,
        "provenance_trust_status": "synthetic_contract_only",
        "outcomes_opened": False,
        "evidence_versions": [item.to_dict()],
        "external_tool_access": False,
        "outcome_memory_policy": "train_only_frozen_pattern_packs",
        "execution_capability": "none",
    }
    with pytest.raises(ValueError, match="consistent identity aliases"):
        HistoricalEvidenceManifest(
            manifest_id=f"historical-evidence-{canonical_hash(core)}",
            case_alias="case-1",
            split=BenchmarkSplit.RETROSPECTIVE_HOLDOUT,
            evidence_pack_id="evidence-pack-1",
            evidence_pack_hash="b" * 64,
            as_of=at(5),
            identity_masking=IdentityMaskingPolicy.NONE,
            masked_agent_input_manifest_id=None,
            masked_agent_input_manifest_hash=None,
            provenance_trust_status=ProvenanceTrustStatus.SYNTHETIC_CONTRACT_ONLY,
            outcomes_opened=False,
            evidence_versions=(item,),
            external_tool_access=False,
            outcome_memory_policy="train_only_frozen_pattern_packs",
            execution_capability="none",
        )


def test_recomputed_untrusted_receipt_and_manifest_cannot_admit_retrospective_holdout() -> None:
    item = version()
    original = item.source_version_receipt
    receipt_core = original.core_dict()
    receipt_core["trust_status"] = "contract_validated_untrusted"
    receipt = SourceVersionReceipt(
        receipt_id=f"source-version-receipt-{canonical_hash(receipt_core)}",
        source_ref=original.source_ref,
        provider_id=original.provider_id,
        archive_id=original.archive_id,
        archive_version=original.archive_version,
        source_version_id=original.source_version_id,
        raw_content_hash=original.raw_content_hash,
        extracted_content_hash=original.extracted_content_hash,
        published_at=original.published_at,
        source_updated_at=original.source_updated_at,
        retrieved_at=original.retrieved_at,
        available_at=original.available_at,
        availability_basis=original.availability_basis,
        latency_calibration=original.latency_calibration,
        trust_status=ProvenanceTrustStatus.CONTRACT_VALIDATED_UNTRUSTED,
    )
    bound_item = replace(item, source_version_receipt=receipt)
    manifest_core = {
        "schema_version": "market-impact.historical-evidence-manifest.v1",
        "case_alias": "case-1",
        "split": "retrospective_holdout",
        "evidence_pack_id": "evidence-pack-1",
        "evidence_pack_hash": "b" * 64,
        "as_of": at(5).isoformat().replace("+00:00", "Z"),
        "identity_masking": "consistent_aliases",
        "masked_agent_input_manifest_id": "masked-agent-input-" + "d" * 64,
        "masked_agent_input_manifest_hash": "d" * 64,
        "provenance_trust_status": "contract_validated_untrusted",
        "outcomes_opened": False,
        "evidence_versions": [bound_item.to_dict()],
        "external_tool_access": False,
        "outcome_memory_policy": "train_only_frozen_pattern_packs",
        "execution_capability": "none",
    }
    with pytest.raises(ValueError, match="admission is unavailable in v1"):
        HistoricalEvidenceManifest(
            manifest_id=f"historical-evidence-{canonical_hash(manifest_core)}",
            case_alias="case-1",
            split=BenchmarkSplit.RETROSPECTIVE_HOLDOUT,
            evidence_pack_id="evidence-pack-1",
            evidence_pack_hash="b" * 64,
            as_of=at(5),
            identity_masking=IdentityMaskingPolicy.CONSISTENT_ALIASES,
            masked_agent_input_manifest_id="masked-agent-input-" + "d" * 64,
            masked_agent_input_manifest_hash="d" * 64,
            provenance_trust_status=ProvenanceTrustStatus.CONTRACT_VALIDATED_UNTRUSTED,
            outcomes_opened=False,
            evidence_versions=(bound_item,),
            external_tool_access=False,
            outcome_memory_policy="train_only_frozen_pattern_packs",
            execution_capability="none",
        )


def test_historical_revision_lineage_rejects_cross_claim_and_future_evidence() -> None:
    original = version()
    revision = version(
        "evidence-2",
        claim_id="other-claim",
        supersedes_id="evidence-1",
        available_hour=4,
        retrieved_hour=5,
    )

    with pytest.raises(ValueError, match="retain claim_id"):
        HistoricalEvidenceManifest(
            manifest_id="historical-evidence-" + "c" * 64,
            case_alias="case-1",
            split=BenchmarkSplit.DEVELOPMENT,
            evidence_pack_id="evidence-pack-1",
            evidence_pack_hash="b" * 64,
            as_of=at(6),
            identity_masking=IdentityMaskingPolicy.CONSISTENT_ALIASES,
            masked_agent_input_manifest_id="masked-agent-input-" + "d" * 64,
            masked_agent_input_manifest_hash="d" * 64,
            provenance_trust_status=ProvenanceTrustStatus.SYNTHETIC_CONTRACT_ONLY,
            outcomes_opened=False,
            evidence_versions=(original, revision),
            external_tool_access=False,
            outcome_memory_policy="train_only_frozen_pattern_packs",
            execution_capability="none",
        )

    with pytest.raises(ValueError, match="future-available evidence"):
        replace(
            load_historical_evidence_manifest(
                ROOT / "examples/research/synthetic-energy-historical-evidence-v1.json"
            ),
            as_of=at(0),
        )


def test_historical_manifest_rejects_source_version_updated_after_as_of() -> None:
    item = version(source_updated_hour=6, retrieved_hour=7)

    with pytest.raises(ValueError, match="source version updated after as_of"):
        HistoricalEvidenceManifest(
            manifest_id="historical-evidence-" + "c" * 64,
            case_alias="case-1",
            split=BenchmarkSplit.DEVELOPMENT,
            evidence_pack_id="evidence-pack-1",
            evidence_pack_hash="b" * 64,
            as_of=at(5),
            identity_masking=IdentityMaskingPolicy.CONSISTENT_ALIASES,
            masked_agent_input_manifest_id="masked-agent-input-" + "d" * 64,
            masked_agent_input_manifest_hash="d" * 64,
            provenance_trust_status=ProvenanceTrustStatus.SYNTHETIC_CONTRACT_ONLY,
            outcomes_opened=False,
            evidence_versions=(item,),
            external_tool_access=False,
            outcome_memory_policy="train_only_frozen_pattern_packs",
            execution_capability="none",
        )


def test_method_quality_registration_is_canonical_and_bound() -> None:
    registration = load_method_quality_benchmark(
        ROOT / "examples/calibration/method-quality-benchmark-v1.json"
    )
    catalog = load_research_method_catalog(
        ROOT / "examples/research/research-method-catalog-v2.json"
    )
    profile = load_model_provider_profile(ROOT / "examples/providers/minimax-m3-research-v1.json")
    evaluation = load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v1.json"
    )

    registration.validate_against(
        catalog=catalog,
        provider_profile=profile,
        skills=SkillRegistry(ROOT / "skills"),
        evaluation_specification=evaluation,
    )

    assert registration.retrospective_holdout_case_count == 24
    assert sum(item.target_case_count for item in registration.strata) == 24
    assert registration.suites[1].minimum_case_count == 8
    assert not registration.outcomes_opened


def test_method_quality_v2_uses_event_cases_as_independent_units() -> None:
    registration = load_method_quality_benchmark(
        ROOT / "examples/calibration/method-quality-benchmark-v2.json"
    )
    catalog = load_research_method_catalog(
        ROOT / "examples/research/research-method-catalog-v2.json"
    )
    profile = load_model_provider_profile(ROOT / "examples/providers/minimax-m3-research-v1.json")
    evaluation = load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v2.json"
    )

    registration.validate_against(
        catalog=catalog,
        provider_profile=profile,
        skills=SkillRegistry(ROOT / "skills"),
        evaluation_specification=evaluation,
    )

    core = evaluation.core_dict()
    clustered_value = core["clustered_paired_estimator"]
    assert isinstance(clustered_value, dict)
    clustered = cast(dict[str, object], clustered_value)
    assert clustered["independent_unit"] == "event_case"
    assert clustered["replicate_role"] == (
        "within_case_stochastic_measurement_not_independent_observation"
    )
    critical_values = clustered["critical_values_by_suite"]
    assert isinstance(critical_values, list)
    typed_critical_values = cast(list[dict[str, object]], critical_values)
    assert typed_critical_values[0]["independent_case_count"] == 24
    assert typed_critical_values[0]["degrees_of_freedom"] == 23
    contrast_value = core["contrast_policy"]
    assert isinstance(contrast_value, dict)
    contrast = cast(dict[str, object], contrast_value)
    assert contrast["selection_policy"] == "no_best_observed_arm_selection"
    assert registration.immutable_prior_registration_ids[-1].startswith(
        "method-quality-benchmark-fbebb357"
    )


def test_method_quality_registration_cannot_cross_bind_v1_and_v2_evaluation() -> None:
    registration = load_method_quality_benchmark(
        ROOT / "examples/calibration/method-quality-benchmark-v2.json"
    )
    catalog = load_research_method_catalog(
        ROOT / "examples/research/research-method-catalog-v2.json"
    )
    profile = load_model_provider_profile(ROOT / "examples/providers/minimax-m3-research-v1.json")
    evaluation = load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v1.json"
    )

    with pytest.raises(ValueError, match="versions do not match"):
        registration.validate_against(
            catalog=catalog,
            provider_profile=profile,
            skills=SkillRegistry(ROOT / "skills"),
            evaluation_specification=evaluation,
        )


def test_method_quality_registration_rejects_changed_skill_instructions(
    tmp_path: Path,
) -> None:
    registration = load_method_quality_benchmark(
        ROOT / "examples/calibration/method-quality-benchmark-v1.json"
    )
    catalog = load_research_method_catalog(
        ROOT / "examples/research/research-method-catalog-v2.json"
    )
    profile = load_model_provider_profile(ROOT / "examples/providers/minimax-m3-research-v1.json")
    evaluation = load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v1.json"
    )
    skill_root = tmp_path / "skills"
    shutil.copytree(ROOT / "skills", skill_root)
    instructions = skill_root / "research-discipline" / "SKILL.md"
    instructions.write_text(
        instructions.read_text(encoding="utf-8") + "\nchanged\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="instructions changed after discovery"):
        registration.validate_against(
            catalog=catalog,
            provider_profile=profile,
            skills=SkillRegistry(skill_root),
            evaluation_specification=evaluation,
        )


def test_evaluation_specification_rejects_changed_procedure() -> None:
    evaluation = load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v1.json"
    )
    changed_core = evaluation.core_dict()
    scoring = changed_core["scoring"]
    assert isinstance(scoring, dict)
    scoring["entry_search_limit_sessions"] = 4
    changed_hash = canonical_hash(changed_core)
    with pytest.raises(ValueError, match="entry search limit"):
        MethodQualityEvaluationSpecification(
            specification_id=f"method-quality-evaluation-{changed_hash}",
            canonical_specification_json=json.dumps(
                changed_core,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )


def test_masked_agent_input_fixture_binds_alias_transform_and_forbidden_scan() -> None:
    original_pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/evidence-pack.json").read_text(encoding="utf-8")
        )
    )
    masked_pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/masked-evidence-pack.json").read_text(
                encoding="utf-8"
            )
        )
    )
    original_documents = json.loads(
        (ROOT / "examples/agent/energy_supply/evidence-documents.json").read_text(encoding="utf-8")
    )
    masked_documents = json.loads(
        (ROOT / "examples/agent/energy_supply/masked-evidence-documents.json").read_text(
            encoding="utf-8"
        )
    )
    original_pattern = pattern_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/pattern-pack.json").read_text(encoding="utf-8")
        )
    )
    masked_pattern = pattern_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/masked-pattern-pack.json").read_text(
                encoding="utf-8"
            )
        )
    )
    manifest = load_masked_agent_input_manifest(
        ROOT / "examples/research/synthetic-energy-masked-input-manifest-v1.json"
    )

    manifest.validate_against(
        original_pack=original_pack,
        original_documents=original_documents,
        original_pattern_packs=(original_pattern,),
        masked_pack=masked_pack,
        masked_documents=masked_documents,
        masked_pattern_packs=(masked_pattern,),
    )

    assert original_pack.event_id != masked_pack.event_id
    assert original_pack.as_of != masked_pack.as_of
    assert original_pack.allowed_targets != masked_pack.allowed_targets
    assert original_pack.research_question != masked_pack.research_question
    repository = FrozenResearchRepository(
        evidence_pack=masked_pack,
        evidence_documents=masked_documents["documents"],
        pattern_packs={masked_pattern.pack_id: masked_pattern},
    )
    evidence_result = asyncio.run(repository.read_evidence({"evidence_id": "official-outage"}))
    pattern_result = asyncio.run(repository.read_pattern_pack({"pack_id": masked_pattern.pack_id}))
    visible_json = json.dumps(
        {"prompt": masked_pack.to_dict(), "evidence": evidence_result, "pattern": pattern_result}
    )
    assert "2026-01-15" not in visible_json
    assert "600938.XSHG" not in visible_json
    assert "2025-12-31" not in visible_json


def test_masked_agent_input_rejects_forbidden_token_on_visible_surface() -> None:
    original_pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/evidence-pack.json").read_text(encoding="utf-8")
        )
    )
    masked_pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/masked-evidence-pack.json").read_text(
                encoding="utf-8"
            )
        )
    )
    contaminated_pack = EvidencePack.build(
        event_id=masked_pack.event_id,
        as_of=masked_pack.as_of,
        research_question=masked_pack.research_question,
        evidence=masked_pack.evidence,
        pattern_packs=masked_pack.pattern_packs,
        allowed_targets=masked_pack.allowed_targets,
        data_gaps=(
            *masked_pack.data_gaps,
            "realized outcome for 600938.XSHG on 2026-01-15 was positive",
        ),
    )
    original_documents = json.loads(
        (ROOT / "examples/agent/energy_supply/evidence-documents.json").read_text(encoding="utf-8")
    )
    masked_documents = json.loads(
        (ROOT / "examples/agent/energy_supply/masked-evidence-documents.json").read_text(
            encoding="utf-8"
        )
    )
    manifest = load_masked_agent_input_manifest(
        ROOT / "examples/research/synthetic-energy-masked-input-manifest-v1.json"
    )
    original_pattern = pattern_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/pattern-pack.json").read_text(encoding="utf-8")
        )
    )
    masked_pattern = pattern_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/masked-pattern-pack.json").read_text(
                encoding="utf-8"
            )
        )
    )
    core = manifest.core_dict()
    core["masked_evidence_pack_id"] = contaminated_pack.pack_id
    core["masked_evidence_pack_hash"] = canonical_hash(contaminated_pack.to_dict())
    contaminated_manifest = MaskedAgentInputManifest(
        manifest_id=f"masked-agent-input-{canonical_hash(core)}",
        original_evidence_pack_id=manifest.original_evidence_pack_id,
        original_evidence_pack_hash=manifest.original_evidence_pack_hash,
        original_documents_hash=manifest.original_documents_hash,
        original_pattern_packs_hash=manifest.original_pattern_packs_hash,
        masked_evidence_pack_id=contaminated_pack.pack_id,
        masked_evidence_pack_hash=canonical_hash(contaminated_pack.to_dict()),
        masked_documents_hash=manifest.masked_documents_hash,
        masked_pattern_packs_hash=manifest.masked_pattern_packs_hash,
        alias_map=manifest.alias_map,
        alias_map_hash=manifest.alias_map_hash,
        forbidden_tokens=manifest.forbidden_tokens,
        forbidden_tokens_hash=manifest.forbidden_tokens_hash,
        agent_visible_fields=manifest.agent_visible_fields,
    )

    with pytest.raises(ValueError, match=r"contains forbidden token: 600938\.XSHG"):
        contaminated_manifest.validate_against(
            original_pack=original_pack,
            original_documents=original_documents,
            original_pattern_packs=(original_pattern,),
            masked_pack=contaminated_pack,
            masked_documents=masked_documents,
            masked_pattern_packs=(masked_pattern,),
        )


def test_masked_agent_input_forbidden_scan_covers_pattern_pack_tool_surface() -> None:
    original_pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/evidence-pack.json").read_text(encoding="utf-8")
        )
    )
    masked_pack = evidence_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/masked-evidence-pack.json").read_text(
                encoding="utf-8"
            )
        )
    )
    original_pattern = pattern_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/pattern-pack.json").read_text(encoding="utf-8")
        )
    )
    masked_pattern = pattern_pack_from_dict(
        json.loads(
            (ROOT / "examples/agent/energy_supply/masked-pattern-pack.json").read_text(
                encoding="utf-8"
            )
        )
    )
    leaked_entry = replace(
        masked_pattern.entries[0],
        mechanism=f"{masked_pattern.entries[0].mechanism} Learned on 2025-12-31.",
    )
    assert isinstance(leaked_entry, PatternEntry)
    leaked_pattern = PatternPack.build(
        version=masked_pattern.version,
        available_at=masked_pattern.available_at,
        entries=(leaked_entry, *masked_pattern.entries[1:]),
    )
    leaked_pack = EvidencePack.build(
        event_id=masked_pack.event_id,
        as_of=masked_pack.as_of,
        research_question=masked_pack.research_question,
        evidence=masked_pack.evidence,
        pattern_packs=(
            PatternPackReference(
                pack_id=leaked_pattern.pack_id,
                version=leaked_pattern.version,
                available_at=leaked_pattern.available_at,
                content_hash=canonical_hash(leaked_pattern.to_dict()),
            ),
        ),
        allowed_targets=masked_pack.allowed_targets,
        data_gaps=masked_pack.data_gaps,
    )
    original_documents = json.loads(
        (ROOT / "examples/agent/energy_supply/evidence-documents.json").read_text(encoding="utf-8")
    )
    masked_documents = json.loads(
        (ROOT / "examples/agent/energy_supply/masked-evidence-documents.json").read_text(
            encoding="utf-8"
        )
    )
    manifest = load_masked_agent_input_manifest(
        ROOT / "examples/research/synthetic-energy-masked-input-manifest-v1.json"
    )
    core = manifest.core_dict()
    core["masked_evidence_pack_id"] = leaked_pack.pack_id
    core["masked_evidence_pack_hash"] = canonical_hash(leaked_pack.to_dict())
    core["masked_pattern_packs_hash"] = canonical_hash([leaked_pattern.to_dict()])
    leaked_manifest = replace(
        manifest,
        manifest_id=f"masked-agent-input-{canonical_hash(core)}",
        masked_evidence_pack_id=leaked_pack.pack_id,
        masked_evidence_pack_hash=canonical_hash(leaked_pack.to_dict()),
        masked_pattern_packs_hash=canonical_hash([leaked_pattern.to_dict()]),
    )

    with pytest.raises(ValueError, match="contains forbidden token: 2025-12-31"):
        leaked_manifest.validate_against(
            original_pack=original_pack,
            original_documents=original_documents,
            original_pattern_packs=(original_pattern,),
            masked_pack=leaked_pack,
            masked_documents=masked_documents,
            masked_pattern_packs=(leaked_pattern,),
        )


def test_catalog_v2_routes_research_discipline_only_into_treatment_arms() -> None:
    catalog = load_research_method_catalog(
        ROOT / "examples/research/research-method-catalog-v2.json"
    )
    router = ResearchMethodRouter(catalog=catalog, skills=SkillRegistry(ROOT / "skills"))
    context = ResearchContext(
        mechanism_family="physical_energy_supply_shock",
        asset_class="public_equity",
        has_pattern_pack=True,
    )

    neutral = router.route(arm=MethodArm.NEUTRAL_EVIDENCE, context=context)
    general = router.route(arm=MethodArm.GENERAL_METHODS, context=context)

    assert "research-discipline" not in neutral.requested_skills
    assert general.requested_skills == (
        "evidence-core",
        "research-discipline",
        "event-market-context",
        "equity-exposure",
        "adversarial-risk",
    )
