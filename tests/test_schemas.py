import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry
from referencing.jsonschema import DRAFT202012, Schema

from market_impact_agent.backtests import backtest_request_from_dict
from market_impact_agent.research import EventArchetype

ROOT = Path(__file__).parents[1]


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


@pytest.mark.parametrize(
    "schema_name",
    [
        "data-query.schema.json",
        "data-snapshot.schema.json",
        "syndication-feed-source.schema.json",
        "csrc-news-source.schema.json",
        "nbs-macro-release-source.schema.json",
        "tushare-observation-source.schema.json",
        "source-route-acceptance-report.schema.json",
        "attention-watch-policy.schema.json",
        "attention-watch-wake.schema.json",
        "prospective-collection-policy.schema.json",
        "prospective-collection-job.schema.json",
        "prospective-collection-tracer-report.schema.json",
        "prospective-dataset-manifest.schema.json",
        "prospective-diagnostic-registration.schema.json",
        "prospective-checkpoint-snapshot-set.schema.json",
        "prospective-query-gate-result.schema.json",
        "checkpoint-decision-input.schema.json",
        "exchange-instrument-rule-set.schema.json",
        "checkpoint-market-universe-view.schema.json",
        "experimental-paper-admission.schema.json",
        "event-transmission.schema.json",
        "backtest-request.schema.json",
        "backtest-result.schema.json",
        "phase2-calibration-gate-result.schema.json",
        "phase2-calibration-evidence.schema.json",
        "phase2-calibration-registration-v2.schema.json",
        "phase2-calibration-evidence-v2.schema.json",
        "phase2-calibration-gate-result-v2.schema.json",
        "order-intent.schema.json",
        "trading-mandate.schema.json",
        "price-basis.schema.json",
        "hard-policy-evaluation.schema.json",
        "approval-decision.schema.json",
        "execution-receipt.schema.json",
        "provider-reconciliation-snapshot.schema.json",
        "execution-reconciliation.schema.json",
        "provider-manifest.schema.json",
        "signal-intent.schema.json",
        "research-method-catalog.schema.json",
        "model-provider-profile.schema.json",
        "method-ablation-registration.schema.json",
        "historical-evidence-manifest.schema.json",
        "masked-agent-input-manifest.schema.json",
        "method-quality-benchmark-registration.schema.json",
        "method-quality-benchmark-registration-v2.schema.json",
        "method-quality-evaluation-specification.schema.json",
        "method-quality-evaluation-specification-v2.schema.json",
        "latency-calibration.schema.json",
        "source-version-receipt.schema.json",
        "method-quality-market-snapshot.schema.json",
        "method-quality-outcome-seal.schema.json",
        "method-quality-outcome-opening.schema.json",
        "method-quality-clustered-estimate.schema.json",
        "common-crawl-locator.schema.json",
        "internet-archive-locator.schema.json",
        "method-development-case.schema.json",
        "market-regime-dataset.schema.json",
        "regime-panel.schema.json",
        "regime-study-registration.schema.json",
        "regime-study-baseline-report.schema.json",
        "regime-evidence-manifest.schema.json",
        "regime-evidence-qualification-report.schema.json",
        "regime-modeled-pit-policy.schema.json",
        "regime-modeled-pit-qualification-report.schema.json",
        "regime-modeled-pit-agent-validation-registration.schema.json",
        "regime-modeled-pit-agent-validation-report.schema.json",
        "regime-publisher-archive-recovery-report.schema.json",
        "regime-agent-experiment-report.schema.json",
        "regime-agent-validation-registration.schema.json",
        "regime-agent-validation-report.schema.json",
        "method-skill-catalog.schema.json",
        "method-evidence-declaration.schema.json",
        "method-skill-ablation-registration.schema.json",
        "method-skill-ablation-audit-correction.schema.json",
    ],
)
def test_schema_is_valid(schema_name: str) -> None:
    Draft202012Validator.check_schema(load_json(ROOT / "schemas" / schema_name))


@pytest.mark.parametrize(
    "example_path",
    [
        "examples/events/real-abqaiq-geopolitical-supply-shock.json",
        "examples/backtests/real-abqaiq-600028-tushare-request-v1.json",
        "examples/events/synthetic-energy-supply-shock.json",
        "examples/providers/tushare-http-unverified.json",
        "examples/providers/veighna-external-bridge.json",
        "examples/providers/federal-reserve-press-feed-v1.json",
        "examples/providers/csrc-official-news-v1.json",
        "examples/providers/nbs-macro-release-cpi-ppi-v1.json",
        "examples/providers/tushare-observation-cn-schedule-v1.json",
        "examples/providers/tushare-observation-etf-basic-v1.json",
        "examples/providers/tushare-observation-fund-daily-v1.json",
        "examples/providers/tushare-observation-index-classify-v1.json",
        "examples/providers/tushare-observation-index-daily-v1.json",
        "examples/providers/tushare-observation-index-member-all-v1.json",
        "examples/providers/tushare-observation-margin-v1.json",
        "examples/providers/tushare-observation-news-v1.json",
        "examples/providers/tushare-observation-report-rc-v1.json",
        "examples/providers/tushare-observation-stk-limit-v1.json",
        "examples/providers/tushare-observation-stock-basic-v1.json",
        "examples/providers/tushare-observation-trade-cal-v1.json",
        "examples/research/research-method-catalog-v1.json",
        "examples/providers/minimax-m3-research-v1.json",
        "examples/calibration/agent-method-ablation-v1.json",
        "examples/research/research-method-catalog-v2.json",
        "examples/research/synthetic-energy-historical-evidence-v1.json",
        "examples/research/synthetic-energy-masked-input-manifest-v1.json",
        "examples/calibration/method-quality-benchmark-v1.json",
        "examples/calibration/method-quality-evaluation-specification-v1.json",
        "examples/calibration/method-quality-benchmark-v2.json",
        "examples/calibration/method-quality-evaluation-specification-v2.json",
        "examples/research/common-crawl-complete-capture-v1.json",
        "examples/research/csrc-2024-policy-common-crawl-v1.json",
        "examples/research/gov-cn-2024-stimulus-common-crawl-v1.json",
        "examples/research/csrc-2024-merger-reform-internet-archive-v1.json",
        "examples/research/nbs-2024-08-economy-internet-archive-v1.json",
        "examples/research/nbs-2024-08-cpi-internet-archive-v1.json",
        "examples/calibration/method-development-abqaiq-v1.json",
        "examples/research/market-regime-dataset-v1.json",
        "examples/research/famous-method-skill-catalog-v1.json",
        "examples/research/abqaiq-recovery-method-evidence-v1.json",
        "examples/research/market-regime-study-registration-v1.json",
        "examples/research/regime-agent-validation-v1.json",
        "examples/research/regime-modeled-pit-policy-v1.json",
        "examples/research/regime-modeled-pit-agent-validation-v1.json",
        "examples/research/prospective-diagnostic-registration-v1.json",
        "examples/research/prospective-diagnostic-registration-v2.json",
    ],
)
def test_examples_conform_to_schema(example_path: str) -> None:
    instance = load_json(ROOT / example_path)
    if example_path.startswith("examples/events/"):
        schema_name = "event-transmission.schema.json"
    elif example_path.startswith("examples/backtests/"):
        schema_name = "backtest-request.schema.json"
    elif "research-method-catalog-v" in example_path:
        schema_name = "research-method-catalog.schema.json"
    elif example_path.endswith("minimax-m3-research-v1.json"):
        schema_name = "model-provider-profile.schema.json"
    elif example_path.endswith("agent-method-ablation-v1.json"):
        schema_name = "method-ablation-registration.schema.json"
    elif example_path.endswith("synthetic-energy-historical-evidence-v1.json"):
        schema_name = "historical-evidence-manifest.schema.json"
    elif example_path.endswith("synthetic-energy-masked-input-manifest-v1.json"):
        schema_name = "masked-agent-input-manifest.schema.json"
    elif example_path.endswith("method-quality-benchmark-v1.json"):
        schema_name = "method-quality-benchmark-registration.schema.json"
    elif example_path.endswith("method-quality-benchmark-v2.json"):
        schema_name = "method-quality-benchmark-registration-v2.schema.json"
    elif example_path.endswith("method-quality-evaluation-specification-v1.json"):
        schema_name = "method-quality-evaluation-specification.schema.json"
    elif example_path.endswith("method-quality-evaluation-specification-v2.json"):
        schema_name = "method-quality-evaluation-specification-v2.schema.json"
    elif example_path.endswith(
        (
            "common-crawl-complete-capture-v1.json",
            "csrc-2024-policy-common-crawl-v1.json",
            "gov-cn-2024-stimulus-common-crawl-v1.json",
        )
    ):
        schema_name = "common-crawl-locator.schema.json"
    elif example_path.endswith(
        (
            "csrc-2024-merger-reform-internet-archive-v1.json",
            "nbs-2024-08-economy-internet-archive-v1.json",
            "nbs-2024-08-cpi-internet-archive-v1.json",
        )
    ):
        schema_name = "internet-archive-locator.schema.json"
    elif example_path.endswith("method-development-abqaiq-v1.json"):
        schema_name = "method-development-case.schema.json"
    elif example_path.endswith("market-regime-dataset-v1.json"):
        schema_name = "market-regime-dataset.schema.json"
    elif example_path.endswith("famous-method-skill-catalog-v1.json"):
        schema_name = "method-skill-catalog.schema.json"
    elif example_path.endswith("abqaiq-recovery-method-evidence-v1.json"):
        schema_name = "method-evidence-declaration.schema.json"
    elif example_path.endswith("market-regime-study-registration-v1.json"):
        schema_name = "regime-study-registration.schema.json"
    elif example_path.endswith("regime-modeled-pit-policy-v1.json"):
        schema_name = "regime-modeled-pit-policy.schema.json"
    elif example_path.endswith("regime-modeled-pit-agent-validation-v1.json"):
        schema_name = "regime-modeled-pit-agent-validation-registration.schema.json"
    elif example_path.endswith("regime-agent-validation-v1.json"):
        schema_name = "regime-agent-validation-registration.schema.json"
    elif example_path.endswith(
        (
            "prospective-diagnostic-registration-v1.json",
            "prospective-diagnostic-registration-v2.json",
        )
    ):
        schema_name = "prospective-diagnostic-registration.schema.json"
    elif example_path.endswith("federal-reserve-press-feed-v1.json"):
        schema_name = "syndication-feed-source.schema.json"
    elif example_path.endswith("csrc-official-news-v1.json"):
        schema_name = "csrc-news-source.schema.json"
    elif example_path.endswith("nbs-macro-release-cpi-ppi-v1.json"):
        schema_name = "nbs-macro-release-source.schema.json"
    elif Path(example_path).name.startswith("tushare-observation-"):
        schema_name = "tushare-observation-source.schema.json"
    else:
        schema_name = "provider-manifest.schema.json"
    registry: Registry[Schema] = Registry()
    for path in (ROOT / "schemas").glob("*.schema.json"):
        schema = load_json(path)
        schema_id = schema.get("$id")
        if isinstance(schema_id, str):
            registry = registry.with_resource(schema_id, DRAFT202012.create_resource(schema))
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / schema_name),
        registry=registry,
        format_checker=FormatChecker(),
    )
    cast(Validator, validator).validate(instance)


def test_order_intent_schema_requires_explicit_expiry() -> None:
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / "order-intent.schema.json"),
        format_checker=FormatChecker(),
    )
    order = {
        "schema_version": "market-impact.order-intent.v1",
        "client_order_id": "order-1",
        "signal_id": "signal-1",
        "account_id": "paper-account",
        "environment": "paper",
        "instrument_id": "TEST",
        "side": "buy",
        "quantity": "10",
        "order_kind": "market",
        "created_at": "2026-08-25T12:00:00Z",
    }

    with pytest.raises(ValidationError, match="'expires_at' is a required property"):
        cast(Validator, validator).validate(order)


def test_prospective_diagnostic_schema_matches_versioned_capability_rules() -> None:
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / "prospective-diagnostic-registration.schema.json"),
        format_checker=FormatChecker(),
    )
    v1_optional = load_json(ROOT / "examples/research/prospective-diagnostic-registration-v1.json")
    v1_checkpoints = cast(list[dict[str, Any]], v1_optional["checkpoints"])
    first_v1_slot = cast(list[dict[str, Any]], v1_checkpoints[0]["capability_slots"])[0]
    first_v1_slot["applicability"] = "optional"

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(v1_optional)

    v2_missing_trigger = load_json(
        ROOT / "examples/research/prospective-diagnostic-registration-v2.json"
    )
    for checkpoint in cast(list[dict[str, Any]], v2_missing_trigger["checkpoints"]):
        for slot in cast(list[dict[str, Any]], checkpoint["capability_slots"]):
            if slot["capability"] == "event_revelation":
                slot["applicability"] = "optional"

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(v2_missing_trigger)


def test_event_schema_requires_values_for_known_expectation_delta() -> None:
    event = load_json(ROOT / "examples/events/synthetic-energy-supply-shock.json")
    event["expectation_delta"]["expected"] = None
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / "event-transmission.schema.json"),
        format_checker=FormatChecker(),
    )

    with pytest.raises(ValidationError, match="None is not of type 'string'"):
        cast(Validator, validator).validate(event)


def test_event_schema_allows_unknown_expectation_delta() -> None:
    event = load_json(ROOT / "examples/events/synthetic-energy-supply-shock.json")
    event["expectation_delta"] = {
        "baseline_source_ref": None,
        "expected": None,
        "observed": None,
        "direction": "unknown",
        "confidence": 0,
    }
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / "event-transmission.schema.json"),
        format_checker=FormatChecker(),
    )

    cast(Validator, validator).validate(event)


def test_method_quality_outcome_seal_event_archetype_matches_runtime() -> None:
    schema = load_json(ROOT / "schemas" / "method-quality-outcome-seal.schema.json")
    properties = cast(dict[str, Any], schema["properties"])
    event_archetype = cast(dict[str, Any], properties["event_archetype"])

    assert event_archetype["enum"] == [item.value for item in EventArchetype]


def test_method_quality_schema_accepts_runtime_decimal_upper_bound() -> None:
    registration = load_json(ROOT / "examples/calibration/method-quality-benchmark-v1.json")
    gate = cast(dict[str, object], registration["promotion_gate"])
    gate["minimum_required_abstention_recall"] = "1"
    gate["maximum_single_case_absolute_outcome_share"] = "1.0"
    gate["maximum_drawdown_increase_vs_neutral"] = "1"
    gate["maximum_cvar95_increase_vs_neutral"] = "1.00"
    validator = Draft202012Validator(
        load_json(ROOT / "schemas/method-quality-benchmark-registration.schema.json"),
        format_checker=FormatChecker(),
    )

    cast(Validator, validator).validate(registration)


def test_backtest_result_schema_closes_manifest_and_embedded_request() -> None:
    request_schema = load_json(ROOT / "schemas" / "backtest-request.schema.json")
    result_schema = load_json(ROOT / "schemas" / "backtest-result.schema.json")
    expected_request_definition = {
        key: value
        for key, value in request_schema.items()
        if key not in {"$schema", "$id", "title"}
    }
    definitions = cast(dict[str, object], result_schema["$defs"])
    assert definitions["backtest_request"] == expected_request_definition

    properties = cast(dict[str, Any], result_schema["properties"])
    manifest = cast(dict[str, Any], properties["manifest"])
    manifest_properties = cast(dict[str, Any], manifest["properties"])
    assert manifest_properties["request"] == {"$ref": "#/$defs/backtest_request"}
    validator = Draft202012Validator(
        result_schema,
        format_checker=FormatChecker(),
    )
    request = load_json(
        ROOT / "examples" / "backtests" / "real-abqaiq-600028-tushare-request-v1.json"
    )
    result = {
        "schema_version": "market-impact.backtest-result.v1",
        "manifest": {
            "run_id": "test-run",
            "request": request,
            "request_hash": "a" * 64,
            "engine_name": "nautilus_trader",
            "engine_version": "1.231.0",
            "bridge_name": "nautilus-backtest",
            "bridge_version": "0.3.0",
            "data_adapter_name": "tushare-xshg-modeled-open",
            "data_adapter_version": "1.0.0",
            "input_hashes": [{"name": "bundle", "value": "b" * 64}],
            "engine_config_hash": "c" * 64,
            "executed_at": "2026-08-25T12:00:00Z",
        },
        "status": "failed",
        "result_hash": "d" * 64,
        "metrics": [{"name": "synthetic_metric", "value": "1.25", "unit": "ratio"}],
        "artifact_refs": [],
        "failure_reasons": ["synthetic failure"],
    }

    cast(Validator, validator).validate(result)

    noncanonical_metric_result = deepcopy(result)
    metrics = cast(list[dict[str, object]], noncanonical_metric_result["metrics"])
    metrics[0]["value"] = "1.0"
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(noncanonical_metric_result)

    request["unexpected"] = True
    with pytest.raises(ValidationError, match="Additional properties are not allowed"):
        cast(Validator, validator).validate(result)


def test_phase2_registration_v2_embeds_the_owning_request_schema() -> None:
    request_schema = load_json(ROOT / "schemas" / "backtest-request.schema.json")
    registration_schema = load_json(
        ROOT / "schemas" / "phase2-calibration-registration-v2.schema.json"
    )
    expected_request_definition = {
        key: value
        for key, value in request_schema.items()
        if key not in {"$schema", "$id", "title"}
    }

    definitions = cast(dict[str, object], registration_schema["$defs"])
    assert definitions["backtest_request"] == expected_request_definition


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        ("0", True),
        ("0.0125", True),
        ("-0.0125", True),
        ("1.25", True),
        ("-1.25", True),
        ("0.0", False),
        ("0.01250", False),
        ("-0", False),
        ("-0.0", False),
        ("01", False),
    ],
)
def test_backtest_result_schema_accepts_exactly_canonical_decimal_metrics(
    value: str,
    valid: bool,
) -> None:
    schema = load_json(ROOT / "schemas" / "backtest-result.schema.json")
    metric_schema = cast(
        dict[str, object],
        cast(dict[str, Any], cast(dict[str, Any], schema["properties"])["metrics"])["items"],
    )
    validator = Draft202012Validator(metric_schema)
    metric = {"name": "return", "value": value, "unit": "ratio"}

    if valid:
        cast(Validator, validator).validate(metric)
    else:
        with pytest.raises(ValidationError):
            cast(Validator, validator).validate(metric)


@pytest.mark.parametrize(
    ("section", "field", "value", "valid"),
    [
        ("signal", "evidence_refs", ["evidence-1"], True),
        ("signal", "evidence_refs", ["evidence-1", "evidence-1"], False),
        ("signal", "invalidation_conditions", ["invalidate"], True),
        ("signal", "invalidation_conditions", ["invalidate", "invalidate"], False),
        ("simulation", "starting_cash", "1.25", True),
        ("simulation", "starting_cash", "0.5", True),
        ("simulation", "starting_cash", "0", False),
        ("simulation", "starting_cash", "1.0", False),
        ("simulation", "starting_cash", "1.20", False),
    ],
)
def test_backtest_request_schema_and_codec_agree_on_signal_refs_and_decimals(
    section: str,
    field: str,
    value: object,
    valid: bool,
) -> None:
    request = deepcopy(
        load_json(ROOT / "examples" / "backtests" / "real-abqaiq-600028-tushare-request-v1.json")
    )
    cast(dict[str, object], request[section])[field] = value
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / "backtest-request.schema.json"),
        format_checker=FormatChecker(),
    )

    if valid:
        cast(Validator, validator).validate(request)
        backtest_request_from_dict(request)
    else:
        with pytest.raises(ValidationError):
            cast(Validator, validator).validate(request)
        with pytest.raises(ValueError):
            backtest_request_from_dict(request)


def test_real_backtest_request_binds_the_existing_event_evidence_and_manual_target() -> None:
    event = load_json(ROOT / "examples/events/real-abqaiq-geopolitical-supply-shock.json")
    request = load_json(
        ROOT / "examples" / "backtests" / "real-abqaiq-600028-tushare-request-v1.json"
    )
    signal = cast(dict[str, Any], request["signal"])
    envelope = cast(dict[str, Any], event["envelope"])
    evidence = cast(list[dict[str, Any]], envelope["evidence"])
    evidence_ids = {item["evidence_id"] for item in evidence}

    assert signal["event_id"] == event["event_id"]
    assert set(cast(list[str], signal["evidence_refs"])) <= evidence_ids
    assert request["target_selection_ref"] == "manual-integration-fixture:abqaiq-600028.v1"


def load_json(path: Path) -> dict[str, Any]:
    payload = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(payload, dict):
        raise TypeError(f"expected object in {path}")
    return cast(dict[str, Any], payload)
