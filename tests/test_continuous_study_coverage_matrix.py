from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from market_impact_agent import continuous_study_runner
from market_impact_agent.continuous_study import (
    build_continuous_study_coverage_matrix,
    build_continuous_study_registration,
    load_pinned_regime_panels,
    load_prior_usage_audit_binding,
)
from market_impact_agent.continuous_study_runner import (
    freeze_continuous_study_coverage_matrix,
    prepare_continuous_study,
    report_continuous_study_coverage_matrix,
)
from market_impact_agent.market_regimes import load_market_regime_dataset

from .test_continuous_study import require_private_continuous_study_inputs

_DATASET = Path("examples/research/market-regime-dataset-v1.json")
_PANELS = Path(".market-impact/regime")
_AUDIT = Path(".market-impact/continuous-20260905/prior-budget-audit.json")


def _matrix() -> dict[str, object]:
    require_private_continuous_study_inputs()
    dataset = load_market_regime_dataset(_DATASET)
    registration = build_continuous_study_registration(
        dataset,
        load_pinned_regime_panels(_PANELS),
        prior_usage_audit=load_prior_usage_audit_binding(_AUDIT),
    )
    return build_continuous_study_coverage_matrix(registration, dataset)


def test_coverage_matrix_preserves_registered_descriptors_and_explicit_gaps() -> None:
    matrix = _matrix()
    rows = cast(list[dict[str, object]], matrix["rows"])

    assert matrix["schema_version"] == "market-impact.continuous-study-coverage-matrix.v1"
    assert matrix["coverage_denominator"] == 18
    assert len(rows) == 18
    assert len(cast(list[object], matrix["deep_selection"])) == 8
    assert len(cast(list[object], matrix["overlaps"])) == 18
    assert matrix["evaluator_only"] is True
    assert matrix["labels_are_model_inputs"] is False
    assert matrix["model_or_network_invocation"] is False

    legacy_support = cast(dict[str, object], rows[0]["support"])["registered_case"]
    legacy_case = cast(dict[str, object], legacy_support)
    assert legacy_case["axes"] == {
        "path_direction": "up",
        "path_speed": "fast",
        "volatility": "high",
        "drawdown": "shallow",
        "recovery": "full",
        "narrative_salience": "diffuse",
        "causal_complexity": "multi_factor",
        "causal_directness": "indirect",
    }
    assert legacy_case["capability_targets"] == [
        "upside_participation",
        "crowding_control",
        "rotation_selection",
    ]

    ordinary = rows[-1]
    ordinary_window = cast(dict[str, object], ordinary["window"])
    ordinary_dimensions = cast(dict[str, object], ordinary["dimensions"])
    ordinary_features = cast(
        dict[str, str],
        cast(dict[str, object], ordinary_dimensions["direction_speed"])["pre_cutoff_features"],
    )
    assert ordinary_features == ordinary_window["features"]
    direction_gap = cast(dict[str, object], ordinary_dimensions["direction_speed"])[
        "path_direction_and_speed"
    ]
    assert cast(dict[str, object], direction_gap)["status"] == "unknown_not_inferred"

    gaps = cast(list[dict[str, object]], matrix["dimension_gaps"])
    assert [(item["dimension"], item["field"]) for item in gaps] == [
        ("direction_speed", "path_direction_and_path_speed"),
        ("volatility_liquidity", "liquidity"),
        ("cross_section_differences", "dispersion"),
        ("transitions", "transition_state"),
        ("information_shape", "news"),
    ]
    assert all(item["status"] == "unknown_not_inferred" for item in gaps)
    for row in rows:
        dimensions = cast(dict[str, object], row["dimensions"])
        volatility_liquidity = cast(dict[str, object], dimensions["volatility_liquidity"])
        liquidity = cast(dict[str, object], volatility_liquidity["liquidity"])
        assert liquidity["value"] is None


def test_coverage_matrix_freeze_is_additive_and_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_private_continuous_study_inputs()
    monkeypatch.setattr(
        continuous_study_runner, "shared_admission_root", lambda: tmp_path / "shared"
    )
    root = tmp_path / "study"
    prepare_continuous_study(
        root,
        dataset_path=_DATASET,
        panel_root=_PANELS,
        prior_usage_audit_path=_AUDIT,
    )
    immutable_before = {
        name: (root / name).read_bytes()
        for name in (
            "registration.json",
            "coverage.json",
            "daily-input-inventory.json",
            "preparation.json",
        )
    }

    frozen = freeze_continuous_study_coverage_matrix(
        root,
        dataset_path=_DATASET,
        panel_root=_PANELS,
        prior_usage_audit_path=_AUDIT,
    )
    matrix = report_continuous_study_coverage_matrix(root)
    replayed = freeze_continuous_study_coverage_matrix(
        root,
        dataset_path=_DATASET,
        panel_root=_PANELS,
        prior_usage_audit_path=_AUDIT,
    )

    assert frozen["status"] == "frozen_coverage_matrix"
    assert replayed["status"] == "replayed_immutable_coverage_matrix"
    assert frozen["coverage_matrix_id"] == matrix["coverage_matrix_id"]
    assert frozen["coverage_denominator"] == 18
    assert frozen["deep_selection_denominator"] == 8
    assert frozen["dimension_gap_count"] == 5
    assert {name: (root / name).read_bytes() for name in immutable_before} == immutable_before
