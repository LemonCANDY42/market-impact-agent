from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.market_regimes import (
    EventAnchor,
    MarketRegimeCase,
    MarketRegimeDataset,
    RegimePanel,
    RegimeSeries,
    RegimeTaxonomy,
    ValidatedRegimePanel,
)
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
    RegimeEvidenceManifest,
    RegimeEvidenceRecord,
    generate_regime_checkpoints,
    load_regime_evidence_manifest,
    load_regime_evidence_record,
    qualify_regime_evidence,
    write_regime_evidence_manifest,
    write_regime_evidence_qualification_report,
    write_regime_evidence_record,
)
from market_impact_agent.regime_market_evidence import build_panel_authority_records
from market_impact_agent.regime_study import (
    RegimeBaselineProtocol,
    RegimeCheckpointProtocol,
    RegimeSourceRequirement,
    RegimeStudyCase,
    RegimeStudyRegistration,
    RegimeStudySource,
)


def _market_case(*, end: date) -> MarketRegimeCase:
    return MarketRegimeCase(
        case_key="synthetic-weekly",
        path_start=date(2020, 1, 2),
        event_anchor=None,
        tradable_start=date(2020, 1, 2),
        end=end,
        axes={
            "path_direction": "mixed",
            "path_speed": "unclassified",
            "volatility": "high",
            "drawdown": "material",
            "recovery": "partial",
            "narrative_salience": "contested",
            "causal_complexity": "multi_factor",
            "causal_directness": "indirect",
        },
        capability_targets=("whipsaw_control",),
        primary_market_index="000300.SH",
        required_market_indices=("000300.SH",),
        required_industry_proxies=("sw2021_computer",),
        source_refs=("synthetic-source",),
    )


def _study_case(schedule: str) -> RegimeStudyCase:
    return RegimeStudyCase(
        case_key="synthetic-weekly",
        decision_schedule=schedule,
        analysis_needs=("cycle_position",),
        candidate_method_skills=("second-level-cycle-context",),
        query_terms=("synthetic",),
        evaluation_horizons=("full_case",),
        source_requirements=(),
    )


def _protocol() -> RegimeCheckpointProtocol:
    return RegimeCheckpointProtocol(
        timezone="Asia/Shanghai",
        decision_time_local="09:25:00",
        price_lookback_sessions=60,
        news_lookback_calendar_days=(
            ("monthly", 31),
            ("weekly", 14),
            ("event_then_weekly", 14),
        ),
        maximum_age_calendar_days=(
            ("official_context", 365),
            ("macro_vintage", 120),
            ("positioning_or_expectations", 14),
            ("issuer_or_sector_fundamentals", 180),
        ),
    )


def test_checkpoints_use_first_trading_session_and_preopen_cutoff() -> None:
    trading_dates = (
        date(2020, 1, 2),
        date(2020, 1, 3),
        date(2020, 1, 6),
        date(2020, 1, 7),
        date(2020, 2, 3),
    )
    case = _market_case(end=date(2020, 2, 3))

    weekly = generate_regime_checkpoints(
        case,
        _study_case("weekly"),
        protocol=_protocol(),
        trading_dates=trading_dates,
    )
    monthly = generate_regime_checkpoints(
        case,
        _study_case("monthly"),
        protocol=_protocol(),
        trading_dates=trading_dates,
    )

    assert tuple(item.session_date for item in weekly) == (
        date(2020, 1, 2),
        date(2020, 1, 6),
        date(2020, 2, 3),
    )
    assert weekly[0].cutoff_at == datetime(2020, 1, 2, 1, 25, tzinfo=UTC)
    assert tuple(item.session_date for item in monthly) == (
        date(2020, 1, 2),
        date(2020, 2, 3),
    )


def _registration() -> RegimeStudyRegistration:
    categories = (
        ("tushare-market", "market_price", "tushare-http", "regulated"),
        ("tushare-industry", "industry_price", "tushare-http", "regulated"),
        ("official", "official_context", "official-archive", "official"),
        ("macro", "macro_vintage", "macro-archive", "official"),
        ("news-a", "established_news", "news-provider", "established_news"),
        ("news-b", "established_news", "news-provider", "established_news"),
        (
            "positioning",
            "positioning_or_expectations",
            "positioning-archive",
            "regulated",
        ),
    )
    sources = tuple(
        RegimeStudySource(
            source_id=source_id,
            category=category,
            provider_id=provider,
            source_tier=tier,
            acquisition_mode="implemented_retrieved_history",
            point_in_time_authority=category not in {"market_price", "industry_price"},
            evidence_types=("new_evidence",),
            license_note="test",
        )
        for source_id, category, provider, tier in categories
    )
    requirements = tuple(
        RegimeSourceRequirement(
            category=category,
            source_ids=("news-a", "news-b") if category == "established_news" else (source_id,),
            minimum_records_per_checkpoint=(
                2 if category in {"market_price", "established_news"} else 1
            ),
            minimum_distinct_sources=2 if category == "established_news" else 1,
            authenticated_availability_required=True,
        )
        for source_id, category, _provider, _tier in categories
        if source_id != "news-b"
    )
    case = replace(_study_case("weekly"), source_requirements=requirements)
    core: dict[str, object] = {"test": "registration"}
    return RegimeStudyRegistration(
        registration_id="regime-study-registration-" + "1" * 64,
        version="test-v1",
        dataset_id="market-regime-dataset-" + "2" * 64,
        dataset_hash="2" * 64,
        method_catalog_id="method-skill-catalog-" + "3" * 64,
        method_catalog_hash="3" * 64,
        outcomes_opened=True,
        source_catalog=sources,
        checkpoint_protocol=replace(_protocol(), price_lookback_sessions=2),
        baseline_protocol=RegimeBaselineProtocol(
            annualization_sessions=252,
            minimum_risk_sessions=2,
            risk_free_rate_annual=Decimal(0),
            cvar_confidence=Decimal("0.95"),
            transaction_cost_bps_one_way=Decimal(0),
            rebalance_frequency="monthly_first_session",
            momentum_lookback_sessions=1,
            momentum_top_k=1,
            strategies=(
                "cash",
                "primary_buy_and_hold",
                "equal_sector_buy_and_hold",
                "lagged_sector_momentum",
            ),
        ),
        cases=(case,),
        core=core,
    )


def _dataset() -> MarketRegimeDataset:
    registration = _registration()
    return MarketRegimeDataset(
        dataset_id=registration.dataset_id,
        dataset_hash=registration.dataset_hash,
        version="test-v1",
        detector={},
        main_market_indices=("000300.SH",),
        industry_proxy_catalog=(
            {
                "proxy_id": "sw2021_computer",
                "source": "SW2021",
                "industry_name": "计算机",
                "tushare_code": "801750.SI",
            },
        ),
        cases=(_market_case(end=date(2020, 1, 2)),),
    )


def _validated_panel(dataset: MarketRegimeDataset) -> ValidatedRegimePanel:
    rows: tuple[dict[str, object], ...] = (
        {"trade_date": "2019-12-30", "open": 100, "close": 100},
        {"trade_date": "2019-12-31", "open": 100, "close": 100},
        {"trade_date": "2020-01-02", "open": 100, "close": 101},
    )
    panel = RegimePanel(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        provider_id="tushare-http",
        provider_version="0.1.0",
        historical_vintage="retrieved_historical_not_original_vintage",
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        industry_taxonomy=RegimeTaxonomy(
            source="SW2021",
            level="L1",
            fields=(
                "index_code",
                "industry_name",
                "parent_code",
                "level",
                "industry_code",
                "is_pub",
                "src",
            ),
            rows=(("801750.SI", "计算机", "", "L1", "710000", "1", "SW2021"),),
            retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
            content_hash="4" * 64,
        ),
        series=(
            RegimeSeries("000300.SH", "market", "000300.SH", "index_daily", "price", rows),
            RegimeSeries("sw2021_computer", "industry", "801750.SI", "sw_daily", "price", rows),
        ),
        proxy_resolution=(("sw2021_computer", "801750.SI"),),
    )
    return ValidatedRegimePanel(
        path=Path("panel.json"),
        panel_id="regime-panel-" + "5" * 64,
        panel_hash="5" * 64,
        panel=panel,
    )


def _record(
    *, source_id: str, category: str, publisher_id: str, suffix: str
) -> RegimeEvidenceRecord:
    return RegimeEvidenceRecord.build(
        case_keys=("synthetic-weekly",),
        category=category,
        source_id=source_id,
        provider_id=(
            "news-provider"
            if category == "established_news"
            else {
                "official_context": "official-archive",
                "macro_vintage": "macro-archive",
                "positioning_or_expectations": "positioning-archive",
            }[category]
        ),
        publisher_id=publisher_id,
        source_ref=f"https://example.test/{suffix}",
        claim_id=f"claim-{suffix}",
        lineage_id=f"lineage-{suffix}",
        title=f"Record {suffix}",
        occurred_at=datetime(2019, 12, 30, tzinfo=UTC),
        published_at=datetime(2019, 12, 30, tzinfo=UTC),
        source_updated_at=None,
        available_at=datetime(2019, 12, 30, 0, 5, tzinfo=UTC),
        availability_basis=RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED,
        latency_model_id=None,
        latency_model_hash=None,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id=f"archive-{suffix}",
        authority_at=datetime(2019, 12, 31, tzinfo=UTC),
        authority_hash=(suffix[0] if suffix[0].isdigit() else "6") * 64,
        content_hash="7" * 64,
        supersedes_id=None,
        license_scope="metadata_only",
    )


def test_availability_basis_fails_closed_for_receipts_and_modeled_latency() -> None:
    with pytest.raises(ValueError, match="actual-receipt authority"):
        replace(
            _record(
                source_id="official",
                category="official_context",
                publisher_id="official",
                suffix="9",
            ),
            availability_basis=RegimeEvidenceAvailabilityBasis.ACTUAL_RECEIPT,
        )

    with pytest.raises(ValueError, match="latency model"):
        replace(
            _record(
                source_id="official",
                category="official_context",
                publisher_id="official",
                suffix="9",
            ),
            availability_basis=RegimeEvidenceAvailabilityBasis.MODELED_LATENCY,
        )


def test_qualification_counts_independent_publishers_and_preserves_price_authority_gap() -> None:
    registration = _registration()
    dataset = _dataset()
    panel = _validated_panel(dataset)
    records = (
        _record(
            source_id="official", category="official_context", publisher_id="official", suffix="1"
        ),
        _record(source_id="macro", category="macro_vintage", publisher_id="macro", suffix="2"),
        _record(
            source_id="positioning",
            category="positioning_or_expectations",
            publisher_id="exchange",
            suffix="3",
        ),
        _record(
            source_id="news-a", category="established_news", publisher_id="publisher-a", suffix="4"
        ),
        _record(
            source_id="news-b", category="established_news", publisher_id="publisher-b", suffix="5"
        ),
    )
    manifest = RegimeEvidenceManifest.build(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        outcomes_opened=True,
        records=records,
    )

    report = qualify_regime_evidence(dataset, panel, registration, manifest)

    cases = cast(list[dict[str, object]], report["cases"])
    checkpoints = cast(list[dict[str, object]], cases[0]["checkpoints"])
    requirements = cast(list[dict[str, object]], checkpoints[0]["requirements"])
    by_category = {cast(str, item["category"]): item for item in requirements}
    assert by_category["established_news"]["record_count"] == 2
    assert by_category["established_news"]["distinct_source_count"] == 2
    assert by_category["established_news"]["ready"] is True
    assert by_category["market_price"]["record_count"] == 2
    assert by_category["market_price"]["content_complete"] is True
    assert by_category["market_price"]["point_in_time_authority"] is False
    assert report["all_source_requirements_ready"] is False

    same_publisher = _record(
        source_id="news-b",
        category="established_news",
        publisher_id="publisher-a",
        suffix="5",
    )
    changed = RegimeEvidenceManifest.build(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        outcomes_opened=True,
        records=(*records[:-1], same_publisher),
    )
    changed_report = qualify_regime_evidence(dataset, panel, registration, changed)
    changed_cases = cast(list[dict[str, object]], changed_report["cases"])
    changed_checkpoints = cast(list[dict[str, object]], changed_cases[0]["checkpoints"])
    changed_requirements = cast(list[dict[str, object]], changed_checkpoints[0]["requirements"])
    changed_news = changed_requirements[4]
    assert changed_news["category"] == "established_news"
    assert changed_news["distinct_source_count"] == 1
    assert changed_news["ready"] is False


def test_panel_publication_records_authorize_prices_without_rewriting_retrieval_time() -> None:
    registration = _registration()
    dataset = _dataset()
    panel = _validated_panel(dataset)
    checkpoint = generate_regime_checkpoints(
        dataset.cases[0],
        registration.cases[0],
        protocol=registration.checkpoint_protocol,
        trading_dates=(date(2020, 1, 2),),
    )[0]
    price_records = build_panel_authority_records(
        panel,
        market_case=dataset.cases[0],
        checkpoints=(checkpoint,),
    )
    non_price_records = (
        _record(
            source_id="official", category="official_context", publisher_id="official", suffix="1"
        ),
        _record(source_id="macro", category="macro_vintage", publisher_id="macro", suffix="2"),
        _record(
            source_id="positioning",
            category="positioning_or_expectations",
            publisher_id="exchange",
            suffix="3",
        ),
        _record(
            source_id="news-a", category="established_news", publisher_id="publisher-a", suffix="4"
        ),
        _record(
            source_id="news-b", category="established_news", publisher_id="publisher-b", suffix="5"
        ),
    )
    manifest = RegimeEvidenceManifest.build(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        outcomes_opened=True,
        records=(*price_records, *non_price_records),
    )

    report = qualify_regime_evidence(dataset, panel, registration, manifest)

    case = cast(list[dict[str, object]], report["cases"])[0]
    checkpoint_result = cast(list[dict[str, object]], case["checkpoints"])[0]
    requirements = cast(list[dict[str, object]], checkpoint_result["requirements"])
    by_category = {cast(str, item["category"]): item for item in requirements}
    assert by_category["market_price"]["point_in_time_authority"] is True
    assert by_category["market_price"]["authority_record_count"] == 2
    assert by_category["industry_price"]["point_in_time_authority"] is True
    assert by_category["industry_price"]["authority_record_count"] == 1
    assert checkpoint_result["ready"] is True


def test_current_verification_can_authenticate_historical_availability() -> None:
    old_record = _record(
        source_id="official",
        category="official_context",
        publisher_id="official",
        suffix="8",
    )
    record = RegimeEvidenceRecord.build(
        case_keys=old_record.case_keys,
        category=old_record.category,
        source_id=old_record.source_id,
        provider_id=old_record.provider_id,
        publisher_id=old_record.publisher_id,
        source_ref=old_record.source_ref,
        claim_id=old_record.claim_id,
        lineage_id=old_record.lineage_id,
        title=old_record.title,
        occurred_at=old_record.occurred_at,
        published_at=old_record.published_at,
        source_updated_at=old_record.source_updated_at,
        available_at=old_record.available_at,
        availability_basis=old_record.availability_basis,
        latency_model_id=old_record.latency_model_id,
        latency_model_hash=old_record.latency_model_hash,
        authority_kind=old_record.authority_kind,
        authority_id=old_record.authority_id,
        authority_at=datetime(2026, 8, 27, tzinfo=UTC),
        authority_hash=old_record.authority_hash,
        content_hash=old_record.content_hash,
        supersedes_id=old_record.supersedes_id,
        license_scope=old_record.license_scope,
    )
    registration = _registration()
    dataset = _dataset()
    panel = _validated_panel(dataset)
    price_records = build_panel_authority_records(
        panel,
        market_case=dataset.cases[0],
        checkpoints=generate_regime_checkpoints(
            dataset.cases[0],
            registration.cases[0],
            protocol=registration.checkpoint_protocol,
            trading_dates=(date(2020, 1, 2),),
        ),
    )
    records = (
        *price_records,
        record,
        _record(source_id="macro", category="macro_vintage", publisher_id="macro", suffix="2"),
        _record(
            source_id="positioning",
            category="positioning_or_expectations",
            publisher_id="exchange",
            suffix="3",
        ),
        _record(
            source_id="news-a", category="established_news", publisher_id="publisher-a", suffix="4"
        ),
        _record(
            source_id="news-b", category="established_news", publisher_id="publisher-b", suffix="5"
        ),
    )
    manifest = RegimeEvidenceManifest.build(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        outcomes_opened=True,
        records=records,
    )

    report = qualify_regime_evidence(dataset, panel, registration, manifest)

    assert cast(list[dict[str, object]], report["cases"])[0]["all_checkpoints_ready"] is True


def test_event_first_checkpoint_requires_post_anchor_revelation() -> None:
    base_registration = _registration()
    event_case = replace(
        _market_case(end=date(2020, 1, 2)),
        event_anchor=EventAnchor(
            observed_at=datetime(2020, 1, 2, 1, 0, tzinfo=UTC),
            anchor_session=date(2020, 1, 2),
            price_anchor="prior_close",
            executable=False,
        ),
    )
    study_case = replace(
        base_registration.cases[0],
        decision_schedule="event_then_weekly",
    )
    registration = replace(base_registration, cases=(study_case,))
    dataset = replace(_dataset(), cases=(event_case,))
    panel = _validated_panel(dataset)
    checkpoint = generate_regime_checkpoints(
        event_case,
        study_case,
        protocol=registration.checkpoint_protocol,
        trading_dates=(date(2020, 1, 2),),
    )[0]
    price_records = build_panel_authority_records(
        panel,
        market_case=event_case,
        checkpoints=(checkpoint,),
    )
    old_records = (
        _record(
            source_id="official", category="official_context", publisher_id="official", suffix="1"
        ),
        _record(source_id="macro", category="macro_vintage", publisher_id="macro", suffix="2"),
        _record(
            source_id="positioning",
            category="positioning_or_expectations",
            publisher_id="exchange",
            suffix="3",
        ),
        _record(
            source_id="news-a", category="established_news", publisher_id="publisher-a", suffix="4"
        ),
        _record(
            source_id="news-b", category="established_news", publisher_id="publisher-b", suffix="5"
        ),
    )

    def qualify(records: tuple[RegimeEvidenceRecord, ...]) -> dict[str, object]:
        manifest = RegimeEvidenceManifest.build(
            dataset_id=dataset.dataset_id,
            dataset_hash=dataset.dataset_hash,
            registration_id=registration.registration_id,
            registration_hash=registration.registration_hash,
            panel_id=panel.panel_id,
            panel_hash=panel.panel_hash,
            outcomes_opened=True,
            records=records,
        )
        return qualify_regime_evidence(dataset, panel, registration, manifest)

    missing = qualify((*price_records, *old_records))
    missing_checkpoint = cast(
        list[dict[str, object]], cast(list[dict[str, object]], missing["cases"])[0]["checkpoints"]
    )[0]
    assert missing_checkpoint["ready"] is False
    assert missing_checkpoint["event_revelation"] == {
        "required": True,
        "ready": False,
        "record_ids": [],
        "blockers": ["missing_event_revelation"],
    }

    base = old_records[0]
    revealed = RegimeEvidenceRecord.build(
        case_keys=base.case_keys,
        category=base.category,
        source_id=base.source_id,
        provider_id=base.provider_id,
        publisher_id=base.publisher_id,
        source_ref=base.source_ref + "#event",
        claim_id="event-revelation",
        lineage_id="event-revelation",
        title="Event revelation",
        occurred_at=datetime(2020, 1, 2, 1, 10, tzinfo=UTC),
        published_at=datetime(2020, 1, 2, 1, 10, tzinfo=UTC),
        source_updated_at=None,
        available_at=datetime(2020, 1, 2, 1, 10, tzinfo=UTC),
        availability_basis=RegimeEvidenceAvailabilityBasis.SOURCE_REPORTED,
        latency_model_id=None,
        latency_model_hash=None,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id="archive-event",
        authority_at=datetime(2020, 1, 3, tzinfo=UTC),
        authority_hash="8" * 64,
        content_hash="9" * 64,
        supersedes_id=None,
        license_scope="public_document",
    )
    ready = qualify((*price_records, *old_records, revealed))
    ready_checkpoint = cast(
        list[dict[str, object]], cast(list[dict[str, object]], ready["cases"])[0]["checkpoints"]
    )[0]
    assert ready_checkpoint["ready"] is True
    event_revelation = cast(dict[str, object], ready_checkpoint["event_revelation"])
    assert event_revelation["ready"] is True
    assert event_revelation["record_ids"] == [revealed.record_id]


def test_manifest_round_trips_and_rejects_identity_tampering(tmp_path: Path) -> None:
    registration = _registration()
    dataset = _dataset()
    panel = _validated_panel(dataset)
    record = _record(
        source_id="official",
        category="official_context",
        publisher_id="official",
        suffix="8",
    )
    manifest = RegimeEvidenceManifest.build(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        outcomes_opened=True,
        records=(record,),
    )
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest.to_dict()), encoding="utf-8")

    assert (
        load_regime_evidence_manifest(
            path,
            dataset=dataset,
            validated_panel=panel,
            registration=registration,
        )
        == manifest
    )

    payload = manifest.to_dict()
    records = cast(list[dict[str, object]], payload["records"])
    records[0]["publisher_id"] = "changed-publisher"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="record_id does not match content"):
        load_regime_evidence_manifest(
            path,
            dataset=dataset,
            validated_panel=panel,
            registration=registration,
        )


def test_private_evidence_artifacts_are_content_named_and_mode_0600(
    tmp_path: Path,
) -> None:
    registration = _registration()
    dataset = _dataset()
    panel = _validated_panel(dataset)
    record = _record(
        source_id="official",
        category="official_context",
        publisher_id="official",
        suffix="8",
    )
    record_path = write_regime_evidence_record(record, root=tmp_path / "records")
    assert record_path.name == f"{record.record_id}.json"
    assert record_path.stat().st_mode & 0o777 == 0o600
    assert load_regime_evidence_record(record_path) == record

    manifest = RegimeEvidenceManifest.build(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        outcomes_opened=True,
        records=(record,),
    )
    manifest_path = write_regime_evidence_manifest(manifest, root=tmp_path / "manifests")
    assert manifest_path.name == f"{manifest.manifest_id}.json"
    assert manifest_path.stat().st_mode & 0o777 == 0o600

    report = qualify_regime_evidence(dataset, panel, registration, manifest)
    assert cast(str, report["report_id"]).startswith("regime-evidence-qualification-report-")
    report_path = write_regime_evidence_qualification_report(
        report,
        root=tmp_path / "qualifications",
    )
    assert report_path.name == f"{report['report_id']}.json"
    assert report_path.stat().st_mode & 0o777 == 0o600

    changed_report = {**report, "diagnostic_agent_run_eligible": True}
    with pytest.raises(ValueError, match="report_id does not match content"):
        write_regime_evidence_qualification_report(
            changed_report,
            root=tmp_path / "qualifications",
        )
