"""One actual native initial decision, registered signed reuse, then a scoped update."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent import continuous_study_runner
from market_impact_agent.continuous_decision import (
    ContinuousCadence,
    ContinuousDecision,
    ContinuousReviewCoordinator,
)
from market_impact_agent.continuous_experiment import (
    FrozenContinuousWindow,
    _compare_rows,  # pyright: ignore[reportPrivateUsage]
    _coordinator,  # pyright: ignore[reportPrivateUsage]
    _research_coverage,  # pyright: ignore[reportPrivateUsage]
    _runtime,  # pyright: ignore[reportPrivateUsage]
    _validated_measurement,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.continuous_portfolio_runtime import (
    ContinuousPortfolioRuntime,
    build_continuous_review_frame,
)
from market_impact_agent.continuous_study_runner import (
    load_prepared_continuous_registration,
    prepare_continuous_study,
    study_budget,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount

from . import test_historical_ashare_inputs as source_fixture
from .test_continuous_portfolio_runtime import (
    native_network,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)
from .test_continuous_study import (
    _registration,  # pyright: ignore[reportPrivateUsage]
    require_private_continuous_study_inputs,
)
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("native_network", ["registered"], indirect=True)
def test_real_native_three_cadence_prefix_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_network: ModelProviderProfile,  # noqa: F811
) -> None:
    require_private_continuous_study_inputs()
    monkeypatch.setattr(
        continuous_study_runner, "shared_admission_root", lambda: tmp_path / "shared"
    )
    study_root = tmp_path / "study"
    prepare_continuous_study(
        study_root,
        dataset_path=Path("examples/research/market-regime-dataset-v1.json"),
        panel_root=Path(".market-impact/regime"),
        prior_usage_audit_path=Path(".market-impact/continuous-20260905/prior-budget-audit.json"),
    )
    registration = load_prepared_continuous_registration(study_root)
    registration_id = str(registration["registration_id"])
    budget = study_budget(study_root, "rolling")
    before_requests = budget.summary()["physical_requests"]
    original_capture = source_fixture._capture  # pyright: ignore[reportPrivateUsage]
    dates = {"20241231": "20240920", "20250102": "20240923", "20250103": "20240924"}

    def capture(
        store: LocalDataSnapshotStore,
        api: str,
        params: dict[str, object],
        rows: list[dict[str, object]],
    ) -> str:
        def shifted(value: object) -> object:
            return dates.get(value, value) if isinstance(value, str) else value

        return original_capture(
            store,
            api,
            {k: shifted(v) for k, v in params.items()},
            [{k: shifted(v) for k, v in row.items()} for row in rows],
        )

    monkeypatch.setattr(source_fixture, "_capture", capture)
    source = source_fixture._source(tmp_path)  # pyright: ignore[reportPrivateUsage]
    snapshots: list[str] = []
    for symbol, exchange, price in (("510300.SH", "SSE", 4), ("000001.SZ", "SZSE", 10)):
        snapshots.append(
            capture(
                source.store,
                "fund_daily" if symbol == "510300.SH" else "daily",
                {"ts_code": symbol, "start_date": "20240925", "end_date": "20240925"},
                [
                    dict(
                        ts_code=symbol,
                        trade_date="20240925",
                        pre_close=price,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        change=0,
                        pct_chg=0,
                        vol=200000,
                        amount=80000,
                    )
                ],
            )
        )
        snapshots.append(
            capture(
                source.store,
                "trade_cal",
                {
                    "exchange": exchange,
                    "start_date": "20240925",
                    "end_date": "20240925",
                },
                [dict(exchange=exchange, cal_date="20240925", is_open=1, pretrade_date="20240924")],
            )
        )
        snapshots.append(
            capture(
                source.store,
                "stk_limit",
                {
                    "ts_code": symbol,
                    "start_date": "20240925",
                    "end_date": "20240925",
                },
                [
                    dict(
                        ts_code=symbol,
                        trade_date="20240925",
                        pre_close=price,
                        up_limit=price * 1.1,
                        down_limit=price * 0.9,
                    )
                ],
            )
        )
        snapshots.append(
            capture(
                source.store,
                "suspend_d",
                {
                    "ts_code": symbol,
                    "start_date": "20240925",
                    "end_date": "20240925",
                },
                [
                    dict(
                        ts_code=symbol, trade_date="20240925", suspend_type="R", suspend_timing=None
                    )
                ],
            )
        )
        snapshots.append(
            capture(
                source.store,
                "fund_adj" if symbol == "510300.SH" else "adj_factor",
                {"ts_code": symbol, "start_date": "20240925", "end_date": "20240925"},
                [dict(ts_code=symbol, trade_date="20240925", adj_factor=1)],
            )
        )
    source = source.with_snapshots(tuple(snapshots))
    assert source.session("510300.SH", date(2024, 9, 25)).execution_ready, source.session(
        "510300.SH", date(2024, 9, 25)
    ).gaps
    seed = source.session("510300.SH", date(2024, 9, 23))
    assert seed.spec is not None and seed.bar is not None, seed.gaps
    at = datetime(2024, 9, 24, 1, 25, tzinfo=UTC)
    later = at + timedelta(days=1)
    repositories = {
        t: _repository("510300.SH", at=t, event_id="registered-case") for t in (at, later)
    }
    frames = {
        t: build_continuous_review_frame(repository=repositories[t], market=source)
        for t in repositories
    }
    typed_registration, _ = _registration()
    window = FrozenContinuousWindow(
        "cn-2024-policy-melt-up",
        tuple(frames.values()),
        tuple(repositories.values()),
        source,
        (at.date(), later.date()),
        ("510300.SH", "000001.SZ"),
    )
    provider = PiRuntimeProvider(native_network, budget=budget)
    accounts: list[HistoricalStreamingAccount] = []

    async def scenario():
        origin = _runtime(
            study_root=study_root,
            registration=typed_registration,
            window=window,
            profile_arm="luna_max",
            cadence="coverage",
            provider=provider,
        )
        accounts.append(origin.account)
        initial = await origin.decide(frames[at], None, "batch-initial", frozenset({1}), False)
        assert isinstance(initial, ContinuousDecision), initial
        assert budget.summary()["physical_requests"] == before_requests + 2
        coordinators: list[tuple[ContinuousPortfolioRuntime, ContinuousReviewCoordinator]] = []
        for cadence in ContinuousCadence:
            destination = _runtime(
                study_root=study_root,
                registration=typed_registration,
                window=window,
                profile_arm="luna_max",
                cadence=cadence.value,
                provider=provider,
                source_runtime=origin,
            )
            accounts.append(destination.account)
            adopted = destination.adopt_initial(initial, frames[at])
            coordinator = _coordinator(
                runtime=destination,
                window=window,
                initial=adopted,
                registration_id=registration_id,
                cadence=cadence,
            )
            coordinators.append((destination, coordinator))
        for destination, coordinator in coordinators:
            assert (await coordinator.run(stop_after_sessions=1))["status"] == "prefix_complete"
            assert len(destination.account.results) == 2
        assert budget.summary()["physical_requests"] == before_requests + 2
        for destination, coordinator in coordinators:
            assert (await coordinator.run())["status"] == "completed"
            assert len(destination.account.results) == 3
        requests = budget.summary()["physical_requests"]
        for destination, coordinator in coordinators:
            assert (await coordinator.run(stop_after_sessions=1))["status"] == "prefix_complete"
            assert (await coordinator.run())["status"] == "completed"
            assert len(destination.account.results) == 3
        assert budget.summary()["physical_requests"] == requests
        destination, coordinator = coordinators[0]

        def reject_reopened_decision(*_: object) -> None:
            raise PermissionError("signed decision proof changed")

        coordinator.validate_decision = reject_reopened_decision
        with pytest.raises(PermissionError, match="proof changed"):
            await coordinator.run()
        assert len(destination.account.results) == 3  # full restored NAV is not validation
        from market_impact_agent.continuous_metrics import measure_continuous_account

        measured = measure_continuous_account(
            initial_nav=destination.account.results[0].nav,
            sessions=destination.account.results[1:],
            expected_sessions=2,
            execution_policy_hash="a" * 64,
            initial_account_hash="b" * 64,
            model_cost_microusd=0,
        )
        assert measured["complete"] is True
        diagnostic = _validated_measurement(measured, trajectory_status="incomplete")
        assert diagnostic["complete"] is False
        assert diagnostic["measurement_status"] == "diagnostic_only"
        assert diagnostic["equity_curve"] == measured["equity_curve"]
        assert (
            _compare_rows(
                {"status": "incomplete", "metrics": diagnostic},
                {"status": "completed", "metrics": measured},
            )["status"]
            == "incomplete_pair"
        )
        for initial_only in (True, False):
            row = {
                "status": "completed",
                "research_coverage": _research_coverage(window, initial=initial_only),
            }
            coverage = cast(dict[str, object], row["research_coverage"])
            assert row["status"] == "completed"
            assert coverage["classification"] == "limited_data"
            assert coverage["data_gaps"] == ["next-quarter management execution is unknown"]
            assert len(cast(list[object], coverage["frames"])) == (1 if initial_only else 2)

    async def run():
        try:
            await scenario()
        finally:
            for account in accounts:
                account.close()
            await provider.close()

    asyncio.run(run())


def test_missing_sources_preserve_all_fixed_rows_without_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    require_private_continuous_study_inputs()
    from market_impact_agent.continuous_experiment import run_continuous_experiment
    from market_impact_agent.model_provider import load_model_provider_profile

    monkeypatch.setattr(
        continuous_study_runner, "shared_admission_root", lambda: tmp_path / "shared"
    )
    study_root = tmp_path / "study"
    prepare_continuous_study(
        study_root,
        dataset_path=Path("examples/research/market-regime-dataset-v1.json"),
        panel_root=Path(".market-impact/regime"),
        prior_usage_audit_path=Path(".market-impact/continuous-20260905/prior-budget-audit.json"),
    )
    registration, panel = _registration()
    available: dict[str, ModelProviderProfile] = {}
    for path in Path("examples/providers").glob("pi-*.json"):
        profile = load_model_provider_profile(path)
        available[profile.profile_hash] = profile
    profiles = tuple(available[item.provider_profile_hash] for item in registration.model_profiles)
    before = study_budget(study_root, "rolling").summary()
    report = asyncio.run(
        run_continuous_experiment(
            study_root=study_root,
            registration=registration,
            selection_panel=panel,
            windows=(),
            profiles=profiles,
        )
    )
    assert report["status"] == "incomplete"
    assert report["model_dispatched"] is False
    assert len(cast(list[object], report["initial"])) == 54
    assert len(cast(list[object], report["rolling"])) == 72
    assert study_budget(study_root, "rolling").summary() == before


def test_after_close_cutoff_rejected_before_source_reopening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_private_continuous_study_inputs()
    from market_impact_agent.continuous_baselines import registered_baseline_windows
    from market_impact_agent.continuous_experiment import (
        prepare_continuous_experiment,
        registered_frame_cutoff,
    )
    from market_impact_agent.model_provider import load_model_provider_profile

    monkeypatch.setattr(
        continuous_study_runner, "shared_admission_root", lambda: tmp_path / "shared"
    )
    study_root = tmp_path / "study"
    prepare_continuous_study(
        study_root,
        dataset_path=Path("examples/research/market-regime-dataset-v1.json"),
        panel_root=Path(".market-impact/regime"),
        prior_usage_audit_path=Path(".market-impact/continuous-20260905/prior-budget-audit.json"),
    )
    registration, panel = _registration()
    available = {
        profile.profile_hash: profile
        for profile in (
            load_model_provider_profile(path)
            for path in Path("examples/providers").glob("pi-*.json")
        )
    }
    profiles = tuple(available[item.provider_profile_hash] for item in registration.model_profiles)
    deep = {cell.coverage_window_id for cell in registration.deep_cells}
    registered = next(
        item
        for item in registered_baseline_windows(registration, panel)
        if item.window.window_id not in deep
    )
    source = source_fixture._source(tmp_path)  # pyright: ignore[reportPrivateUsage]
    after_close = registered_frame_cutoff(registered.window.decision_session) + timedelta(hours=7)
    repository = _repository("510300.SH", at=after_close)
    frame = build_continuous_review_frame(repository=repository, market=source)
    window = FrozenContinuousWindow(
        registered.window.window_id, (frame,), (repository,), source, registered.sessions
    )

    def forbidden_source(*_: object) -> object:
        raise AssertionError("source reopened before cutoff policy")

    monkeypatch.setattr(source, "session", forbidden_source)
    with pytest.raises(PermissionError, match="09:25 Asia/Shanghai"):
        asyncio.run(
            prepare_continuous_experiment(
                study_root=study_root,
                registration=registration,
                selection_panel=panel,
                windows=(window,),
                profiles=profiles,
            )
        )
