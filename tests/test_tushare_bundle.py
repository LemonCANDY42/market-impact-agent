import json
import stat
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from importlib import import_module
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.backtests import (
    BacktestRequest,
    BacktestRunStatus,
    SimulationSpec,
    backtest_request_to_dict,
)
from market_impact_agent.cli import main
from market_impact_agent.domain import Side, SignalIntent
from market_impact_agent.tushare import (
    TUSHARE_HTTP_ENDPOINT,
    TushareHttpAdapter,
    tushare_table_content_hash,
)
from market_impact_agent.tushare_bundle import (
    TushareDataCapture,
    TushareDataRequest,
    capture_tushare_data_bundle,
    validate_tushare_data_bundle,
    write_tushare_data_bundle,
)

NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
LATER = datetime(2026, 8, 25, 12, 0, 1, tzinfo=UTC)
TOKEN = "synthetic-bundle-token"
STOCK_FIELDS = (
    "ts_code",
    "symbol",
    "name",
    "exchange",
    "list_status",
    "list_date",
    "delist_date",
)
CALENDAR_FIELDS = ("exchange", "cal_date", "is_open", "pretrade_date")
DAILY_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "vol",
    "amount",
)
ADJ_FACTOR_FIELDS = ("ts_code", "trade_date", "adj_factor")
STK_LIMIT_FIELDS = ("ts_code", "trade_date", "pre_close", "up_limit", "down_limit")
pa = cast(Any, import_module("pyarrow"))
pq = cast(Any, import_module("pyarrow.parquet"))


class FakeTransport:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.requests: list[dict[str, object]] = []

    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
        assert endpoint == TUSHARE_HTTP_ENDPOINT
        assert timeout_seconds == 10.0
        self.requests.append(cast(dict[str, object], json.loads(body)))
        return json.dumps(self._responses.pop(0)).encode()


def response(fields: tuple[str, ...], items: list[list[object]]) -> dict[str, object]:
    return {"code": 0, "msg": None, "data": {"fields": list(fields), "items": items}}


def stock_row(
    ts_code: str,
    *,
    exchange: str,
    status: str,
    list_date: str,
    delist_date: str = "",
) -> list[object]:
    symbol = ts_code.split(".", 1)[0]
    return [ts_code, symbol, f"Stock {symbol}", exchange, status, list_date, delist_date]


def successful_responses(
    *,
    include_listing_anomaly: bool = False,
    missing_calendar_date: str | None = None,
    missing_daily_date: str | None = None,
) -> list[dict[str, object]]:
    listings = [
        response(
            STOCK_FIELDS,
            [
                stock_row("600028.SH", exchange="SSE", status="L", list_date="20010808"),
                stock_row("688999.SH", exchange="SSE", status="L", list_date="20270101"),
            ],
        ),
        response(
            STOCK_FIELDS,
            (
                [
                    stock_row(
                        "T00018.SH",
                        exchange="SSE",
                        status="D",
                        list_date="20000719",
                        delist_date="20061020",
                    )
                ]
                if include_listing_anomaly
                else []
            ),
        ),
        response(STOCK_FIELDS, []),
        response(STOCK_FIELDS, []),
        response(
            STOCK_FIELDS,
            [stock_row("000001.SZ", exchange="SZSE", status="L", list_date="19910403")],
        ),
        response(STOCK_FIELDS, []),
        response(STOCK_FIELDS, []),
        response(STOCK_FIELDS, []),
    ]
    calendar_rows: list[list[object]] = [
        ["SSE", "20190919", 1, "20190918"],
        ["SSE", "20190920", 1, "20190919"],
        ["SSE", "20190921", 0, "20190920"],
        ["SSE", "20190922", 0, "20190920"],
        ["SSE", "20190923", 1, "20190920"],
    ]
    if missing_calendar_date is not None:
        calendar_rows = [row for row in calendar_rows if row[1] != missing_calendar_date]
    calendar = response(CALENDAR_FIELDS, calendar_rows)
    daily_rows: list[list[object]] = [
        ["600028.SH", "20190919", 5.00, 5.10, 4.95, 5.05, 4.98, 1000.0, 5000.0],
        ["600028.SH", "20190920", 5.05, 5.20, 5.00, 5.15, 5.05, 1200.0, 6100.0],
        ["600028.SH", "20190923", 5.15, 5.25, 5.10, 5.20, 5.15, 900.0, 4700.0],
    ]
    if missing_daily_date is not None:
        daily_rows = [row for row in daily_rows if row[1] != missing_daily_date]
    return [*listings, calendar, response(DAILY_FIELDS, daily_rows)]


def hardened_responses(
    *,
    changed_factor: bool = False,
    pre_event_factor_change: bool = False,
) -> list[dict[str, object]]:
    base = successful_responses()
    calendar = response(
        CALENDAR_FIELDS,
        [
            ["SSE", "20190918", 1, "20190917"],
            ["SSE", "20190919", 1, "20190918"],
            ["SSE", "20190920", 1, "20190919"],
            ["SSE", "20190921", 0, "20190920"],
            ["SSE", "20190922", 0, "20190920"],
            ["SSE", "20190923", 1, "20190920"],
        ],
    )
    daily_rows: list[list[object]] = [
        ["600028.SH", "20190918", 4.95, 5.02, 4.92, 4.98, 4.95, 800.0, 3980.0],
        ["600028.SH", "20190919", 5.00, 5.10, 4.95, 5.05, 4.98, 1000.0, 5000.0],
        ["600028.SH", "20190920", 5.05, 5.20, 5.00, 5.15, 5.05, 1200.0, 6100.0],
        ["600028.SH", "20190923", 5.15, 5.25, 5.10, 5.20, 5.15, 900.0, 4700.0],
    ]
    factors: list[list[object]] = []
    for index, trade_date in enumerate(("20190918", "20190919", "20190920", "20190923")):
        factor = 6.75
        if changed_factor and index >= 2:
            factor = 6.8
        elif pre_event_factor_change and index == 0:
            factor = 6.7
        factors.append(["600028.SH", trade_date, factor])
    limits: list[list[object]] = [
        ["600028.SH", "20190918", 4.95, 5.45, 4.46],
        ["600028.SH", "20190919", 4.98, 5.48, 4.48],
        ["600028.SH", "20190920", 5.05, 5.56, 4.55],
        ["600028.SH", "20190923", 5.15, 5.67, 4.64],
    ]
    return [
        *base[:8],
        calendar,
        response(DAILY_FIELDS, daily_rows),
        response(ADJ_FACTOR_FIELDS, factors),
        response(STK_LIMIT_FIELDS, limits),
    ]


def request() -> TushareDataRequest:
    return TushareDataRequest(
        tushare_code="600028.SH",
        as_of_date=date(2019, 9, 18),
        start_date=date(2019, 9, 19),
        end_date=date(2019, 9, 23),
    )


def hardened_request() -> TushareDataRequest:
    return TushareDataRequest(
        tushare_code="600028.SH",
        as_of_date=date(2019, 9, 18),
        start_date=date(2019, 9, 18),
        evaluation_start_date=date(2019, 9, 19),
        end_date=date(2019, 9, 23),
    )


def replay_request(data_snapshot_id: str, /, **changes: object) -> BacktestRequest:
    values: dict[str, object] = {
        "request_id": "synthetic-tushare-modeled-open-v1",
        "signal": replay_signal("600028.XSHG"),
        "as_of": datetime(2019, 9, 18, 23, 59, 59, tzinfo=UTC),
        "start_at": datetime(2019, 9, 19, 1, 30, tzinfo=UTC),
        "end_at": datetime(2019, 9, 23, 7, tzinfo=UTC),
        "market": "CN",
        "instrument_ids": ("600028.XSHG",),
        "data_snapshot_id": data_snapshot_id,
        "target_selection_ref": "manual-integration-fixture:abqaiq-600028.v1",
        "strategy_ref": "event-impact-hold.v1",
        "horizons_sessions": (1,),
        "simulation": SimulationSpec(
            data_granularity="tushare_unadjusted_daily.v1",
            book_type="modeled_open_one_lot.v1",
            fill_model="modeled_open_one_lot_no_slippage.v1",
            fee_model="xshg_2019_fee_assumption.v1",
            venue_ruleset="xshg_600028_main_board_10pct.v1",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=0,
        ),
    }
    values.update(changes)
    return BacktestRequest(**cast(Any, values))


def replay_signal(instrument_id: str) -> SignalIntent:
    return SignalIntent(
        signal_id="synthetic-tushare-signal-v1",
        event_id="abqaiq-khurais-attack-2019",
        instrument_id=instrument_id,
        side=Side.BUY,
        valid_from=datetime(2019, 9, 18, 23, 59, 59, tzinfo=UTC),
        expires_at=datetime(2019, 9, 24, 1, 30, tzinfo=UTC),
        evidence_refs=("aramco-attack-confirmation",),
        invalidation_conditions=("synthetic fixture invalidated",),
    )


def capture(
    *,
    include_listing_anomaly: bool = False,
    missing_calendar_date: str | None = None,
    missing_daily_date: str | None = None,
    clock: Callable[[], datetime] = lambda: NOW,
) -> TushareDataCapture:
    adapter = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport(
            successful_responses(
                include_listing_anomaly=include_listing_anomaly,
                missing_calendar_date=missing_calendar_date,
                missing_daily_date=missing_daily_date,
            )
        ),
        clock=clock,
    )
    return capture_tushare_data_bundle(adapter, request())


def _canonical_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode()).hexdigest()


def _canonical_arrow_value(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _arrow_table_hash(table: Any) -> str:
    rows = cast(list[dict[str, object]], table.to_pylist())
    return _canonical_hash(
        {
            "fields": [{"name": field.name, "type": str(field.type)} for field in table.schema],
            "rows": [
                {key: _canonical_arrow_value(value) for key, value in row.items()} for row in rows
            ],
        }
    )


def _replace_parquet_and_reseal(
    bundle_path: Path,
    table_name: str,
    table: Any,
    *,
    compression: str = "zstd",
    update_source_content_hash: bool = False,
) -> Path:
    manifest_path = bundle_path / "manifest.json"
    manifest = cast(dict[str, object], json.loads(manifest_path.read_bytes()))
    tables = cast(dict[str, dict[str, object]], manifest["tables"])
    metadata = tables[table_name]
    table_path = bundle_path / cast(str, metadata["file"])
    pq.write_table(
        table,
        table_path,
        compression=compression,
        use_dictionary=False,
        write_statistics=True,
        data_page_version="1.0",
    )
    table_path.chmod(0o600)
    metadata["logical_identity"] = _arrow_table_hash(table)
    metadata["rows"] = table.num_rows
    metadata["sha256"] = sha256(table_path.read_bytes()).hexdigest()
    if update_source_content_hash:
        source = cast(dict[str, object], metadata["source"])
        params = cast(dict[str, str], source["params"])
        fields = tuple(cast(list[str], source["fields"]))
        rows = cast(list[dict[str, object]], table.to_pylist())
        if table_name != "daily":
            raise ValueError("test helper supports daily source resealing only")
        source_rows = tuple(
            (
                cast(str, row["ts_code"]),
                cast(date, row["trade_date"]).strftime("%Y%m%d"),
                *(row[field] for field in ("open", "high", "low", "close", "pre_close")),
                row["vol"],
                row["amount"],
            )
            for row in rows
        )
        source["content_hash"] = tushare_table_content_hash(
            api_name="daily",
            params=params,
            fields=fields,
            rows=source_rows,
        )
    return _reseal_manifest(bundle_path, manifest)


def _reseal_manifest(bundle_path: Path, manifest: dict[str, object]) -> Path:
    manifest_path = bundle_path / "manifest.json"
    core_manifest = {
        key: value
        for key, value in manifest.items()
        if key not in {"bundle_hash", "data_snapshot_id"}
    }
    bundle_hash = _canonical_hash(core_manifest)
    request_manifest = cast(dict[str, str], manifest["request"])
    code_slug = request_manifest["tushare_code"].lower().replace(".", "-")
    start_slug = request_manifest["start_date"].replace("-", "")
    end_slug = request_manifest["end_date"].replace("-", "")
    data_snapshot_id = f"tushare-{code_slug}-{start_slug}-{end_slug}-{bundle_hash[:16]}"
    manifest["bundle_hash"] = bundle_hash
    manifest["data_snapshot_id"] = data_snapshot_id
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    destination = bundle_path.parent / data_snapshot_id
    bundle_path.rename(destination)
    return destination


def test_request_requires_a_strictly_post_cutoff_window() -> None:
    with pytest.raises(ValueError, match="start_date must be after as_of_date"):
        TushareDataRequest(
            tushare_code="600028.SH",
            as_of_date=date(2019, 9, 19),
            start_date=date(2019, 9, 19),
            end_date=date(2019, 9, 23),
        )
    with pytest.raises(ValueError, match="end_date"):
        TushareDataRequest(
            tushare_code="600028.SH",
            as_of_date=date(2019, 9, 18),
            start_date=date(2019, 9, 23),
            end_date=date(2019, 9, 19),
        )


def test_capture_binds_listing_universe_calendar_and_daily_data() -> None:
    bundle = capture()

    assert bundle.request.instrument_id == "600028.XSHG"
    assert bundle.request.exchange == "SSE"
    assert bundle.request.instrument_id in bundle.universe.instrument_ids
    assert bundle.trade_calendar.api_name == "trade_cal"
    assert bundle.daily.api_name == "daily"
    assert bundle.provider_manifest.provider_id == "tushare-http"
    assert not bundle.provider_manifest.enabled
    assert not bundle.provider_manifest.verified_capabilities


def test_capture_retains_noncanonical_listing_as_anomaly_outside_universe(
    tmp_path: Path,
) -> None:
    bundle = capture(include_listing_anomaly=True)

    assert len(bundle.listing_snapshot.anomalies) == 1
    anomaly = bundle.listing_snapshot.anomalies[0]
    assert anomaly.tushare_code == "T00018.SH"
    assert anomaly.reason == "unsupported_tushare_stock_code"
    assert all("T00018" not in item for item in bundle.universe.instrument_ids)

    validated = validate_tushare_data_bundle(write_tushare_data_bundle(bundle, tmp_path))

    assert validated.listing_anomaly_count == 1


def test_capture_rejects_an_open_session_without_a_daily_row() -> None:
    with pytest.raises(ValueError, match="missing daily rows for open sessions: 20190920"):
        capture(missing_daily_date="20190920")


def test_capture_rejects_a_calendar_omission_even_when_daily_has_the_same_gap() -> None:
    with pytest.raises(ValueError, match="trade_cal response omits calendar dates: 20190920"):
        capture(missing_calendar_date="20190920", missing_daily_date="20190920")


def test_bundle_writes_private_deterministic_parquet_and_validates(tmp_path: Path) -> None:
    bundle = capture()
    first = write_tushare_data_bundle(bundle, tmp_path / "first")
    second = write_tushare_data_bundle(bundle, tmp_path / "second")

    first_manifest = validate_tushare_data_bundle(first)
    second_manifest = validate_tushare_data_bundle(second)

    assert first.name == first_manifest.data_snapshot_id
    assert first_manifest.manifest == second_manifest.manifest
    assert first_manifest.bundle_hash == second_manifest.bundle_hash
    assert first_manifest.data_snapshot_id.startswith("tushare-600028-sh-20190919-20190923-")
    assert first_manifest.instrument_id == "600028.XSHG"
    assert set(cast(dict[str, object], first_manifest.manifest["tables"])) == {
        "daily",
        "listing_anomalies",
        "listings",
        "trade_calendar",
        "universe",
    }
    assert stat.S_IMODE(first.stat().st_mode) == 0o700
    for path in first.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert TOKEN.encode() not in path.read_bytes()

    listing_snapshot = cast(dict[str, object], first_manifest.manifest["listing_snapshot"])
    queries = cast(list[dict[str, object]], listing_snapshot["queries"])
    assert len(queries) == 8
    assert queries[0] == {
        "api_name": "stock_basic",
        "content_hash": cast(list[str], listing_snapshot["query_hashes"])[0],
        "endpoint": TUSHARE_HTTP_ENDPOINT,
        "fields": list(STOCK_FIELDS),
        "params": {"exchange": "SSE", "list_status": "L"},
        "retrieved_at": "2026-08-25T12:00:00Z",
    }
    table_manifests = cast(dict[str, dict[str, object]], first_manifest.manifest["tables"])
    assert table_manifests["daily"]["source"] == {
        "api_name": "daily",
        "content_hash": bundle.daily.content_hash,
        "endpoint": TUSHARE_HTTP_ENDPOINT,
        "fields": list(DAILY_FIELDS),
        "params": {
            "end_date": "20190923",
            "start_date": "20190919",
            "ts_code": "600028.SH",
        },
        "retrieved_at": "2026-08-25T12:00:00Z",
    }


def test_hardened_bundle_binds_adjustments_source_limits_and_replay_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = capture_tushare_data_bundle(
        TushareHttpAdapter(
            TOKEN,
            transport=FakeTransport(hardened_responses()),
            clock=lambda: NOW,
        ),
        hardened_request(),
    )

    validated = validate_tushare_data_bundle(write_tushare_data_bundle(capture, tmp_path))
    manifest = validated.manifest
    tables = cast(dict[str, object], manifest["tables"])

    assert manifest["schema_version"] == "market-impact.tushare-data-bundle.v2"
    assert cast(dict[str, object], manifest["request"])["evaluation_start_date"] == "2019-09-19"
    assert set(tables) == {
        "adj_factors",
        "daily",
        "listing_anomalies",
        "listings",
        "stock_limits",
        "trade_calendar",
        "universe",
    }
    hardened_replay = replay_request(
        validated.data_snapshot_id,
        target_selection_ref="registered-a-share-integrated-oil-proxy:600028.v1",
        simulation=SimulationSpec(
            data_granularity="tushare_unadjusted_daily_with_source_limits.v2",
            book_type="modeled_open_one_lot.v1",
            fill_model="modeled_open_one_lot_no_slippage.v1",
            fee_model="xshg_2019_fee_assumption.v1",
            venue_ruleset="xshg_main_board_source_limit.v2",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=0,
        ),
    )

    from market_impact_agent.tushare_replay import run_validated_tushare_replay

    assert run_validated_tushare_replay(hardened_replay, validated.path).status is (
        BacktestRunStatus.COMPLETED
    )

    from market_impact_agent import tushare_replay

    snapshot, contract = tushare_replay.load_validated_tushare_modeled_open(validated.path)
    assert contract.data_adapter_version == "2.0.0"
    monkeypatch.setattr(
        tushare_replay,
        "TUSHARE_HARDENED_MODELED_OPEN_ADAPTER_VERSION",
        "2.0.1",
    )
    changed_snapshot, _ = tushare_replay.load_validated_tushare_modeled_open(validated.path)
    assert changed_snapshot.content_hash != snapshot.content_hash


def test_hardened_bundle_rejects_adjustment_factor_change(tmp_path: Path) -> None:
    capture = capture_tushare_data_bundle(
        TushareHttpAdapter(
            TOKEN,
            transport=FakeTransport(hardened_responses(changed_factor=True)),
            clock=lambda: NOW,
        ),
        hardened_request(),
    )

    with pytest.raises(ValueError, match="adjustment-factor change"):
        validate_tushare_data_bundle(write_tushare_data_bundle(capture, tmp_path))


def test_hardened_bundle_uses_adjusted_observation_history_before_replay(
    tmp_path: Path,
) -> None:
    capture = capture_tushare_data_bundle(
        TushareHttpAdapter(
            TOKEN,
            transport=FakeTransport(hardened_responses(pre_event_factor_change=True)),
            clock=lambda: NOW,
        ),
        hardened_request(),
    )
    bundle_path = write_tushare_data_bundle(capture, tmp_path)

    from market_impact_agent.tushare_replay import load_validated_tushare_adjusted_closes

    closes = load_validated_tushare_adjusted_closes(
        bundle_path,
        visible_at=datetime(2019, 9, 18, 23, 59, 59, tzinfo=UTC),
    )

    assert closes == ((datetime(2019, 9, 18, 7, tzinfo=UTC), Decimal("4.98") * Decimal("6.7")),)


def test_bundle_identity_binds_calendar_and_daily_retrieval_times(tmp_path: Path) -> None:
    first_capture = capture()
    clock_values = iter([NOW] * 8 + [LATER, LATER])
    later_capture = capture(clock=lambda: next(clock_values))

    assert (
        first_capture.listing_snapshot.snapshot_hash == later_capture.listing_snapshot.snapshot_hash
    )
    assert first_capture.daily.content_hash == later_capture.daily.content_hash

    first = validate_tushare_data_bundle(
        write_tushare_data_bundle(first_capture, tmp_path / "first")
    )
    later = validate_tushare_data_bundle(
        write_tushare_data_bundle(later_capture, tmp_path / "later")
    )

    assert first.bundle_hash != later.bundle_hash
    assert first.data_snapshot_id != later.data_snapshot_id


def test_bundle_write_removes_its_partial_directory_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_write = pq.write_table
    calls = 0

    def failing_write(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic Parquet failure")
        original_write(*args, **kwargs)

    monkeypatch.setattr(pq, "write_table", failing_write)
    output_root = tmp_path / "bundles"

    with pytest.raises(RuntimeError, match="synthetic Parquet failure"):
        write_tushare_data_bundle(capture(), output_root)

    assert output_root.is_dir()
    assert list(output_root.iterdir()) == []


def test_bundle_validation_rejects_tampered_parquet(tmp_path: Path) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    daily_path = bundle_path / "daily.parquet"
    daily_path.write_bytes(daily_path.read_bytes() + b"tampered")

    with pytest.raises(ValueError, match=r"daily\.parquet hash does not match"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_daily_for_another_instrument(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    daily_path = bundle_path / "daily.parquet"
    daily = pq.read_table(daily_path)
    replacement = daily.set_column(
        daily.schema.get_field_index("ts_code"),
        "ts_code",
        pa.array(["000001.SZ"] * daily.num_rows, type=pa.string()),
    )
    bundle_path = _replace_parquet_and_reseal(
        bundle_path,
        "daily",
        replacement,
        update_source_content_hash=True,
    )

    with pytest.raises(ValueError, match=r"daily\.parquet instrument does not match"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_daily_with_stale_source_hash(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    daily = pq.read_table(bundle_path / "daily.parquet")
    closes = cast(list[Decimal], daily.column("close").to_pylist())
    closes[0] = Decimal("5.060000")
    replacement = daily.set_column(
        daily.schema.get_field_index("close"),
        "close",
        pa.array(closes, type=daily.schema.field("close").type),
    )
    bundle_path = _replace_parquet_and_reseal(bundle_path, "daily", replacement)

    with pytest.raises(ValueError, match="does not match its source content_hash"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_calendar_with_stale_source_hash(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    calendar = pq.read_table(bundle_path / "trade_calendar.parquet")
    previous_dates = cast(list[date | None], calendar.column("pretrade_date").to_pylist())
    previous_dates[0] = date(2019, 9, 17)
    replacement = calendar.set_column(
        calendar.schema.get_field_index("pretrade_date"),
        "pretrade_date",
        pa.array(previous_dates, type=pa.date32()),
    )
    bundle_path = _replace_parquet_and_reseal(
        bundle_path,
        "trade_calendar",
        replacement,
    )

    with pytest.raises(ValueError, match="does not match its source content_hash"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_universe_not_derived_from_listings(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    replacement = pa.Table.from_pylist(
        [{"instrument_id": "000001.XSHE"}],
        schema=pa.schema([("instrument_id", pa.string())]),
    )
    bundle_path = _replace_parquet_and_reseal(bundle_path, "universe", replacement)

    with pytest.raises(ValueError, match=r"universe\.parquet is not derived"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_listings_not_bound_to_source_queries(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    listings_path = bundle_path / "listings.parquet"
    listings = pq.read_table(listings_path)
    names = cast(list[str], listings.column("name").to_pylist())
    names[0] = "Changed without source evidence"
    replacement = listings.set_column(
        listings.schema.get_field_index("name"),
        "name",
        pa.array(names, type=pa.string()),
    )
    bundle_path = _replace_parquet_and_reseal(bundle_path, "listings", replacement)

    with pytest.raises(ValueError, match="query hash does not match"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_listing_anomaly_with_stale_source_hash(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(
        capture(include_listing_anomaly=True),
        tmp_path,
    )
    anomaly_path = bundle_path / "listing_anomalies.parquet"
    anomalies = pq.read_table(anomaly_path)
    replacement = anomalies.set_column(
        anomalies.schema.get_field_index("name"),
        "name",
        pa.array(["Changed without source evidence"], type=pa.string()),
    )
    bundle_path = _replace_parquet_and_reseal(
        bundle_path,
        "listing_anomalies",
        replacement,
    )

    with pytest.raises(ValueError, match="query hash does not match"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_secret_fields(tmp_path: Path) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    manifest_path = bundle_path / "manifest.json"
    manifest = cast(dict[str, object], json.loads(manifest_path.read_bytes()))
    manifest["token"] = TOKEN
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="forbidden secret field"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_enabled_provider_manifest(tmp_path: Path) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    manifest = cast(
        dict[str, object],
        json.loads((bundle_path / "manifest.json").read_bytes()),
    )
    provider_manifest = cast(dict[str, object], manifest["provider_manifest"])
    provider_manifest["enabled"] = True
    bundle_path = _reseal_manifest(bundle_path, manifest)

    with pytest.raises(ValueError, match="disabled, unverified Tushare contract"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_resealed_nonofficial_source_endpoint(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    manifest = cast(
        dict[str, object],
        json.loads((bundle_path / "manifest.json").read_bytes()),
    )
    listing_snapshot = cast(dict[str, object], manifest["listing_snapshot"])
    for query in cast(list[dict[str, object]], listing_snapshot["queries"]):
        query["endpoint"] = "https://example.invalid/tushare"
    tables = cast(dict[str, dict[str, object]], manifest["tables"])
    cast(dict[str, object], tables["daily"]["source"])["endpoint"] = (
        "https://example.invalid/tushare"
    )
    cast(dict[str, object], tables["trade_calendar"]["source"])["endpoint"] = (
        "https://example.invalid/tushare"
    )
    bundle_path = _reseal_manifest(bundle_path, manifest)

    with pytest.raises(ValueError, match="official HTTPS endpoint"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_unknown_top_level_manifest_authority(
    tmp_path: Path,
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    manifest = cast(
        dict[str, object],
        json.loads((bundle_path / "manifest.json").read_bytes()),
    )
    manifest["authority"] = "provider"
    bundle_path = _reseal_manifest(bundle_path, manifest)

    with pytest.raises(ValueError, match="unknown fields: authority"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_false_format_metadata(tmp_path: Path) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    manifest = cast(
        dict[str, object],
        json.loads((bundle_path / "manifest.json").read_bytes()),
    )
    format_manifest = cast(dict[str, object], manifest["format"])
    format_manifest["compression"] = "snappy"
    bundle_path = _reseal_manifest(bundle_path, manifest)

    with pytest.raises(ValueError, match="compression must be zstd"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_parquet_that_is_not_zstd(tmp_path: Path) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    daily = pq.read_table(bundle_path / "daily.parquet")
    bundle_path = _replace_parquet_and_reseal(
        bundle_path,
        "daily",
        daily,
        compression="snappy",
    )

    with pytest.raises(ValueError, match="compression does not match zstd"):
        validate_tushare_data_bundle(bundle_path)


def test_bundle_validation_rejects_widened_file_permissions(tmp_path: Path) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    manifest_path = bundle_path / "manifest.json"
    manifest_path.chmod(0o644)

    with pytest.raises(ValueError, match=r"manifest\.json must have mode 0600"):
        validate_tushare_data_bundle(bundle_path)


def test_cli_validates_a_written_bundle(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle_path = write_tushare_data_bundle(capture(), tmp_path)

    assert main(["tushare", "validate", str(bundle_path)]) == 0

    result = cast(dict[str, object], json.loads(capsys.readouterr().out))
    assert result["valid"] is True
    assert result["path"] == bundle_path.as_posix()
    assert result["data_snapshot_id"] == bundle_path.name


def test_modeled_open_adapter_converts_timestamps_volume_and_one_lot_liquidity(
    tmp_path: Path,
) -> None:
    pytest.importorskip("nautilus_trader")
    from market_impact_agent.tushare_replay import load_validated_tushare_modeled_open

    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    snapshot, contract = load_validated_tushare_modeled_open(bundle_path)

    assert snapshot.bars[0].session_open_at.isoformat() == "2019-09-19T09:30:00+08:00"
    assert snapshot.bars[0].session_close_at.isoformat() == "2019-09-19T15:00:00+08:00"
    assert snapshot.bars[0].volume == 100_000
    assert snapshot.bars[0].open_bid_quantity == 100
    assert snapshot.bars[0].open_ask_quantity == 100
    assert contract.data_adapter_name == "tushare-xshg-modeled-open"
    assert tuple(item.name for item in contract.input_hashes) == (
        "bundle",
        "daily.parquet",
        "trade_calendar.parquet",
    )


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ("changed", "daily.parquet hash does not match"),
        ("symlink", "daily.parquet must be a real private file"),
    ],
)
def test_modeled_open_adapter_rebinds_parquet_bytes_after_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
    message: str,
) -> None:
    pytest.importorskip("nautilus_trader")
    import market_impact_agent.tushare_replay as tushare_replay

    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    original_validate = tushare_replay.validate_tushare_data_bundle

    def validate_then_replace(path: Path) -> object:
        bundle = original_validate(path)
        daily_path = bundle.path / "daily.parquet"
        if replacement == "changed":
            daily_path.write_bytes(daily_path.read_bytes() + b"post-validation replacement")
        else:
            daily_path.unlink()
            daily_path.symlink_to(bundle.path / "trade_calendar.parquet")
        return bundle

    monkeypatch.setattr(tushare_replay, "validate_tushare_data_bundle", validate_then_replace)

    with pytest.raises(ValueError, match=message):
        tushare_replay.load_validated_tushare_modeled_open(bundle_path)


def test_modeled_open_adapter_uses_zero_liquidity_on_the_locked_side(tmp_path: Path) -> None:
    pytest.importorskip("nautilus_trader")
    from market_impact_agent.tushare_replay import load_validated_tushare_modeled_open

    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    daily = pq.read_table(bundle_path / "daily.parquet")
    rows = cast(list[dict[str, object]], daily.to_pylist())
    rows[0]["open"] = Decimal("5.48")
    rows[0]["high"] = Decimal("5.48")
    rows[1]["open"] = Decimal("4.55")
    rows[1]["low"] = Decimal("4.55")
    changed = pa.Table.from_pylist(rows, schema=daily.schema)
    bundle_path = _replace_parquet_and_reseal(
        bundle_path,
        "daily",
        changed,
        update_source_content_hash=True,
    )

    snapshot, _ = load_validated_tushare_modeled_open(bundle_path)

    assert snapshot.bars[0].open_bid_quantity == 100
    assert snapshot.bars[0].open_ask_quantity == 0
    assert snapshot.bars[1].open_bid_quantity == 0
    assert snapshot.bars[1].open_ask_quantity == 100


def test_validated_tushare_replay_is_deterministic_and_binds_all_inputs(tmp_path: Path) -> None:
    pytest.importorskip("nautilus_trader")
    from market_impact_agent.tushare_replay import run_validated_tushare_replay

    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    request_value = replay_request(bundle_path.name)

    first = run_validated_tushare_replay(request_value, bundle_path)
    second = run_validated_tushare_replay(request_value, bundle_path)

    assert first.status is BacktestRunStatus.COMPLETED
    assert first.result_hash == second.result_hash
    assert first.metrics == second.metrics
    assert first.manifest.run_id != second.manifest.run_id
    assert first.manifest.data_adapter_name == "tushare-xshg-modeled-open"
    assert tuple(item.name for item in first.manifest.input_hashes) == (
        "bundle",
        "daily.parquet",
        "trade_calendar.parquet",
    )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"data_snapshot_id": "wrong-snapshot"}, "unsupported data_snapshot_id"),
        (
            {
                "signal": replay_signal("601857.XSHG"),
                "instrument_ids": ("601857.XSHG",),
            },
            "supports exactly the snapshot instrument",
        ),
        (
            {"start_at": datetime(2019, 9, 20, 1, 30, tzinfo=UTC)},
            "request start_at does not match",
        ),
        ({"horizons_sessions": (3,)}, "does not cover the requested holding horizon"),
        (
            {
                "simulation": SimulationSpec(
                    data_granularity="tushare_unadjusted_daily.v1",
                    book_type="modeled_open_one_lot.v1",
                    fill_model="modeled_open_one_lot_no_slippage.v1",
                    fee_model="xshg_2019_fee_assumption.v1",
                    venue_ruleset="unsupported-rules.v1",
                    base_currency="CNY",
                    starting_cash=Decimal("1000000"),
                    random_seed=0,
                )
            },
            "unsupported venue_ruleset",
        ),
    ],
)
def test_validated_tushare_replay_fails_closed_on_request_scope_mismatch(
    tmp_path: Path,
    change: dict[str, object],
    message: str,
) -> None:
    pytest.importorskip("nautilus_trader")
    from market_impact_agent.tushare_replay import run_validated_tushare_replay

    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    result = run_validated_tushare_replay(
        replay_request(bundle_path.name, **change),
        bundle_path,
    )

    assert result.status is BacktestRunStatus.FAILED
    assert message in result.failure_reasons[0]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "pre_close",
            Decimal("5.04"),
            "pre_close discontinuity is ambiguous with a corporate action or source gap",
        ),
        ("high", Decimal("5.49"), "breaches the modeled XSHG 10% price band"),
        ("vol", Decimal("1200.001"), "cannot be converted exactly from hands to shares"),
    ],
)
def test_modeled_open_adapter_rejects_ambiguous_or_malformed_daily_data(
    tmp_path: Path,
    field: str,
    value: Decimal,
    message: str,
) -> None:
    pytest.importorskip("nautilus_trader")
    from market_impact_agent.tushare_replay import load_validated_tushare_modeled_open

    bundle_path = write_tushare_data_bundle(capture(), tmp_path)
    daily = pq.read_table(bundle_path / "daily.parquet")
    rows = cast(list[dict[str, object]], daily.to_pylist())
    row_index = 1 if field == "pre_close" else 0
    rows[row_index][field] = value
    changed = pa.Table.from_pylist(rows, schema=daily.schema)
    bundle_path = _replace_parquet_and_reseal(
        bundle_path,
        "daily",
        changed,
        update_source_content_hash=True,
    )

    with pytest.raises(ValueError, match=message):
        load_validated_tushare_modeled_open(bundle_path)


def test_backtest_cli_succeeds_without_token_and_rejects_tampered_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("nautilus_trader")
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    bundle_path = write_tushare_data_bundle(capture(), tmp_path / "bundles")
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(backtest_request_to_dict(replay_request(bundle_path.name))),
        encoding="utf-8",
    )

    command = [
        "backtest",
        "run",
        "--request",
        str(request_path),
        "--data-snapshot",
        str(bundle_path),
    ]
    assert main(command) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "completed"

    daily_path = bundle_path / "daily.parquet"
    daily_path.write_bytes(daily_path.read_bytes() + b"tampered")
    assert main(command) == 1
    failure = json.loads(capsys.readouterr().err)
    assert failure["completed"] is False
    assert "hash does not match" in failure["error"]
