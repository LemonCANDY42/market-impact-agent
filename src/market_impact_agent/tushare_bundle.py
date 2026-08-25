from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from market_impact_agent.providers import ProviderManifest
from market_impact_agent.tushare import (
    TUSHARE_HTTP_ENDPOINT,
    FixedAshareUniverse,
    StockListing,
    StockListingAnomaly,
    StockListingSnapshot,
    TushareHttpAdapter,
    TushareTable,
    build_pre_event_universe,
    canonical_instrument_id,
    exchange_for_tushare_code,
    tushare_provider_manifest,
    tushare_table_content_hash,
)

TUSHARE_DATA_BUNDLE_SCHEMA = "market-impact.tushare-data-bundle.v1"
TUSHARE_HARDENED_DATA_BUNDLE_SCHEMA = "market-impact.tushare-data-bundle.v2"

_TABLE_FILES = {
    "daily": "daily.parquet",
    "listing_anomalies": "listing_anomalies.parquet",
    "listings": "listings.parquet",
    "trade_calendar": "trade_calendar.parquet",
    "universe": "universe.parquet",
}
_HARDENED_TABLE_FILES = {
    **_TABLE_FILES,
    "adj_factors": "adj_factors.parquet",
    "stock_limits": "stock_limits.parquet",
}
_STOCK_SOURCE_FIELDS = (
    "ts_code",
    "symbol",
    "name",
    "exchange",
    "list_status",
    "list_date",
    "delist_date",
)
_CALENDAR_SOURCE_FIELDS = ("exchange", "cal_date", "is_open", "pretrade_date")
_DAILY_SOURCE_FIELDS = (
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
_ADJ_FACTOR_SOURCE_FIELDS = ("ts_code", "trade_date", "adj_factor")
_STK_LIMIT_SOURCE_FIELDS = (
    "ts_code",
    "trade_date",
    "pre_close",
    "up_limit",
    "down_limit",
)
_LISTING_PARTITIONS = tuple(
    (exchange, status) for exchange in ("SSE", "SZSE") for status in ("L", "D", "P", "G")
)


def _table_files_for_schema(schema_version: str) -> dict[str, str]:
    if schema_version == TUSHARE_DATA_BUNDLE_SCHEMA:
        return _TABLE_FILES
    if schema_version == TUSHARE_HARDENED_DATA_BUNDLE_SCHEMA:
        return _HARDENED_TABLE_FILES
    raise ValueError("unsupported Tushare bundle schema_version")


def _table_files_for_capture(capture: TushareDataCapture) -> dict[str, str]:
    if capture.request.hardened:
        if capture.adj_factors is None or capture.stock_limits is None:
            raise ValueError("hardened Tushare capture requires adjustment and limit tables")
        return _HARDENED_TABLE_FILES
    if capture.adj_factors is not None or capture.stock_limits is not None:
        raise ValueError("legacy Tushare capture cannot include hardened tables")
    return _TABLE_FILES


@dataclass(frozen=True, slots=True)
class TushareDataRequest:
    tushare_code: str
    as_of_date: date
    start_date: date
    end_date: date
    evaluation_start_date: date | None = None

    def __post_init__(self) -> None:
        canonical_instrument_id(self.tushare_code)
        if self.evaluation_start_date is None:
            if self.start_date <= self.as_of_date:
                raise ValueError("start_date must be after as_of_date")
        else:
            if self.start_date > self.as_of_date:
                raise ValueError("hardened data start_date must not be after as_of_date")
            if self.evaluation_start_date <= self.as_of_date:
                raise ValueError("evaluation_start_date must be after as_of_date")
            if self.evaluation_start_date < self.start_date:
                raise ValueError("evaluation_start_date must not be before start_date")
        if self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        if self.evaluation_start_date is not None and self.end_date < self.evaluation_start_date:
            raise ValueError("end_date must not be before evaluation_start_date")

    @property
    def instrument_id(self) -> str:
        return canonical_instrument_id(self.tushare_code)

    @property
    def exchange(self) -> str:
        return exchange_for_tushare_code(self.tushare_code)

    @property
    def replay_start_date(self) -> date:
        return self.start_date if self.evaluation_start_date is None else self.evaluation_start_date

    @property
    def hardened(self) -> bool:
        return self.evaluation_start_date is not None


@dataclass(frozen=True, slots=True)
class TushareDataCapture:
    request: TushareDataRequest
    provider_manifest: ProviderManifest
    listing_snapshot: StockListingSnapshot
    universe: FixedAshareUniverse
    trade_calendar: TushareTable
    daily: TushareTable
    adj_factors: TushareTable | None = None
    stock_limits: TushareTable | None = None


@dataclass(frozen=True, slots=True)
class ValidatedTushareDataBundle:
    path: Path
    data_snapshot_id: str
    bundle_hash: str
    instrument_id: str
    listing_anomaly_count: int
    universe_id: str
    manifest: dict[str, object]


def capture_tushare_data_bundle(
    adapter: TushareHttpAdapter,
    request: TushareDataRequest,
) -> TushareDataCapture:
    manifest = adapter.manifest
    manifest.assert_valid()
    listing_snapshot = adapter.fetch_stock_listings()
    universe = build_pre_event_universe(
        listing_snapshot,
        as_of_date=request.as_of_date,
    )
    if request.instrument_id not in universe.instrument_ids:
        raise ValueError(
            f"{request.instrument_id} is not eligible in universe {universe.universe_id}"
        )

    start_date = request.start_date.strftime("%Y%m%d")
    end_date = request.end_date.strftime("%Y%m%d")
    trade_calendar = adapter.fetch_trade_calendar(
        exchange=request.exchange,
        start_date=start_date,
        end_date=end_date,
    )
    daily = adapter.fetch_daily(
        tushare_code=request.tushare_code,
        start_date=start_date,
        end_date=end_date,
    )
    _require_complete_daily_window(trade_calendar, daily)
    adj_factors: TushareTable | None = None
    stock_limits: TushareTable | None = None
    if request.hardened:
        adj_factors = adapter.fetch_adj_factors(
            tushare_code=request.tushare_code,
            start_date=start_date,
            end_date=end_date,
        )
        stock_limits = adapter.fetch_stock_limits(
            tushare_code=request.tushare_code,
            start_date=start_date,
            end_date=end_date,
        )
        _require_complete_daily_window(trade_calendar, adj_factors)
        _require_complete_daily_window(trade_calendar, stock_limits)
    return TushareDataCapture(
        request=request,
        provider_manifest=manifest,
        listing_snapshot=listing_snapshot,
        universe=universe,
        trade_calendar=trade_calendar,
        daily=daily,
        adj_factors=adj_factors,
        stock_limits=stock_limits,
    )


def write_tushare_data_bundle(capture: TushareDataCapture, output_root: Path) -> Path:
    pa, pq = _pyarrow_modules()
    table_files = _table_files_for_capture(capture)
    output_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary_path = Path(tempfile.mkdtemp(prefix=".tmp-tushare-", dir=output_root))
    try:
        tables = _write_parquet_tables(
            capture,
            temporary_path,
            table_files=table_files,
            pa=pa,
            pq=pq,
        )
        core_manifest = _core_manifest(capture, tables=tables, pyarrow_version=pa.__version__)
        bundle_hash = _canonical_hash(core_manifest)
        data_snapshot_id = _data_snapshot_id(capture.request, bundle_hash)
        manifest = {
            **core_manifest,
            "bundle_hash": bundle_hash,
            "data_snapshot_id": data_snapshot_id,
        }
        manifest_path = temporary_path / "manifest.json"
        _write_private_file(manifest_path, _pretty_json_bytes(manifest))

        destination = output_root / data_snapshot_id
        if destination.exists():
            existing = validate_tushare_data_bundle(destination)
            if existing.bundle_hash != bundle_hash:
                raise FileExistsError(f"conflicting Tushare bundle exists: {destination}")
            _remove_created_directory(temporary_path)
            return destination
        temporary_path.rename(destination)
        return destination
    except Exception:
        if temporary_path.exists():
            _remove_created_directory(temporary_path)
        raise


def validate_tushare_data_bundle(bundle_path: Path) -> ValidatedTushareDataBundle:
    if bundle_path.is_symlink() or not bundle_path.is_dir():
        raise ValueError("Tushare bundle path must be a real directory")
    if stat.S_IMODE(bundle_path.stat().st_mode) != 0o700:
        raise ValueError("Tushare bundle directory must have mode 0700")
    manifest_path = bundle_path / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("manifest.json must be a real file")
    if stat.S_IMODE(manifest_path.stat().st_mode) != 0o600:
        raise ValueError("manifest.json must have mode 0600")
    payload = json.loads(manifest_path.read_bytes())
    if not isinstance(payload, dict):
        raise ValueError("Tushare bundle manifest must be a JSON object")
    manifest = cast(dict[str, object], payload)
    if _contains_forbidden_key(manifest):
        raise ValueError("Tushare bundle manifest contains a forbidden secret field")
    schema_version = manifest.get("schema_version")
    if schema_version not in {
        TUSHARE_DATA_BUNDLE_SCHEMA,
        TUSHARE_HARDENED_DATA_BUNDLE_SCHEMA,
    }:
        raise ValueError("unsupported Tushare bundle schema_version")
    table_files = _table_files_for_schema(cast(str, schema_version))
    _validate_manifest_shape(manifest, table_files=table_files)

    bundle_hash = _required_string(manifest, "bundle_hash")
    _require_sha256(bundle_hash, "bundle_hash")
    data_snapshot_id = _required_string(manifest, "data_snapshot_id")
    core_manifest = {
        key: value
        for key, value in manifest.items()
        if key not in {"bundle_hash", "data_snapshot_id"}
    }
    if _canonical_hash(core_manifest) != bundle_hash:
        raise ValueError("Tushare bundle hash does not match its manifest")

    request = _required_mapping(manifest, "request")
    capture_request = TushareDataRequest(
        tushare_code=_required_string(request, "tushare_code"),
        as_of_date=date.fromisoformat(_required_string(request, "as_of_date")),
        start_date=date.fromisoformat(_required_string(request, "start_date")),
        end_date=date.fromisoformat(_required_string(request, "end_date")),
        evaluation_start_date=(
            date.fromisoformat(_required_string(request, "evaluation_start_date"))
            if schema_version == TUSHARE_HARDENED_DATA_BUNDLE_SCHEMA
            else None
        ),
    )
    if _required_string(request, "instrument_id") != capture_request.instrument_id:
        raise ValueError("Tushare request instrument_id does not match tushare_code")
    if _required_string(request, "exchange") != capture_request.exchange:
        raise ValueError("Tushare request exchange does not match tushare_code")
    expected_id = _data_snapshot_id_from_values(
        tushare_code=capture_request.tushare_code,
        start_date=capture_request.start_date.isoformat(),
        end_date=capture_request.end_date.isoformat(),
        bundle_hash=bundle_hash,
    )
    if data_snapshot_id != expected_id or bundle_path.name != data_snapshot_id:
        raise ValueError("Tushare data_snapshot_id does not match bundle identity")

    provider_manifest = ProviderManifest.from_dict(_required_mapping(manifest, "provider_manifest"))
    provider_manifest.assert_valid()
    if provider_manifest != tushare_provider_manifest():
        raise ValueError("Provider manifest must remain the disabled, unverified Tushare contract")
    listing_snapshot = _required_mapping(manifest, "listing_snapshot")
    universe = _required_mapping(manifest, "universe")
    if _required_string(listing_snapshot, "provider_id") != provider_manifest.provider_id:
        raise ValueError("Listing Snapshot provider does not match Provider manifest")
    if _required_string(listing_snapshot, "provider_version") != provider_manifest.provider_version:
        raise ValueError("Listing Snapshot version does not match Provider manifest")
    if _required_string(universe, "as_of_date") != capture_request.as_of_date.isoformat():
        raise ValueError("Pre-event Universe cutoff does not match the request")
    if _required_string(universe, "listing_snapshot_hash") != _required_string(
        listing_snapshot, "snapshot_hash"
    ):
        raise ValueError("Pre-event Universe does not bind the Listing Snapshot")

    tables = _required_mapping(manifest, "tables")
    if set(tables) != set(table_files):
        raise ValueError("Tushare bundle tables do not match the required set")
    pa, pq = _pyarrow_modules()
    format_manifest = _required_mapping(manifest, "format")
    if _required_string(format_manifest, "parquet_writer") != f"pyarrow-{pa.__version__}":
        raise ValueError("Tushare bundle parquet_writer does not match the pinned runtime")
    expected_schemas = _arrow_schemas(pa)
    arrow_tables: dict[str, Any] = {}
    for table_name, expected_file in table_files.items():
        metadata = _required_mapping(tables, table_name)
        file_name = _required_string(metadata, "file")
        if file_name != expected_file:
            raise ValueError(f"unexpected file for {table_name}")
        table_path = bundle_path / file_name
        if table_path.is_symlink() or not table_path.is_file():
            raise ValueError(f"{file_name} must be a real file")
        if stat.S_IMODE(table_path.stat().st_mode) != 0o600:
            raise ValueError(f"{file_name} must have mode 0600")
        expected_hash = _required_string(metadata, "sha256")
        _require_sha256(expected_hash, f"{table_name} sha256")
        if _file_hash(table_path) != expected_hash:
            raise ValueError(f"{file_name} hash does not match")
        expected_rows = _required_integer(metadata, "rows")
        parquet_file = pq.ParquetFile(table_path)
        _validate_parquet_compression(parquet_file, file_name)
        arrow_table = pq.read_table(table_path)
        actual_rows = int(arrow_table.num_rows)
        if actual_rows != expected_rows:
            raise ValueError(f"{file_name} row count does not match")
        if not arrow_table.schema.equals(expected_schemas[table_name], check_metadata=True):
            raise ValueError(f"{file_name} schema does not match")
        logical_identity = _required_string(metadata, "logical_identity")
        _require_sha256(logical_identity, f"{table_name} logical_identity")
        if _arrow_table_hash(arrow_table) != logical_identity:
            raise ValueError(f"{file_name} logical identity does not match")
        _required_timestamp(metadata, "retrieved_at")
        arrow_tables[table_name] = arrow_table
    expected_entries = {"manifest.json", *table_files.values()}
    if {path.name for path in bundle_path.iterdir()} != expected_entries:
        raise ValueError("Tushare bundle contains unexpected files")
    universe_id = _validate_bundle_semantics(
        manifest=manifest,
        request=capture_request,
        provider_manifest=provider_manifest,
        tables=tables,
        arrow_tables=arrow_tables,
    )
    return ValidatedTushareDataBundle(
        path=bundle_path,
        data_snapshot_id=data_snapshot_id,
        bundle_hash=bundle_hash,
        instrument_id=capture_request.instrument_id,
        listing_anomaly_count=len(_arrow_rows(arrow_tables["listing_anomalies"])),
        universe_id=universe_id,
        manifest=manifest,
    )


def _require_complete_daily_window(calendar: TushareTable, daily: TushareTable) -> None:
    calendar_indexes = {field: calendar.fields.index(field) for field in calendar.fields}
    daily_indexes = {field: daily.fields.index(field) for field in daily.fields}
    open_dates = {
        cast(str, row[calendar_indexes["cal_date"]])
        for row in calendar.rows
        if row[calendar_indexes["is_open"]] in (1, "1")
    }
    if not open_dates:
        raise ValueError("trade calendar contains no open sessions")
    daily_dates = {cast(str, row[daily_indexes["trade_date"]]) for row in daily.rows}
    missing = sorted(open_dates - daily_dates)
    if missing:
        raise ValueError(f"missing daily rows for open sessions: {', '.join(missing)}")
    unexpected = sorted(daily_dates - open_dates)
    if unexpected:
        raise ValueError(f"daily rows fall outside open sessions: {', '.join(unexpected)}")


def _write_parquet_tables(
    capture: TushareDataCapture,
    directory: Path,
    *,
    table_files: dict[str, str],
    pa: Any,
    pq: Any,
) -> dict[str, object]:
    arrow_tables = _arrow_tables(capture, pa=pa)
    retrieved_at = {
        "daily": capture.daily.retrieved_at,
        "listing_anomalies": capture.listing_snapshot.retrieved_at,
        "listings": capture.listing_snapshot.retrieved_at,
        "trade_calendar": capture.trade_calendar.retrieved_at,
        "universe": capture.universe.built_at,
    }
    if capture.adj_factors is not None:
        retrieved_at["adj_factors"] = capture.adj_factors.retrieved_at
    if capture.stock_limits is not None:
        retrieved_at["stock_limits"] = capture.stock_limits.retrieved_at
    metadata: dict[str, object] = {}
    for name, file_name in table_files.items():
        table = arrow_tables[name]
        path = directory / file_name
        pq.write_table(
            table,
            path,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
            data_page_version="1.0",
        )
        path.chmod(0o600)
        metadata[name] = {
            "file": file_name,
            "logical_identity": _arrow_table_hash(table),
            "retrieved_at": _canonical_timestamp(retrieved_at[name]),
            "rows": int(table.num_rows),
            "sha256": _file_hash(path),
        }
        if name == "daily":
            cast(dict[str, object], metadata[name])["source"] = _query_provenance(capture.daily)
        elif name == "adj_factors" and capture.adj_factors is not None:
            cast(dict[str, object], metadata[name])["source"] = _query_provenance(
                capture.adj_factors
            )
        elif name == "stock_limits" and capture.stock_limits is not None:
            cast(dict[str, object], metadata[name])["source"] = _query_provenance(
                capture.stock_limits
            )
        elif name == "trade_calendar":
            cast(dict[str, object], metadata[name])["source"] = _query_provenance(
                capture.trade_calendar
            )
    return metadata


def _arrow_tables(capture: TushareDataCapture, *, pa: Any) -> dict[str, Any]:
    listings = pa.Table.from_pylist(
        [
            {
                "instrument_id": item.instrument_id,
                "tushare_code": item.tushare_code,
                "symbol": item.symbol,
                "name": item.name,
                "exchange": item.exchange,
                "current_status": item.current_status,
                "listed_on": item.listed_on,
                "delisted_on": item.delisted_on,
            }
            for item in capture.listing_snapshot.listings
        ],
        schema=_arrow_schemas(pa)["listings"],
    )
    listing_anomalies = pa.Table.from_pylist(
        [
            {
                "tushare_code": item.tushare_code,
                "symbol": item.symbol,
                "name": item.name,
                "exchange": item.exchange,
                "current_status": item.current_status,
                "listed_on": item.listed_on,
                "delisted_on": item.delisted_on,
                "reason": item.reason,
            }
            for item in capture.listing_snapshot.anomalies
        ],
        schema=_arrow_schemas(pa)["listing_anomalies"],
    )
    universe = pa.Table.from_pylist(
        [{"instrument_id": item} for item in capture.universe.instrument_ids],
        schema=_arrow_schemas(pa)["universe"],
    )

    calendar_rows = _rows_by_field(capture.trade_calendar)
    trade_calendar = pa.Table.from_pylist(
        [
            {
                "exchange": _string_value(row["exchange"], "exchange"),
                "cal_date": _yyyymmdd_value(row["cal_date"], "cal_date"),
                "is_open": row["is_open"] in (1, "1"),
                "pretrade_date": _optional_yyyymmdd_value(row["pretrade_date"], "pretrade_date"),
            }
            for row in calendar_rows
        ],
        schema=_arrow_schemas(pa)["trade_calendar"],
    )

    daily_rows = _rows_by_field(capture.daily)
    daily = pa.Table.from_pylist(
        [
            {
                "ts_code": _string_value(row["ts_code"], "ts_code"),
                "trade_date": _yyyymmdd_value(row["trade_date"], "trade_date"),
                "open": _decimal_value(row["open"], "open"),
                "high": _decimal_value(row["high"], "high"),
                "low": _decimal_value(row["low"], "low"),
                "close": _decimal_value(row["close"], "close"),
                "pre_close": _decimal_value(row["pre_close"], "pre_close"),
                "vol": _decimal_value(row["vol"], "vol"),
                "amount": _decimal_value(row["amount"], "amount"),
            }
            for row in daily_rows
        ],
        schema=_arrow_schemas(pa)["daily"],
    )
    tables = {
        "daily": daily,
        "listing_anomalies": listing_anomalies,
        "listings": listings,
        "trade_calendar": trade_calendar,
        "universe": universe,
    }
    if capture.adj_factors is not None:
        tables["adj_factors"] = pa.Table.from_pylist(
            [
                {
                    "ts_code": _string_value(row["ts_code"], "ts_code"),
                    "trade_date": _yyyymmdd_value(row["trade_date"], "trade_date"),
                    "adj_factor": _decimal_value(row["adj_factor"], "adj_factor"),
                }
                for row in _rows_by_field(capture.adj_factors)
            ],
            schema=_arrow_schemas(pa)["adj_factors"],
        )
    if capture.stock_limits is not None:
        tables["stock_limits"] = pa.Table.from_pylist(
            [
                {
                    "ts_code": _string_value(row["ts_code"], "ts_code"),
                    "trade_date": _yyyymmdd_value(row["trade_date"], "trade_date"),
                    "pre_close": _decimal_value(row["pre_close"], "pre_close"),
                    "up_limit": _decimal_value(row["up_limit"], "up_limit"),
                    "down_limit": _decimal_value(row["down_limit"], "down_limit"),
                }
                for row in _rows_by_field(capture.stock_limits)
            ],
            schema=_arrow_schemas(pa)["stock_limits"],
        )
    return tables


def _core_manifest(
    capture: TushareDataCapture,
    *,
    tables: dict[str, object],
    pyarrow_version: str,
) -> dict[str, object]:
    return {
        "schema_version": (
            TUSHARE_HARDENED_DATA_BUNDLE_SCHEMA
            if capture.request.hardened
            else TUSHARE_DATA_BUNDLE_SCHEMA
        ),
        "format": {
            "compression": "zstd",
            "parquet_writer": f"pyarrow-{pyarrow_version}",
        },
        "provider_manifest": capture.provider_manifest.to_dict(),
        "request": {
            "as_of_date": capture.request.as_of_date.isoformat(),
            "end_date": capture.request.end_date.isoformat(),
            "exchange": capture.request.exchange,
            "instrument_id": capture.request.instrument_id,
            "start_date": capture.request.start_date.isoformat(),
            "tushare_code": capture.request.tushare_code,
            **(
                {"evaluation_start_date": capture.request.replay_start_date.isoformat()}
                if capture.request.hardened
                else {}
            ),
        },
        "listing_snapshot": {
            "anomaly_count": len(capture.listing_snapshot.anomalies),
            "provider_id": capture.listing_snapshot.provider_id,
            "provider_version": capture.listing_snapshot.provider_version,
            "queries": [_query_provenance(query) for query in capture.listing_snapshot.queries],
            "query_hashes": list(capture.listing_snapshot.query_hashes),
            "retrieved_at": _canonical_timestamp(capture.listing_snapshot.retrieved_at),
            "snapshot_hash": capture.listing_snapshot.snapshot_hash,
        },
        "universe": {
            "as_of_date": capture.universe.as_of_date.isoformat(),
            "built_at": _canonical_timestamp(capture.universe.built_at),
            "exchanges": list(capture.universe.exchanges),
            "listing_snapshot_hash": capture.universe.listing_snapshot_hash,
            "universe_id": capture.universe.universe_id,
        },
        "tables": tables,
    }


def _arrow_schemas(pa: Any) -> dict[str, Any]:
    return {
        "listing_anomalies": pa.schema(
            [
                ("tushare_code", pa.string()),
                ("symbol", pa.string()),
                ("name", pa.string()),
                ("exchange", pa.string()),
                ("current_status", pa.string()),
                ("listed_on", pa.date32()),
                ("delisted_on", pa.date32()),
                ("reason", pa.string()),
            ]
        ),
        "listings": pa.schema(
            [
                ("instrument_id", pa.string()),
                ("tushare_code", pa.string()),
                ("symbol", pa.string()),
                ("name", pa.string()),
                ("exchange", pa.string()),
                ("current_status", pa.string()),
                ("listed_on", pa.date32()),
                ("delisted_on", pa.date32()),
            ]
        ),
        "universe": pa.schema([("instrument_id", pa.string())]),
        "trade_calendar": pa.schema(
            [
                ("exchange", pa.string()),
                ("cal_date", pa.date32()),
                ("is_open", pa.bool_()),
                ("pretrade_date", pa.date32()),
            ]
        ),
        "daily": pa.schema(
            [
                ("ts_code", pa.string()),
                ("trade_date", pa.date32()),
                ("open", pa.decimal128(24, 6)),
                ("high", pa.decimal128(24, 6)),
                ("low", pa.decimal128(24, 6)),
                ("close", pa.decimal128(24, 6)),
                ("pre_close", pa.decimal128(24, 6)),
                ("vol", pa.decimal128(28, 6)),
                ("amount", pa.decimal128(28, 6)),
            ]
        ),
        "adj_factors": pa.schema(
            [
                ("ts_code", pa.string()),
                ("trade_date", pa.date32()),
                ("adj_factor", pa.decimal128(24, 6)),
            ]
        ),
        "stock_limits": pa.schema(
            [
                ("ts_code", pa.string()),
                ("trade_date", pa.date32()),
                ("pre_close", pa.decimal128(24, 6)),
                ("up_limit", pa.decimal128(24, 6)),
                ("down_limit", pa.decimal128(24, 6)),
            ]
        ),
    }


def _query_provenance(table: TushareTable) -> dict[str, object]:
    return {
        "api_name": table.api_name,
        "content_hash": table.content_hash,
        "endpoint": table.endpoint,
        "fields": list(table.fields),
        "params": dict(table.params),
        "retrieved_at": _canonical_timestamp(table.retrieved_at),
    }


def _validate_manifest_shape(
    manifest: dict[str, object],
    *,
    table_files: dict[str, str],
) -> None:
    _require_exact_keys(
        manifest,
        {
            "bundle_hash",
            "data_snapshot_id",
            "format",
            "listing_snapshot",
            "provider_manifest",
            "request",
            "schema_version",
            "tables",
            "universe",
        },
        "Tushare bundle manifest",
    )
    format_manifest = _required_mapping(manifest, "format")
    _require_exact_keys(
        format_manifest,
        {"compression", "parquet_writer"},
        "Tushare bundle format",
    )
    if _required_string(format_manifest, "compression") != "zstd":
        raise ValueError("Tushare bundle compression must be zstd")
    _required_string(format_manifest, "parquet_writer")

    request = _required_mapping(manifest, "request")
    request_keys = {
        "as_of_date",
        "end_date",
        "exchange",
        "instrument_id",
        "start_date",
        "tushare_code",
    }
    if manifest.get("schema_version") == TUSHARE_HARDENED_DATA_BUNDLE_SCHEMA:
        request_keys.add("evaluation_start_date")
    _require_exact_keys(request, request_keys, "Tushare bundle request")
    listing_snapshot = _required_mapping(manifest, "listing_snapshot")
    _require_exact_keys(
        listing_snapshot,
        {
            "anomaly_count",
            "provider_id",
            "provider_version",
            "queries",
            "query_hashes",
            "retrieved_at",
            "snapshot_hash",
        },
        "Listing Snapshot manifest",
    )
    universe = _required_mapping(manifest, "universe")
    _require_exact_keys(
        universe,
        {
            "as_of_date",
            "built_at",
            "exchanges",
            "listing_snapshot_hash",
            "universe_id",
        },
        "Pre-event Universe manifest",
    )
    tables = _required_mapping(manifest, "tables")
    _require_exact_keys(tables, set(table_files), "Tushare bundle tables")
    common_table_fields = {
        "file",
        "logical_identity",
        "retrieved_at",
        "rows",
        "sha256",
    }
    for table_name in table_files:
        metadata = _required_mapping(tables, table_name)
        source_fields: set[str] = (
            {"source"}
            if table_name in {"adj_factors", "daily", "stock_limits", "trade_calendar"}
            else set()
        )
        expected_fields = common_table_fields | source_fields
        _require_exact_keys(metadata, expected_fields, f"{table_name} table manifest")


def _validate_parquet_compression(parquet_file: Any, file_name: str) -> None:
    metadata = parquet_file.metadata
    if metadata.num_row_groups == 0:
        raise ValueError(f"{file_name} must contain a Parquet row group")
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        for column_index in range(row_group.num_columns):
            if row_group.column(column_index).compression != "ZSTD":
                raise ValueError(f"{file_name} compression does not match zstd")


def _validate_bundle_semantics(
    *,
    manifest: dict[str, object],
    request: TushareDataRequest,
    provider_manifest: ProviderManifest,
    tables: dict[str, object],
    arrow_tables: dict[str, Any],
) -> str:
    listing_manifest = _required_mapping(manifest, "listing_snapshot")
    listings = _listings_from_arrow(arrow_tables["listings"])
    anomalies = _listing_anomalies_from_arrow(arrow_tables["listing_anomalies"])
    if _required_integer(listing_manifest, "anomaly_count") != len(anomalies):
        raise ValueError("Listing Snapshot anomaly_count does not match its table")
    listing_queries = _required_object_array(listing_manifest, "queries")
    if len(listing_queries) != len(_LISTING_PARTITIONS):
        raise ValueError("Listing Snapshot must retain all eight source queries")

    query_hashes: list[str] = []
    query_times: list[datetime] = []
    endpoints: set[str] = set()
    for query, (exchange, status) in zip(listing_queries, _LISTING_PARTITIONS, strict=True):
        retrieved_at, content_hash, endpoint = _validate_query_provenance(
            query,
            expected_api_name="stock_basic",
            expected_params={"exchange": exchange, "list_status": status},
            expected_fields=_STOCK_SOURCE_FIELDS,
        )
        source_rows = tuple(
            (
                listing.tushare_code,
                listing.symbol,
                listing.name,
                listing.exchange,
                listing.current_status,
                listing.listed_on.strftime("%Y%m%d"),
                listing.delisted_on.strftime("%Y%m%d") if listing.delisted_on else "",
            )
            for listing in listings
            if listing.exchange == exchange and listing.current_status == status
        ) + tuple(
            (
                anomaly.tushare_code,
                anomaly.symbol,
                anomaly.name,
                anomaly.exchange,
                anomaly.current_status,
                anomaly.listed_on.strftime("%Y%m%d"),
                anomaly.delisted_on.strftime("%Y%m%d") if anomaly.delisted_on else "",
            )
            for anomaly in anomalies
            if anomaly.exchange == exchange and anomaly.current_status == status
        )
        expected_content_hash = tushare_table_content_hash(
            api_name="stock_basic",
            params={"exchange": exchange, "list_status": status},
            fields=_STOCK_SOURCE_FIELDS,
            rows=tuple(sorted(source_rows, key=_canonical_row_key)),
        )
        if content_hash != expected_content_hash:
            raise ValueError("Listing Snapshot query hash does not match listing source tables")
        query_hashes.append(content_hash)
        query_times.append(retrieved_at)
        endpoints.add(endpoint)

    persisted_query_hashes = _required_string_array(listing_manifest, "query_hashes")
    if persisted_query_hashes != tuple(query_hashes):
        raise ValueError("Listing Snapshot query_hashes do not match source queries")
    listing_retrieved_at = _required_timestamp(listing_manifest, "retrieved_at")
    if listing_retrieved_at != max(query_times):
        raise ValueError("Listing Snapshot retrieval time does not match its source queries")
    snapshot_hash = _listing_snapshot_hash(
        provider_id=provider_manifest.provider_id,
        provider_version=provider_manifest.provider_version,
        retrieved_at=listing_retrieved_at,
        listings=listings,
        anomalies=anomalies,
        query_hashes=persisted_query_hashes,
    )
    if _required_string(listing_manifest, "snapshot_hash") != snapshot_hash:
        raise ValueError("Listing Snapshot hash does not match its persisted source tables")
    listing_table_manifest = _required_mapping(tables, "listings")
    if _required_timestamp(listing_table_manifest, "retrieved_at") != listing_retrieved_at:
        raise ValueError("listings.parquet retrieval time does not match Listing Snapshot")
    anomaly_table_manifest = _required_mapping(tables, "listing_anomalies")
    if _required_timestamp(anomaly_table_manifest, "retrieved_at") != listing_retrieved_at:
        raise ValueError("listing_anomalies.parquet retrieval time does not match Listing Snapshot")

    snapshot = StockListingSnapshot(
        provider_id=provider_manifest.provider_id,
        provider_version=provider_manifest.provider_version,
        retrieved_at=listing_retrieved_at,
        listings=listings,
        anomalies=anomalies,
        queries=(),
        query_hashes=persisted_query_hashes,
        snapshot_hash=snapshot_hash,
    )
    universe_manifest = _required_mapping(manifest, "universe")
    exchanges = _required_string_array(universe_manifest, "exchanges")
    if exchanges != ("SSE", "SZSE"):
        raise ValueError("Pre-event Universe must retain the complete SSE/SZSE scope")
    rebuilt_universe = build_pre_event_universe(
        snapshot,
        as_of_date=request.as_of_date,
        exchanges=exchanges,
    )
    universe_ids = _universe_ids_from_arrow(arrow_tables["universe"])
    if universe_ids != rebuilt_universe.instrument_ids:
        raise ValueError("universe.parquet is not derived from listings.parquet")
    if request.instrument_id not in universe_ids:
        raise ValueError("requested instrument is absent from universe.parquet")
    universe_id = _required_string(universe_manifest, "universe_id")
    if universe_id != rebuilt_universe.universe_id:
        raise ValueError("Pre-event Universe identity does not match universe.parquet")
    if _required_timestamp(universe_manifest, "built_at") != listing_retrieved_at:
        raise ValueError("Pre-event Universe build time does not match Listing Snapshot")
    universe_table_manifest = _required_mapping(tables, "universe")
    if _required_timestamp(universe_table_manifest, "retrieved_at") != listing_retrieved_at:
        raise ValueError("universe.parquet retrieval time does not match Pre-event Universe")

    calendar_manifest = _required_mapping(tables, "trade_calendar")
    calendar_source = _required_mapping(calendar_manifest, "source")
    calendar_params = {
        "end_date": request.end_date.strftime("%Y%m%d"),
        "exchange": request.exchange,
        "start_date": request.start_date.strftime("%Y%m%d"),
    }
    calendar_time, calendar_content_hash, calendar_endpoint = _validate_query_provenance(
        calendar_source,
        expected_api_name="trade_cal",
        expected_params=calendar_params,
        expected_fields=_CALENDAR_SOURCE_FIELDS,
    )
    if _required_timestamp(calendar_manifest, "retrieved_at") != calendar_time:
        raise ValueError("trade_calendar.parquet retrieval time does not match its source")
    if calendar_content_hash != tushare_table_content_hash(
        api_name="trade_cal",
        params=calendar_params,
        fields=_CALENDAR_SOURCE_FIELDS,
        rows=_calendar_source_rows(arrow_tables["trade_calendar"]),
    ):
        raise ValueError("trade_calendar.parquet does not match its source content_hash")
    endpoints.add(calendar_endpoint)

    daily_manifest = _required_mapping(tables, "daily")
    daily_source = _required_mapping(daily_manifest, "source")
    daily_params = {
        "end_date": request.end_date.strftime("%Y%m%d"),
        "start_date": request.start_date.strftime("%Y%m%d"),
        "ts_code": request.tushare_code,
    }
    daily_time, daily_content_hash, daily_endpoint = _validate_query_provenance(
        daily_source,
        expected_api_name="daily",
        expected_params=daily_params,
        expected_fields=_DAILY_SOURCE_FIELDS,
    )
    if _required_timestamp(daily_manifest, "retrieved_at") != daily_time:
        raise ValueError("daily.parquet retrieval time does not match its source")
    if daily_content_hash != tushare_table_content_hash(
        api_name="daily",
        params=daily_params,
        fields=_DAILY_SOURCE_FIELDS,
        rows=_daily_source_rows(arrow_tables["daily"]),
    ):
        raise ValueError("daily.parquet does not match its source content_hash")
    endpoints.add(daily_endpoint)

    if request.hardened:
        adj_time, adj_endpoint = _validate_hardened_table_provenance(
            tables=tables,
            table_name="adj_factors",
            api_name="adj_factor",
            params=daily_params,
            fields=_ADJ_FACTOR_SOURCE_FIELDS,
            rows=_adj_factor_source_rows(arrow_tables["adj_factors"]),
        )
        limit_time, limit_endpoint = _validate_hardened_table_provenance(
            tables=tables,
            table_name="stock_limits",
            api_name="stk_limit",
            params=daily_params,
            fields=_STK_LIMIT_SOURCE_FIELDS,
            rows=_stock_limit_source_rows(arrow_tables["stock_limits"]),
        )
        endpoints.update((adj_endpoint, limit_endpoint))
        if adj_time < listing_retrieved_at or limit_time < listing_retrieved_at:
            raise ValueError("hardened table retrieval cannot precede the Listing Snapshot")
    if len(endpoints) != 1:
        raise ValueError("Tushare bundle source queries must use one endpoint")

    open_dates = _calendar_dates_from_arrow(arrow_tables["trade_calendar"], request=request)
    daily_dates = _daily_dates_from_arrow(arrow_tables["daily"], request=request)
    missing = sorted(open_dates - daily_dates)
    if missing:
        values = ", ".join(item.isoformat() for item in missing)
        raise ValueError(f"daily.parquet omits exchange-open dates: {values}")
    unexpected = sorted(daily_dates - open_dates)
    if unexpected:
        values = ", ".join(item.isoformat() for item in unexpected)
        raise ValueError(f"daily.parquet contains non-open dates: {values}")
    if request.hardened:
        _validate_hardened_market_tables(arrow_tables, request=request)
    return universe_id


def _validate_hardened_table_provenance(
    *,
    tables: dict[str, object],
    table_name: str,
    api_name: str,
    params: dict[str, str],
    fields: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
) -> tuple[datetime, str]:
    manifest = _required_mapping(tables, table_name)
    source = _required_mapping(manifest, "source")
    retrieved_at, content_hash, endpoint = _validate_query_provenance(
        source,
        expected_api_name=api_name,
        expected_params=params,
        expected_fields=fields,
    )
    if _required_timestamp(manifest, "retrieved_at") != retrieved_at:
        raise ValueError(f"{table_name}.parquet retrieval time does not match its source")
    if content_hash != tushare_table_content_hash(
        api_name=api_name,
        params=params,
        fields=fields,
        rows=rows,
    ):
        raise ValueError(f"{table_name}.parquet does not match its source content_hash")
    return retrieved_at, endpoint


def _validate_hardened_market_tables(
    arrow_tables: dict[str, Any],
    *,
    request: TushareDataRequest,
) -> None:
    daily_by_date = {
        _required_date(row, "trade_date"): row for row in _arrow_rows(arrow_tables["daily"])
    }
    adj_by_date = {
        _required_date(row, "trade_date"): row for row in _arrow_rows(arrow_tables["adj_factors"])
    }
    limits_by_date = {
        _required_date(row, "trade_date"): row for row in _arrow_rows(arrow_tables["stock_limits"])
    }
    expected_dates = set(daily_by_date)
    if set(adj_by_date) != expected_dates or set(limits_by_date) != expected_dates:
        raise ValueError("hardened market tables must cover every daily session exactly")
    factors = {
        _required_decimal(row, "adj_factor", positive=True)
        for trade_date, row in adj_by_date.items()
        if trade_date >= request.replay_start_date
    }
    if len(factors) != 1:
        raise ValueError("adjustment-factor change makes the evaluation window ambiguous")
    tick = Decimal("0.01")
    ratio = Decimal("0.10")
    for trade_date, daily in daily_by_date.items():
        limits = limits_by_date[trade_date]
        if _required_string(daily, "ts_code") != _required_string(limits, "ts_code"):
            raise ValueError("stock-limit instrument does not match daily data")
        previous_close = _required_decimal(daily, "pre_close", positive=True)
        if _required_decimal(limits, "pre_close", positive=True) != previous_close:
            raise ValueError("stock-limit pre_close does not match daily data")
        expected_down = _limit_price(previous_close * (Decimal(1) - ratio), tick)
        expected_up = _limit_price(previous_close * (Decimal(1) + ratio), tick)
        if (
            _required_decimal(limits, "down_limit", positive=True) != expected_down
            or _required_decimal(limits, "up_limit", positive=True) != expected_up
        ):
            raise ValueError("source price limits do not match the supported 10% main-board rule")


def _limit_price(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).quantize(Decimal(1), rounding=ROUND_HALF_UP) * tick


def _validate_query_provenance(
    query: dict[str, object],
    *,
    expected_api_name: str,
    expected_params: dict[str, str],
    expected_fields: tuple[str, ...],
) -> tuple[datetime, str, str]:
    if set(query) != {
        "api_name",
        "content_hash",
        "endpoint",
        "fields",
        "params",
        "retrieved_at",
    }:
        raise ValueError("Tushare query provenance fields do not match the contract")
    if _required_string(query, "api_name") != expected_api_name:
        raise ValueError("Tushare query provenance has an unexpected api_name")
    params = _required_mapping(query, "params")
    if params != expected_params:
        raise ValueError("Tushare query provenance params do not match the request")
    if _required_string_array(query, "fields") != expected_fields:
        raise ValueError("Tushare query provenance fields do not match the request")
    endpoint = _required_string(query, "endpoint")
    _validate_persisted_endpoint(endpoint)
    content_hash = _required_string(query, "content_hash")
    _require_sha256(content_hash, "query content_hash")
    return _required_timestamp(query, "retrieved_at"), content_hash, endpoint


def _listings_from_arrow(table: Any) -> tuple[StockListing, ...]:
    listings: list[StockListing] = []
    for row in _arrow_rows(table):
        tushare_code = _required_string(row, "tushare_code")
        instrument_id = canonical_instrument_id(tushare_code)
        if _required_string(row, "instrument_id") != instrument_id:
            raise ValueError("listings.parquet instrument_id conflicts with tushare_code")
        exchange = _required_string(row, "exchange")
        if exchange != exchange_for_tushare_code(tushare_code):
            raise ValueError("listings.parquet exchange conflicts with tushare_code")
        symbol = _required_string(row, "symbol")
        if symbol != tushare_code.split(".", 1)[0]:
            raise ValueError("listings.parquet symbol conflicts with tushare_code")
        current_status = _required_string(row, "current_status")
        if current_status not in {"L", "D", "P", "G"}:
            raise ValueError("listings.parquet contains an unsupported status")
        listed_on = _required_date(row, "listed_on")
        delisted_on = _optional_date_value(row.get("delisted_on"), "delisted_on")
        if delisted_on is not None and delisted_on < listed_on:
            raise ValueError("listings.parquet delist date precedes list date")
        if current_status == "D" and delisted_on is None:
            raise ValueError("listings.parquet delisted stock requires delisted_on")
        listings.append(
            StockListing(
                instrument_id=instrument_id,
                tushare_code=tushare_code,
                symbol=symbol,
                name=_required_string(row, "name"),
                exchange=exchange,
                current_status=current_status,
                listed_on=listed_on,
                delisted_on=delisted_on,
            )
        )
    result = tuple(listings)
    if not result:
        raise ValueError("listings.parquet must not be empty")
    if result != tuple(sorted(result, key=lambda item: item.instrument_id)):
        raise ValueError("listings.parquet rows must use canonical order")
    if len({item.instrument_id for item in result}) != len(result):
        raise ValueError("listings.parquet contains duplicate instruments")
    return result


def _listing_anomalies_from_arrow(table: Any) -> tuple[StockListingAnomaly, ...]:
    anomalies: list[StockListingAnomaly] = []
    for row in _arrow_rows(table):
        tushare_code = _required_string(row, "tushare_code")
        try:
            canonical_instrument_id(tushare_code)
        except ValueError:
            pass
        else:
            raise ValueError("listing_anomalies.parquet contains a canonical Tushare stock code")
        parts = tushare_code.split(".")
        if len(parts) != 2 or not parts[0] or parts[1] not in {"SH", "SZ"}:
            raise ValueError("listing_anomalies.parquet code cannot be safely classified")
        exchange = _required_string(row, "exchange")
        expected_exchange = "SSE" if parts[1] == "SH" else "SZSE"
        if exchange != expected_exchange:
            raise ValueError("listing_anomalies.parquet exchange conflicts with tushare_code")
        symbol = _required_string(row, "symbol")
        if symbol != parts[0]:
            raise ValueError("listing_anomalies.parquet symbol conflicts with tushare_code")
        current_status = _required_string(row, "current_status")
        if current_status not in {"L", "D", "P", "G"}:
            raise ValueError("listing_anomalies.parquet contains an unsupported status")
        listed_on = _required_date(row, "listed_on")
        delisted_on = _optional_date_value(row.get("delisted_on"), "delisted_on")
        if delisted_on is not None and delisted_on < listed_on:
            raise ValueError("listing_anomalies.parquet delist date precedes list date")
        if current_status == "D" and delisted_on is None:
            raise ValueError("listing_anomalies.parquet delisted row requires delisted_on")
        reason = _required_string(row, "reason")
        if reason != "unsupported_tushare_stock_code":
            raise ValueError("listing_anomalies.parquet contains an unsupported reason")
        anomalies.append(
            StockListingAnomaly(
                tushare_code=tushare_code,
                symbol=symbol,
                name=_required_string(row, "name"),
                exchange=exchange,
                current_status=current_status,
                listed_on=listed_on,
                delisted_on=delisted_on,
                reason=reason,
            )
        )
    result = tuple(anomalies)
    expected_order = tuple(
        sorted(
            result,
            key=lambda item: (item.exchange, item.current_status, item.tushare_code),
        )
    )
    if result != expected_order:
        raise ValueError("listing_anomalies.parquet rows must use canonical order")
    keys = {(item.tushare_code, item.exchange, item.current_status) for item in result}
    if len(keys) != len(result):
        raise ValueError("listing_anomalies.parquet contains duplicate source rows")
    return result


def _universe_ids_from_arrow(table: Any) -> tuple[str, ...]:
    instrument_ids = tuple(_required_string(row, "instrument_id") for row in _arrow_rows(table))
    if not instrument_ids:
        raise ValueError("universe.parquet must not be empty")
    if instrument_ids != tuple(sorted(instrument_ids)):
        raise ValueError("universe.parquet rows must use canonical order")
    if len(set(instrument_ids)) != len(instrument_ids):
        raise ValueError("universe.parquet contains duplicate instruments")
    for instrument_id in instrument_ids:
        suffix = instrument_id.rsplit(".", 1)[-1]
        if suffix not in {"XSHG", "XSHE"}:
            raise ValueError("universe.parquet contains a non-A-share instrument")
    return instrument_ids


def _calendar_source_rows(table: Any) -> tuple[tuple[object, ...], ...]:
    rows: list[tuple[object, ...]] = []
    for row in _arrow_rows(table):
        is_open = row.get("is_open")
        if not isinstance(is_open, bool):
            raise ValueError("trade_calendar.parquet is_open must be boolean")
        pretrade_date = _optional_date_value(row.get("pretrade_date"), "pretrade_date")
        rows.append(
            (
                _required_string(row, "exchange"),
                _required_date(row, "cal_date").strftime("%Y%m%d"),
                1 if is_open else 0,
                pretrade_date.strftime("%Y%m%d") if pretrade_date else "",
            )
        )
    return tuple(rows)


def _daily_source_rows(table: Any) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            _required_string(row, "ts_code"),
            _required_date(row, "trade_date").strftime("%Y%m%d"),
            *(
                _required_decimal(row, field, positive=True)
                for field in ("open", "high", "low", "close", "pre_close")
            ),
            _required_decimal(row, "vol", positive=False),
            _required_decimal(row, "amount", positive=False),
        )
        for row in _arrow_rows(table)
    )


def _adj_factor_source_rows(table: Any) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            _required_string(row, "ts_code"),
            _required_date(row, "trade_date").strftime("%Y%m%d"),
            _required_decimal(row, "adj_factor", positive=True),
        )
        for row in _arrow_rows(table)
    )


def _stock_limit_source_rows(table: Any) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            _required_string(row, "ts_code"),
            _required_date(row, "trade_date").strftime("%Y%m%d"),
            _required_decimal(row, "pre_close", positive=True),
            _required_decimal(row, "up_limit", positive=True),
            _required_decimal(row, "down_limit", positive=True),
        )
        for row in _arrow_rows(table)
    )


def _calendar_dates_from_arrow(
    table: Any,
    *,
    request: TushareDataRequest,
) -> set[date]:
    rows = _arrow_rows(table)
    calendar_dates: list[date] = []
    open_dates: set[date] = set()
    for row in rows:
        if _required_string(row, "exchange") != request.exchange:
            raise ValueError("trade_calendar.parquet exchange does not match the request")
        calendar_date = _required_date(row, "cal_date")
        calendar_dates.append(calendar_date)
        is_open = row.get("is_open")
        if not isinstance(is_open, bool):
            raise ValueError("trade_calendar.parquet is_open must be boolean")
        _optional_date_value(row.get("pretrade_date"), "pretrade_date")
        if is_open:
            open_dates.add(calendar_date)
    expected_dates = tuple(
        date.fromordinal(ordinal)
        for ordinal in range(request.start_date.toordinal(), request.end_date.toordinal() + 1)
    )
    if tuple(calendar_dates) != expected_dates:
        raise ValueError("trade_calendar.parquet must cover every requested calendar date")
    if not open_dates:
        raise ValueError("trade_calendar.parquet contains no open sessions")
    return open_dates


def _daily_dates_from_arrow(
    table: Any,
    *,
    request: TushareDataRequest,
) -> set[date]:
    trade_dates: list[date] = []
    for row in _arrow_rows(table):
        if _required_string(row, "ts_code") != request.tushare_code:
            raise ValueError("daily.parquet instrument does not match the request")
        trade_date = _required_date(row, "trade_date")
        if not request.start_date <= trade_date <= request.end_date:
            raise ValueError("daily.parquet date falls outside the request")
        trade_dates.append(trade_date)
        prices = {
            field: _required_decimal(row, field, positive=True)
            for field in ("open", "high", "low", "close", "pre_close")
        }
        _required_decimal(row, "vol", positive=False)
        _required_decimal(row, "amount", positive=False)
        if prices["high"] < max(prices["open"], prices["low"], prices["close"]):
            raise ValueError("daily.parquet high is below another OHLC price")
        if prices["low"] > min(prices["open"], prices["high"], prices["close"]):
            raise ValueError("daily.parquet low is above another OHLC price")
    if not trade_dates:
        raise ValueError("daily.parquet must not be empty")
    if trade_dates != sorted(trade_dates) or len(set(trade_dates)) != len(trade_dates):
        raise ValueError("daily.parquet dates must be unique and use canonical order")
    return set(trade_dates)


def _listing_snapshot_hash(
    *,
    provider_id: str,
    provider_version: str,
    retrieved_at: datetime,
    listings: tuple[StockListing, ...],
    anomalies: tuple[StockListingAnomaly, ...],
    query_hashes: tuple[str, ...],
) -> str:
    return _canonical_hash(
        {
            "provider_id": provider_id,
            "provider_version": provider_version,
            "retrieved_at": _canonical_timestamp(retrieved_at),
            "listings": [
                {
                    "current_status": listing.current_status,
                    "delisted_on": (
                        listing.delisted_on.isoformat() if listing.delisted_on else None
                    ),
                    "exchange": listing.exchange,
                    "instrument_id": listing.instrument_id,
                    "listed_on": listing.listed_on.isoformat(),
                    "name": listing.name,
                    "symbol": listing.symbol,
                    "tushare_code": listing.tushare_code,
                }
                for listing in listings
            ],
            "anomalies": [
                {
                    "current_status": anomaly.current_status,
                    "delisted_on": (
                        anomaly.delisted_on.isoformat() if anomaly.delisted_on else None
                    ),
                    "exchange": anomaly.exchange,
                    "listed_on": anomaly.listed_on.isoformat(),
                    "name": anomaly.name,
                    "reason": anomaly.reason,
                    "symbol": anomaly.symbol,
                    "tushare_code": anomaly.tushare_code,
                }
                for anomaly in anomalies
            ],
            "query_hashes": list(query_hashes),
        }
    )


def _arrow_table_hash(table: Any) -> str:
    rows = [
        {key: _canonical_arrow_value(value) for key, value in row.items()}
        for row in _arrow_rows(table)
    ]
    return _canonical_hash(
        {
            "fields": [{"name": field.name, "type": str(field.type)} for field in table.schema],
            "rows": rows,
        }
    )


def _canonical_arrow_value(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if value is None or isinstance(value, (bool, str)):
        return value
    raise ValueError("Parquet table contains a non-canonical value")


def _arrow_rows(table: Any) -> tuple[dict[str, object], ...]:
    raw_rows = cast(list[object], table.to_pylist())
    if not all(isinstance(row, dict) for row in raw_rows):
        raise ValueError("Parquet table rows must be objects")
    return tuple(cast(dict[str, object], row) for row in raw_rows)


def _rows_by_field(table: TushareTable) -> tuple[dict[str, object], ...]:
    return tuple(dict(zip(table.fields, row, strict=True)) for row in table.rows)


def _yyyymmdd_value(value: object, field_name: str) -> date:
    raw = _string_value(value, field_name)
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must use a valid YYYYMMDD date") from exc


def _optional_yyyymmdd_value(value: object, field_name: str) -> date | None:
    if value in (None, ""):
        return None
    return _yyyymmdd_value(value, field_name)


def _decimal_value(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a JSON number")
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return number


def _string_value(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _pyarrow_modules() -> tuple[Any, Any]:
    try:
        pa = cast(Any, import_module("pyarrow"))
        pq = cast(Any, import_module("pyarrow.parquet"))
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Tushare bundle persistence requires the 'data' optional dependency"
        ) from exc
    return pa, pq


def _write_private_file(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(content)


def _remove_created_directory(path: Path) -> None:
    for child in path.iterdir():
        if not child.is_file():
            raise RuntimeError(f"refusing to remove unexpected bundle entry: {child}")
        child.unlink()
    path.rmdir()


def _required_mapping(fields: dict[str, object], name: str) -> dict[str, object]:
    value = fields.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _require_exact_keys(
    fields: dict[str, object],
    expected: set[str],
    name: str,
) -> None:
    missing = sorted(expected - fields.keys())
    unknown = sorted(fields.keys() - expected)
    if missing:
        raise ValueError(f"{name} missing fields: {', '.join(missing)}")
    if unknown:
        raise ValueError(f"{name} has unknown fields: {', '.join(unknown)}")


def _required_object_array(fields: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of objects")
    items = cast(list[object], value)
    if not all(isinstance(item, dict) for item in items):
        raise ValueError(f"{name} must be an array of objects")
    return tuple(cast(dict[str, object], item) for item in items)


def _required_string_array(fields: dict[str, object], name: str) -> tuple[str, ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of non-empty strings")
    items = cast(list[object], value)
    if not all(isinstance(item, str) and item for item in items):
        raise ValueError(f"{name} must be an array of non-empty strings")
    return tuple(cast(str, item) for item in items)


def _required_string(fields: dict[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _required_integer(fields: dict[str, object], name: str) -> int:
    value = fields.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _required_timestamp(fields: dict[str, object], name: str) -> datetime:
    raw = _required_string(fields, name)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if _canonical_timestamp(parsed) != raw:
        raise ValueError(f"{name} must use canonical UTC format")
    return parsed


def _required_date(fields: dict[str, object], name: str) -> date:
    value = fields.get(name)
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{name} must be a date")
    return value


def _optional_date_value(value: object, name: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{name} must be a date or null")
    return value


def _required_decimal(
    fields: dict[str, object],
    name: str,
    *,
    positive: bool,
) -> Decimal:
    value = fields.get(name)
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite decimal")
    if value <= 0 if positive else value < 0:
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {qualifier}")
    return value


def _require_sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _validate_persisted_endpoint(endpoint: str) -> None:
    if endpoint != TUSHARE_HTTP_ENDPOINT:
        raise ValueError("persisted Tushare endpoint must be the official HTTPS endpoint")


def _contains_forbidden_key(value: object) -> bool:
    if isinstance(value, dict):
        fields = cast(dict[object, object], value)
        for key, item in fields.items():
            if isinstance(key, str) and key.lower() in {
                "api_key",
                "secret",
                "token",
                "tushare_token",
            }:
                return True
            if _contains_forbidden_key(item):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in cast(list[object], value))
    return False


def _data_snapshot_id(request: TushareDataRequest, bundle_hash: str) -> str:
    return _data_snapshot_id_from_values(
        tushare_code=request.tushare_code,
        start_date=request.start_date.isoformat(),
        end_date=request.end_date.isoformat(),
        bundle_hash=bundle_hash,
    )


def _data_snapshot_id_from_values(
    *,
    tushare_code: str,
    start_date: str,
    end_date: str,
    bundle_hash: str,
) -> str:
    code_slug = tushare_code.lower().replace(".", "-")
    start_slug = start_date.replace("-", "")
    end_slug = end_date.replace("-", "")
    return f"tushare-{code_slug}-{start_slug}-{end_slug}-{bundle_hash[:16]}"


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode()).hexdigest()


def _canonical_row_key(row: tuple[object, ...]) -> str:
    return json.dumps(row, ensure_ascii=True, separators=(",", ":"))


def _pretty_json_bytes(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
