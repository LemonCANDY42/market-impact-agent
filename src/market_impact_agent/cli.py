from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, cast

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent import __version__
from market_impact_agent.backtests import (
    BacktestRunStatus,
    backtest_request_from_dict,
    backtest_result_to_dict,
)
from market_impact_agent.events import event_transmission_chronology_errors
from market_impact_agent.providers import MockExecutionProvider, ProviderManifest
from market_impact_agent.registry import ProviderRegistry
from market_impact_agent.tushare import TushareHttpAdapter
from market_impact_agent.tushare_bundle import (
    TushareDataRequest,
    ValidatedTushareDataBundle,
    capture_tushare_data_bundle,
    validate_tushare_data_bundle,
    write_tushare_data_bundle,
)


class EventTransmissionValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-impact")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="Print fail-closed runtime and provider status")

    provider_parser = subparsers.add_parser("provider", help="Inspect provider manifests")
    provider_subparsers = provider_parser.add_subparsers(dest="provider_command", required=True)
    validate_parser = provider_subparsers.add_parser(
        "validate", help="Validate a provider manifest"
    )
    validate_parser.add_argument("path", type=Path)

    event_parser = subparsers.add_parser(
        "event", help="Validate point-in-time event transmission records"
    )
    event_subparsers = event_parser.add_subparsers(dest="event_command", required=True)
    event_validate_parser = event_subparsers.add_parser(
        "validate", help="Validate a point-in-time event assessment"
    )
    event_validate_parser.add_argument("path", type=Path)

    tushare_parser = subparsers.add_parser(
        "tushare", help="Capture or validate local Tushare data bundles"
    )
    tushare_subparsers = tushare_parser.add_subparsers(dest="tushare_command", required=True)
    tushare_capture_parser = tushare_subparsers.add_parser(
        "capture", help="Capture one token-backed read-only data window"
    )
    tushare_capture_parser.add_argument("--instrument", required=True)
    tushare_capture_parser.add_argument("--as-of-date", required=True, type=_compact_date)
    tushare_capture_parser.add_argument("--start-date", required=True, type=_compact_date)
    tushare_capture_parser.add_argument("--end-date", required=True, type=_compact_date)
    tushare_validate_parser = tushare_subparsers.add_parser(
        "validate", help="Validate one local Tushare data bundle"
    )
    tushare_validate_parser.add_argument("path", type=Path)

    backtest_parser = subparsers.add_parser("backtest", help="Run deterministic backtests")
    backtest_subparsers = backtest_parser.add_subparsers(dest="backtest_command", required=True)
    backtest_run_parser = backtest_subparsers.add_parser(
        "run", help="Replay one strict request from a validated private Data Snapshot"
    )
    backtest_run_parser.add_argument("--request", required=True, type=Path)
    backtest_run_parser.add_argument("--data-snapshot", required=True, type=Path)
    return parser


def default_registry() -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(MockExecutionProvider())
    return registry


def status_payload() -> dict[str, object]:
    return {
        "project": "market-impact-agent",
        "version": __version__,
        "python": platform.python_version(),
        "live_trading": "disabled",
        "providers": [manifest.to_dict() for manifest in default_registry().manifests()],
    }


def validate_provider(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    manifest = ProviderManifest.from_dict(payload)
    errors = manifest.validation_errors()
    return {
        "path": path.as_posix(),
        "provider_id": manifest.provider_id,
        "valid": not errors,
        "errors": list(errors),
        "verified_capabilities": sorted(
            capability.value for capability in manifest.verified_capabilities
        ),
    }


def validate_event(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    schema_errors = _event_transmission_schema_errors(payload)
    errors = schema_errors or event_transmission_chronology_errors(payload)
    return {
        "path": path.as_posix(),
        "valid": not errors,
        "errors": list(errors),
    }


def capture_tushare(
    *,
    token: str,
    tushare_code: str,
    as_of_date: date,
    start_date: date,
    end_date: date,
    output_root: Path = Path(".market-impact/tushare"),
) -> ValidatedTushareDataBundle:
    request = TushareDataRequest(
        tushare_code=tushare_code,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    capture = capture_tushare_data_bundle(TushareHttpAdapter(token), request)
    path = write_tushare_data_bundle(capture, output_root)
    return validate_tushare_data_bundle(path)


def _event_transmission_schema_errors(payload: object) -> tuple[str, ...]:
    schema_path = _event_transmission_schema_path()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = cast(
        EventTransmissionValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
    errors = sorted(
        validator.iter_errors(payload), key=lambda error: (error.json_path, error.message)
    )
    return tuple(_format_schema_error(error) for error in errors)


def _event_transmission_schema_path() -> Path:
    package_root = Path(__file__).resolve().parent
    installed_schema = package_root / "schemas" / "event-transmission.schema.json"
    if installed_schema.is_file():
        return installed_schema
    return package_root.parents[1] / "schemas" / "event-transmission.schema.json"


def _format_schema_error(error: ValidationError) -> str:
    return f"{error.json_path}: {error.message}"


def _compact_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use valid YYYYMMDD values") from exc


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "status":
        print(json.dumps(status_payload(), indent=2, sort_keys=True))
        return 0
    if args.command == "provider" and args.provider_command == "validate":
        try:
            result = validate_provider(args.path)
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "event" and args.event_command == "validate":
        try:
            result = validate_event(args.path)
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "tushare" and args.tushare_command == "capture":
        token = os.environ.get("TUSHARE_TOKEN", "")
        if not token:
            print(
                json.dumps({"captured": False, "error": "TUSHARE_TOKEN is not configured"}),
                file=sys.stderr,
            )
            return 1
        try:
            bundle = capture_tushare(
                token=token,
                tushare_code=args.instrument,
                as_of_date=args.as_of_date,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"captured": False, "error": str(exc)}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "bundle_hash": bundle.bundle_hash,
                    "captured": True,
                    "data_snapshot_id": bundle.data_snapshot_id,
                    "instrument_id": bundle.instrument_id,
                    "listing_anomaly_count": bundle.listing_anomaly_count,
                    "path": bundle.path.as_posix(),
                    "provider_verified": False,
                    "universe_id": bundle.universe_id,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "tushare" and args.tushare_command == "validate":
        try:
            bundle = validate_tushare_data_bundle(args.path)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "bundle_hash": bundle.bundle_hash,
                    "data_snapshot_id": bundle.data_snapshot_id,
                    "instrument_id": bundle.instrument_id,
                    "listing_anomaly_count": bundle.listing_anomaly_count,
                    "path": bundle.path.as_posix(),
                    "universe_id": bundle.universe_id,
                    "valid": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "backtest" and args.backtest_command == "run":
        try:
            request_payload = json.loads(args.request.read_text(encoding="utf-8"))
            request = backtest_request_from_dict(request_payload)
            from market_impact_agent.tushare_replay import run_validated_tushare_replay

            result = run_validated_tushare_replay(request, args.data_snapshot)
        except (
            ImportError,
            KeyError,
            ModuleNotFoundError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(backtest_result_to_dict(result), indent=2, sort_keys=True))
        return 0 if result.status is BacktestRunStatus.COMPLETED else 1
    raise AssertionError("unreachable command")
