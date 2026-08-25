from __future__ import annotations

import argparse
import json
import platform
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Protocol, cast

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent import __version__
from market_impact_agent.events import event_transmission_chronology_errors
from market_impact_agent.providers import MockExecutionProvider, ProviderManifest
from market_impact_agent.registry import ProviderRegistry


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
    raise AssertionError("unreachable command")
