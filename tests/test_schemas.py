import json
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

ROOT = Path(__file__).parents[1]


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


@pytest.mark.parametrize(
    "schema_name",
    [
        "event-transmission.schema.json",
        "order-intent.schema.json",
        "provider-manifest.schema.json",
        "signal-intent.schema.json",
    ],
)
def test_schema_is_valid(schema_name: str) -> None:
    Draft202012Validator.check_schema(load_json(ROOT / "schemas" / schema_name))


@pytest.mark.parametrize(
    "example_path",
    [
        "examples/events/synthetic-energy-supply-shock.json",
        "examples/providers/nautilus-planned.json",
        "examples/providers/tushare-http-planned.json",
        "examples/providers/veighna-external-bridge.json",
    ],
)
def test_examples_conform_to_schema(example_path: str) -> None:
    instance = load_json(ROOT / example_path)
    if example_path.startswith("examples/events/"):
        schema_name = "event-transmission.schema.json"
    else:
        schema_name = "provider-manifest.schema.json"
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / schema_name),
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


def load_json(path: Path) -> dict[str, Any]:
    payload = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(payload, dict):
        raise TypeError(f"expected object in {path}")
    return cast(dict[str, Any], payload)
