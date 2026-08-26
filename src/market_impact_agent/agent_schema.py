from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Protocol, cast

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry
from referencing.jsonschema import DRAFT202012, Schema

AGENT_SCHEMA_FILES = (
    "pattern-pack.schema.json",
    "evidence-pack.schema.json",
    "judgment-proposal.schema.json",
    "judgment-artifact.schema.json",
    "exposure-registry.schema.json",
    "agent-phase2-preregistration.schema.json",
)


class ContractValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


def validate_agent_contract(payload: object, schema_file: str) -> tuple[str, ...]:
    if schema_file not in AGENT_SCHEMA_FILES:
        raise ValueError(f"unsupported Agent contract schema: {schema_file}")
    schemas = {name: _read_schema(name) for name in AGENT_SCHEMA_FILES}
    registry: Registry[Schema] = Registry()
    for schema in schemas.values():
        schema_id = schema.get("$id")
        if not isinstance(schema_id, str):
            raise TypeError("Agent contract schema requires a string $id")
        registry = registry.with_resource(schema_id, DRAFT202012.create_resource(schema))
    validator = cast(
        ContractValidator,
        Draft202012Validator(
            schemas[schema_file],
            registry=registry,
            format_checker=FormatChecker(),
        ),
    )
    errors = sorted(
        validator.iter_errors(payload),
        key=lambda error: (error.json_path, error.message),
    )
    return tuple(f"{error.json_path}: {error.message}" for error in errors)


def _read_schema(name: str) -> dict[str, object]:
    package_root = Path(__file__).resolve().parent
    installed = package_root / "schemas" / name
    path = installed if installed.is_file() else package_root.parents[1] / "schemas" / name
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Agent contract schema must be an object: {name}")
    raw = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"Agent contract schema keys must be strings: {name}")
    return cast(dict[str, object], payload)
