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
    "candidate-event-observation.schema.json",
    "source-coverage-registration.schema.json",
    "coverage-receipt.schema.json",
    "agent-ensemble-decision.schema.json",
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
    "news-observation-batch.schema.json",
    "method-skill-catalog.schema.json",
    "method-evidence-declaration.schema.json",
    "method-skill-ablation-registration.schema.json",
    "method-skill-ablation-audit-correction.schema.json",
    "regime-agent-experiment-report.schema.json",
    "regime-agent-validation-registration.schema.json",
    "regime-agent-validation-report.schema.json",
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
