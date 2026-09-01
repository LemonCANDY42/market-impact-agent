"""Harness-owned Modeled-PIT readiness materialization.

There is deliberately no caller-buildable readiness bundle or replay authority in
this module. The prospective Decision pipeline reopens the durable sources and
calls the private materializer below. The resulting artifact can authorize a
later Judgment only; current tradability and an executable price remain required
before any non-empty Intent.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, time
from functools import lru_cache
from pathlib import Path
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRuleSet,
    build_checkpoint_market_universe_view,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_checkpoint_sets import (
    ProspectiveCheckpointSnapshotSet,
    materialize_checkpoint_decision_inputs,
)
from market_impact_agent.prospective_diagnostic import (
    CapabilityApplicability,
    ProspectiveDiagnosticRegistration,
)
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveEventAssessmentArtifact,
    ProspectiveTriggerAdmission,
    TransmissionPath,
)

MODELED_PIT_READINESS_CHECKPOINT_SCHEMA = "market-impact.modeled-pit-readiness-checkpoint.v1"
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _materialize_modeled_pit_readiness_checkpoints(  # pyright: ignore[reportUnusedFunction]
    *,
    registration: ProspectiveDiagnosticRegistration,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    snapshot_store: LocalDataSnapshotStore,
    trigger: ProspectiveTriggerAdmission,
    assessment: ProspectiveEventAssessmentArtifact,
    rule_set: ExchangeInstrumentRuleSet,
) -> tuple[dict[str, object], ...]:
    """Derive readiness only from sources reopened by the composition root."""

    if snapshot_set.__class__ is not ProspectiveCheckpointSnapshotSet:
        raise ValueError("Modeled-PIT requires the exact canonical Snapshot Set")
    if snapshot_store.__class__ is not LocalDataSnapshotStore:
        raise ValueError("Modeled-PIT requires the exact Harness Snapshot store")
    if (
        trigger.registration_id != registration.registration_id
        or trigger.checkpoint_key != snapshot_set.checkpoint_key
        or trigger.registration_id != snapshot_set.registration_id
        or trigger.event_assessment_id != assessment.assessment_id
    ):
        raise ValueError("Modeled-PIT sources do not share one durable Trigger context")

    checkpoint = registration.checkpoint(snapshot_set.checkpoint_key)
    inputs = materialize_checkpoint_decision_inputs(snapshot_set, store=snapshot_store)
    universe = build_checkpoint_market_universe_view(
        decision_inputs=inputs,
        rule_set=rule_set,
        target_venues=checkpoint.target_venues,
        allowed_instrument_classes=checkpoint.allowed_instrument_classes,
    )
    event_record_ids = tuple(
        sorted(
            cast(str, item["record_id"])
            for item in inputs
            if item["capability"] == ObservationCapability.EVENT_REVELATION.value
            and item["record_type"] == "event_fact"
        )
    )
    expectation = _prior_expectation(
        inputs,
        registration=registration,
        checkpoint_key=checkpoint.checkpoint_key,
    )
    results = tuple(
        _checkpoint_for_path(
            registration=registration,
            snapshot_set=snapshot_set,
            trigger=trigger,
            assessment=assessment,
            path=path,
            inputs=inputs,
            universe=universe,
            event_record_ids=event_record_ids,
            expectation=expectation,
        )
        for path in assessment.paths
    )
    return tuple(sorted(results, key=lambda item: cast(str, item["checkpoint_id"])))


def _checkpoint_for_path(
    *,
    registration: ProspectiveDiagnosticRegistration,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    trigger: ProspectiveTriggerAdmission,
    assessment: ProspectiveEventAssessmentArtifact,
    path: TransmissionPath,
    inputs: tuple[dict[str, object], ...],
    universe: dict[str, object],
    event_record_ids: tuple[str, ...],
    expectation: dict[str, object],
) -> dict[str, object]:
    checkpoint = registration.checkpoint(snapshot_set.checkpoint_key)
    if path.horizon_sessions not in checkpoint.candidate_horizon_sessions:
        raise PermissionError("EventAssessment horizon is absent from durable preregistration")
    if (
        path.target_id not in trigger.admitted_target_ids
        or path.venue not in checkpoint.target_venues
        or path.instrument_class not in checkpoint.allowed_instrument_classes
    ):
        raise PermissionError("EventAssessment path is outside the registered Trigger boundary")

    target_state = _target_state(
        path,
        inputs=inputs,
        universe=universe,
        cutoff_at=snapshot_set.barrier_at,
    )
    judgment_blockers: set[str] = set()
    information_gaps: set[str] = {
        "current_tradability_unknown",
        "suspension_status_unknown",
        "hedge_mapping_unavailable",
    }
    if not snapshot_set.complete:
        judgment_blockers.add("checkpoint_snapshot_set_incomplete")
    if not event_record_ids:
        judgment_blockers.add("event_fact_missing")
    if expectation["kind"] == "unknown":
        information_gaps.add("prior_expectation_unknown")
        if expectation["required"] is True:
            judgment_blockers.add("prior_expectation_required")
    if target_state["mapping_status"] != "then_effective":
        judgment_blockers.add("target_mapping_unavailable")
    if target_state["research_eligible"] is not True:
        judgment_blockers.add("target_research_ineligible")

    intent_blockers = {
        *judgment_blockers,
        "current_tradability_unverified",
        "executable_raw_price_unavailable",
        "suspension_status_unverified",
    }
    core: dict[str, object] = {
        "schema_version": MODELED_PIT_READINESS_CHECKPOINT_SCHEMA,
        "registration_id": registration.registration_id,
        "checkpoint_snapshot_set_id": snapshot_set.snapshot_set_id,
        "market_universe_view_id": universe["view_id"],
        "trigger_admission_id": trigger.admission_id,
        "event_assessment_id": assessment.assessment_id,
        "checkpoint_key": snapshot_set.checkpoint_key,
        "cutoff_at": _timestamp(snapshot_set.barrier_at),
        "path": path.to_dict(),
        "new_fact_record_ids": list(event_record_ids),
        "prior_expectation": {
            key: value for key, value in expectation.items() if key != "required"
        },
        "target_state": target_state,
        "hedge_readiness": {
            "status": "unavailable",
            "reason_code": "portfolio_exposure_mapping_not_registered",
        },
        "judgment_blockers": sorted(judgment_blockers),
        "judgment_information_gaps": sorted(information_gaps),
        "intent_blockers": sorted(intent_blockers),
        "judgment_ready": not judgment_blockers,
        "intent_ready": False,
        "historical_pit_claim": False,
        "model_call_authorized": False,
        "execution_capability": False,
    }
    result = {
        **core,
        "checkpoint_id": f"modeled-pit-readiness-checkpoint-{canonical_hash(core)}",
    }
    return parse_untrusted_modeled_pit_readiness_checkpoint(result)


def _prior_expectation(
    inputs: tuple[dict[str, object], ...],
    *,
    registration: ProspectiveDiagnosticRegistration,
    checkpoint_key: str,
) -> dict[str, object]:
    record_ids = sorted(
        cast(str, item["record_id"])
        for item in inputs
        if item["capability"] == ObservationCapability.PRIOR_EXPECTATION.value
    )
    required = (
        registration.checkpoint(checkpoint_key)
        .slot(ObservationCapability.PRIOR_EXPECTATION)
        .applicability
        is CapabilityApplicability.REQUIRED
    )
    if record_ids:
        return {"kind": "observation", "record_ids": record_ids, "required": required}
    return {
        "kind": "unknown",
        "reason_code": "no_registered_source",
        "required": required,
    }


def _target_state(
    path: TransmissionPath,
    *,
    inputs: tuple[dict[str, object], ...],
    universe: Mapping[str, object],
    cutoff_at: datetime,
) -> dict[str, object]:
    instruments = [
        cast(dict[str, object], item) for item in cast(list[object], universe["instruments"])
    ]
    instrument = next(
        (
            item
            for item in instruments
            if item.get("instrument_code") == path.target_id
            and item.get("venue") == path.venue
            and item.get("instrument_class") == path.instrument_class
        ),
        None,
    )
    if instrument is None:
        return {
            "target_id": path.target_id,
            "venue": path.venue,
            "instrument_class": path.instrument_class,
            "mapping_status": "unavailable",
            "research_eligible": False,
            "decision_time_tradability": "ineligible",
            "suspension_status": "unknown",
            "raw_price_record_id": None,
            "raw_price_trade_date": None,
            "raw_price_session_close_at": None,
            "raw_price_available_at": None,
            "raw_price_execution_eligible": False,
        }

    raw_price_record_id = cast(str | None, instrument["raw_price_record_id"])
    raw_price = (
        None
        if raw_price_record_id is None
        else next((item for item in inputs if item["record_id"] == raw_price_record_id), None)
    )
    trade_date: str | None = None
    session_close_at: str | None = None
    available_at: str | None = None
    if raw_price is not None:
        data = cast(dict[str, object], raw_price["data"])
        trade_date = _trade_date(data.get("trade_date"))
        session_close = datetime.combine(
            datetime.strptime(trade_date, "%Y%m%d").date(),
            time(15),
            tzinfo=_SHANGHAI,
        ).astimezone(UTC)
        if session_close > cutoff_at:
            raise ValueError("raw price trade-date session closes after the checkpoint cutoff")
        session_close_at = _timestamp(session_close)
        available_at = cast(str, cast(dict[str, object], raw_price["times"])["available_at"])

    return {
        "target_id": path.target_id,
        "venue": path.venue,
        "instrument_class": path.instrument_class,
        "mapping_status": "then_effective",
        "research_eligible": instrument["research_eligible"],
        "decision_time_tradability": instrument["decision_time_tradability"],
        "suspension_status": "unknown",
        "raw_price_record_id": raw_price_record_id,
        "raw_price_trade_date": trade_date,
        "raw_price_session_close_at": session_close_at,
        "raw_price_available_at": available_at,
        "raw_price_execution_eligible": False,
    }


def parse_untrusted_modeled_pit_readiness_checkpoint(value: object) -> dict[str, object]:
    """Parse checkpoint-shaped JSON without granting Harness authority.

    Content addressing proves only that the payload is internally consistent. A
    caller must use ``ProspectiveDecisionPipeline.reopen_modeled_pit_readiness``
    before treating any readiness result as a Harness-produced decision input.
    """

    errors = _schema_errors(value)
    if errors:
        raise ValueError("modeled-PIT readiness checkpoint schema error: " + "; ".join(errors))
    if not isinstance(value, dict):
        raise ValueError("modeled-PIT readiness checkpoint must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise ValueError("modeled-PIT readiness checkpoint must be an object")
    payload = {cast(str, key): item for key, item in raw.items()}
    if payload["schema_version"] != MODELED_PIT_READINESS_CHECKPOINT_SCHEMA:
        raise ValueError("unsupported modeled-PIT readiness checkpoint schema")
    if any(
        payload[field] is not False
        for field in ("historical_pit_claim", "model_call_authorized", "execution_capability")
    ):
        raise ValueError("modeled-PIT readiness checkpoint grants forbidden authority")
    core = {key: item for key, item in payload.items() if key != "checkpoint_id"}
    if payload["checkpoint_id"] != f"modeled-pit-readiness-checkpoint-{canonical_hash(core)}":
        raise ValueError("modeled-PIT readiness checkpoint ID does not match content")
    if payload["judgment_ready"] is not (not cast(list[str], payload["judgment_blockers"])):
        raise ValueError("modeled-PIT Judgment readiness contradicts blockers")
    if payload["intent_ready"] is not False or not cast(list[str], payload["intent_blockers"]):
        raise ValueError("modeled-PIT Intent readiness must remain fail-closed")
    return cast(
        dict[str, object],
        json.loads(json.dumps(payload, sort_keys=True, separators=(",", ":"))),
    )


def _record_pipeline_modeled_pit_readiness(  # pyright: ignore[reportUnusedFunction]
    *,
    store: LocalDataSnapshotStore,
    checkpoint: dict[str, object],
    artifact_hash: str,
    registration_artifact_hash: str,
    snapshot_set_artifact_hash: str,
    registration_id: str,
    snapshot_set_id: str,
    admission_id: str,
    assessment_id: str,
    rule_set_id: str,
    rule_set_artifact_hash: str,
) -> None:
    """Record one output after the pipeline has reopened its full source context."""

    parsed = parse_untrusted_modeled_pit_readiness_checkpoint(checkpoint)
    checkpoint_id = cast(str, parsed["checkpoint_id"])
    checkpoint_hash = checkpoint_id.removeprefix("modeled-pit-readiness-checkpoint-")
    if store.artifacts.read_json(artifact_hash) != parsed:
        raise ValueError("modeled-PIT artifact differs from checkpoint content")
    authority_id = store.harness_authority_id
    values = (
        checkpoint_id,
        checkpoint_hash,
        artifact_hash,
        authority_id,
        registration_id,
        registration_artifact_hash,
        snapshot_set_id,
        snapshot_set_artifact_hash,
        admission_id,
        assessment_id,
        rule_set_id,
        rule_set_artifact_hash,
    )
    with store.authority_transaction() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS modeled_pit_readiness_authority (
                checkpoint_id TEXT PRIMARY KEY,
                checkpoint_hash TEXT NOT NULL,
                artifact_hash TEXT NOT NULL UNIQUE,
                harness_authority_id TEXT NOT NULL,
                registration_id TEXT NOT NULL,
                registration_artifact_hash TEXT NOT NULL,
                snapshot_set_id TEXT NOT NULL,
                snapshot_set_artifact_hash TEXT NOT NULL,
                admission_id TEXT NOT NULL,
                assessment_id TEXT NOT NULL,
                rule_set_id TEXT NOT NULL,
                rule_set_artifact_hash TEXT NOT NULL
            )
            """
        )
        root = connection.execute(
            "SELECT authority_id FROM harness_authority WHERE singleton = 1"
        ).fetchone()
        if root is None or root["authority_id"] != authority_id:
            raise PermissionError("modeled-PIT Harness authority changed during materialization")
        existing = connection.execute(
            """
            SELECT checkpoint_id, checkpoint_hash, artifact_hash, harness_authority_id,
                   registration_id, registration_artifact_hash, snapshot_set_id,
                   snapshot_set_artifact_hash, admission_id, assessment_id,
                   rule_set_id, rule_set_artifact_hash
            FROM modeled_pit_readiness_authority
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        if existing is not None:
            if tuple(existing) != values:
                raise ValueError("modeled-PIT authority identity conflicts with durable content")
            return
        connection.execute(
            """
            INSERT INTO modeled_pit_readiness_authority(
                checkpoint_id, checkpoint_hash, artifact_hash, harness_authority_id,
                registration_id, registration_artifact_hash, snapshot_set_id,
                snapshot_set_artifact_hash, admission_id, assessment_id,
                rule_set_id, rule_set_artifact_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )


def _reopen_pipeline_modeled_pit_readiness(  # pyright: ignore[reportUnusedFunction]
    *,
    store: LocalDataSnapshotStore,
    checkpoint_id: str,
    expected_checkpoint: dict[str, object],
    registration_artifact_hash: str,
    snapshot_set_artifact_hash: str,
    registration_id: str,
    snapshot_set_id: str,
    admission_id: str,
    assessment_id: str,
    rule_set_id: str,
    rule_set_payload: dict[str, object],
) -> dict[str, object]:
    """Reopen one pipeline record against the current concrete authority root."""

    authority_id = store.harness_authority_id
    try:
        with store.authority_transaction() as connection:
            row = connection.execute(
                """
                SELECT checkpoint_hash, artifact_hash, harness_authority_id,
                       registration_id, registration_artifact_hash, snapshot_set_id,
                       snapshot_set_artifact_hash, admission_id, assessment_id,
                       rule_set_id, rule_set_artifact_hash
                FROM modeled_pit_readiness_authority
                WHERE checkpoint_id = ? AND harness_authority_id = ?
                """,
                (checkpoint_id, authority_id),
            ).fetchone()
    except sqlite3.OperationalError as exc:
        raise PermissionError("modeled-PIT checkpoint has no durable Harness authority") from exc
    if row is None:
        raise PermissionError("modeled-PIT checkpoint has no durable Harness authority")
    expected_identity = (
        checkpoint_id.removeprefix("modeled-pit-readiness-checkpoint-"),
        authority_id,
        registration_id,
        registration_artifact_hash,
        snapshot_set_id,
        snapshot_set_artifact_hash,
        admission_id,
        assessment_id,
        rule_set_id,
    )
    actual_identity = (
        row["checkpoint_hash"],
        row["harness_authority_id"],
        row["registration_id"],
        row["registration_artifact_hash"],
        row["snapshot_set_id"],
        row["snapshot_set_artifact_hash"],
        row["admission_id"],
        row["assessment_id"],
        row["rule_set_id"],
    )
    if actual_identity != expected_identity:
        raise PermissionError("modeled-PIT checkpoint authority differs from current sources")
    rule_set_artifact_hash = cast(str, row["rule_set_artifact_hash"])
    if (
        canonical_hash(rule_set_payload) != rule_set_artifact_hash
        or store.artifacts.read_json(rule_set_artifact_hash) != rule_set_payload
    ):
        raise PermissionError("modeled-PIT rule-set authority differs from current sources")
    artifact_hash = cast(str, row["artifact_hash"])
    reopened = parse_untrusted_modeled_pit_readiness_checkpoint(
        store.artifacts.read_json(artifact_hash)
    )
    if canonical_hash(reopened) != artifact_hash or reopened != expected_checkpoint:
        raise PermissionError("modeled-PIT artifact differs from authoritative reconstruction")
    return reopened


def _trade_date(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("raw price trade_date is missing")
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, pattern).date()
        except ValueError:
            continue
        return parsed.strftime("%Y%m%d")
    raise ValueError("raw price trade_date is invalid")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class _SchemaValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


@lru_cache(maxsize=1)
def _validator() -> _SchemaValidator:
    path = Path(__file__).parents[2] / "schemas" / "modeled-pit-readiness-checkpoint.schema.json"
    return cast(
        _SchemaValidator,
        Draft202012Validator(
            json.loads(path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        ),
    )


def _schema_errors(value: object) -> tuple[str, ...]:
    return tuple(sorted(error.message for error in _validator().iter_errors(value)))
