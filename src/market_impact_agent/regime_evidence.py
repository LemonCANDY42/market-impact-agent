from __future__ import annotations

import json
import os
import stat
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    MarketRegimeDataset,
    RegimePanel,
    RegimeSeries,
    ValidatedRegimePanel,
)
from market_impact_agent.regime_study import (
    RegimeCheckpointProtocol,
    RegimeSourceRequirement,
    RegimeStudyCase,
    RegimeStudyRegistration,
    RegimeStudySource,
)

REGIME_EVIDENCE_MANIFEST_SCHEMA = "market-impact.regime-evidence-manifest.v1"
REGIME_EVIDENCE_QUALIFICATION_SCHEMA = "market-impact.regime-evidence-qualification-report.v1"
_PRIVATE_EVIDENCE_ROOT = Path(".market-impact") / "regime" / "evidence"


class _JsonSchemaValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


class RegimeEvidenceAuthorityKind(StrEnum):
    ACTUAL_RECEIPT = "actual_receipt"
    PROVIDER_VERSION = "provider_version"
    VERIFIED_ARCHIVE = "verified_archive"


class RegimeEvidenceAvailabilityBasis(StrEnum):
    ACTUAL_RECEIPT = "actual_receipt"
    SOURCE_REPORTED = "source_reported"
    MODELED_LATENCY = "modeled_latency"


@dataclass(frozen=True, slots=True)
class RegimeCheckpoint:
    case_key: str
    session_date: date
    cutoff_at: datetime


@dataclass(frozen=True, slots=True)
class RegimeEvidenceRecord:
    record_id: str
    case_keys: tuple[str, ...]
    category: str
    source_id: str
    provider_id: str
    publisher_id: str
    source_ref: str
    claim_id: str
    lineage_id: str
    title: str
    occurred_at: datetime | None
    published_at: datetime
    source_updated_at: datetime | None
    available_at: datetime
    availability_basis: RegimeEvidenceAvailabilityBasis
    latency_model_id: str | None
    latency_model_hash: str | None
    authority_kind: RegimeEvidenceAuthorityKind
    authority_id: str
    authority_at: datetime
    authority_hash: str
    content_hash: str
    supersedes_id: str | None
    license_scope: str

    def __post_init__(self) -> None:
        _unique_nonempty(self.case_keys, "case_keys")
        for name in (
            "category",
            "source_id",
            "provider_id",
            "publisher_id",
            "source_ref",
            "claim_id",
            "lineage_id",
            "title",
            "authority_id",
            "license_scope",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        for name in ("authority_hash", "content_hash"):
            _sha256(cast(str, getattr(self, name)), name)
        for name in ("occurred_at", "source_updated_at"):
            value = cast(datetime | None, getattr(self, name))
            if value is not None:
                _aware(value, name)
        for name in ("published_at", "available_at", "authority_at"):
            _aware(cast(datetime, getattr(self, name)), name)
        if self.published_at > self.available_at:
            raise ValueError("regime evidence published_at must not follow available_at")
        if self.source_updated_at is not None and self.source_updated_at > self.available_at:
            raise ValueError("regime evidence source_updated_at must not follow available_at")
        if self.available_at > self.authority_at:
            raise ValueError("regime evidence authority cannot predate availability")
        if (
            self.availability_basis is RegimeEvidenceAvailabilityBasis.ACTUAL_RECEIPT
            and self.available_at != self.authority_at
        ):
            raise ValueError("actual-receipt authority must equal available_at")
        if (self.availability_basis is RegimeEvidenceAvailabilityBasis.ACTUAL_RECEIPT) != (
            self.authority_kind is RegimeEvidenceAuthorityKind.ACTUAL_RECEIPT
        ):
            raise ValueError("actual-receipt availability and authority must be paired")
        if self.availability_basis is RegimeEvidenceAvailabilityBasis.MODELED_LATENCY:
            if self.latency_model_id is None or self.latency_model_hash is None:
                raise ValueError("modeled availability requires a frozen latency model")
            _nonempty(self.latency_model_id, "latency_model_id")
            _sha256(self.latency_model_hash, "latency_model_hash")
        elif self.latency_model_id is not None or self.latency_model_hash is not None:
            raise ValueError("latency model identity is only valid for modeled availability")
        if self.supersedes_id == self.record_id:
            raise ValueError("regime evidence cannot supersede itself")
        if self.record_id != self.expected_record_id:
            raise ValueError("regime evidence record_id does not match content")

    @property
    def expected_record_id(self) -> str:
        return f"regime-evidence-record-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "case_keys": list(self.case_keys),
            "category": self.category,
            "source_id": self.source_id,
            "provider_id": self.provider_id,
            "publisher_id": self.publisher_id,
            "source_ref": self.source_ref,
            "claim_id": self.claim_id,
            "lineage_id": self.lineage_id,
            "title": self.title,
            "occurred_at": _optional_timestamp(self.occurred_at),
            "published_at": _timestamp(self.published_at),
            "source_updated_at": _optional_timestamp(self.source_updated_at),
            "available_at": _timestamp(self.available_at),
            "availability_basis": self.availability_basis.value,
            "latency_model_id": self.latency_model_id,
            "latency_model_hash": self.latency_model_hash,
            "authority_kind": self.authority_kind.value,
            "authority_id": self.authority_id,
            "authority_at": _timestamp(self.authority_at),
            "authority_hash": self.authority_hash,
            "content_hash": self.content_hash,
            "supersedes_id": self.supersedes_id,
            "license_scope": self.license_scope,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "record_id": self.record_id}

    @classmethod
    def build(
        cls,
        *,
        case_keys: tuple[str, ...],
        category: str,
        source_id: str,
        provider_id: str,
        publisher_id: str,
        source_ref: str,
        claim_id: str,
        lineage_id: str,
        title: str,
        occurred_at: datetime | None,
        published_at: datetime,
        source_updated_at: datetime | None,
        available_at: datetime,
        availability_basis: RegimeEvidenceAvailabilityBasis,
        latency_model_id: str | None,
        latency_model_hash: str | None,
        authority_kind: RegimeEvidenceAuthorityKind,
        authority_id: str,
        authority_at: datetime,
        authority_hash: str,
        content_hash: str,
        supersedes_id: str | None,
        license_scope: str,
    ) -> RegimeEvidenceRecord:
        core = {
            "case_keys": list(case_keys),
            "category": category,
            "source_id": source_id,
            "provider_id": provider_id,
            "publisher_id": publisher_id,
            "source_ref": source_ref,
            "claim_id": claim_id,
            "lineage_id": lineage_id,
            "title": title,
            "occurred_at": _optional_timestamp(occurred_at),
            "published_at": _timestamp(published_at),
            "source_updated_at": _optional_timestamp(source_updated_at),
            "available_at": _timestamp(available_at),
            "availability_basis": availability_basis.value,
            "latency_model_id": latency_model_id,
            "latency_model_hash": latency_model_hash,
            "authority_kind": authority_kind.value,
            "authority_id": authority_id,
            "authority_at": _timestamp(authority_at),
            "authority_hash": authority_hash,
            "content_hash": content_hash,
            "supersedes_id": supersedes_id,
            "license_scope": license_scope,
        }
        return cls(
            record_id=f"regime-evidence-record-{canonical_hash(core)}",
            case_keys=case_keys,
            category=category,
            source_id=source_id,
            provider_id=provider_id,
            publisher_id=publisher_id,
            source_ref=source_ref,
            claim_id=claim_id,
            lineage_id=lineage_id,
            title=title,
            occurred_at=occurred_at,
            published_at=published_at,
            source_updated_at=source_updated_at,
            available_at=available_at,
            availability_basis=availability_basis,
            latency_model_id=latency_model_id,
            latency_model_hash=latency_model_hash,
            authority_kind=authority_kind,
            authority_id=authority_id,
            authority_at=authority_at,
            authority_hash=authority_hash,
            content_hash=content_hash,
            supersedes_id=supersedes_id,
            license_scope=license_scope,
        )


@dataclass(frozen=True, slots=True)
class RegimeEvidenceManifest:
    manifest_id: str
    dataset_id: str
    dataset_hash: str
    registration_id: str
    registration_hash: str
    panel_id: str
    panel_hash: str
    outcomes_opened: bool
    records: tuple[RegimeEvidenceRecord, ...]

    def __post_init__(self) -> None:
        for name in ("dataset_id", "registration_id", "panel_id"):
            _nonempty(cast(str, getattr(self, name)), name)
        for name in ("dataset_hash", "registration_hash", "panel_hash"):
            _sha256(cast(str, getattr(self, name)), name)
        record_ids = tuple(item.record_id for item in self.records)
        if len(record_ids) != len(set(record_ids)):
            raise ValueError("regime evidence record_id values must be unique")
        _validate_revision_lineage(self.records)
        if self.manifest_id != self.expected_manifest_id:
            raise ValueError("regime evidence manifest_id does not match content")

    @property
    def expected_manifest_id(self) -> str:
        return f"regime-evidence-manifest-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": REGIME_EVIDENCE_MANIFEST_SCHEMA,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "panel_id": self.panel_id,
            "panel_hash": self.panel_hash,
            "outcomes_opened": self.outcomes_opened,
            "records": [item.to_dict() for item in self.records],
            "licensed_payloads_committed": False,
            "execution_capability": "none",
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "manifest_id": self.manifest_id}

    @classmethod
    def build(
        cls,
        *,
        dataset_id: str,
        dataset_hash: str,
        registration_id: str,
        registration_hash: str,
        panel_id: str,
        panel_hash: str,
        outcomes_opened: bool,
        records: tuple[RegimeEvidenceRecord, ...],
    ) -> RegimeEvidenceManifest:
        core = {
            "schema_version": REGIME_EVIDENCE_MANIFEST_SCHEMA,
            "dataset_id": dataset_id,
            "dataset_hash": dataset_hash,
            "registration_id": registration_id,
            "registration_hash": registration_hash,
            "panel_id": panel_id,
            "panel_hash": panel_hash,
            "outcomes_opened": outcomes_opened,
            "records": [item.to_dict() for item in records],
            "licensed_payloads_committed": False,
            "execution_capability": "none",
        }
        return cls(
            manifest_id=f"regime-evidence-manifest-{canonical_hash(core)}",
            dataset_id=dataset_id,
            dataset_hash=dataset_hash,
            registration_id=registration_id,
            registration_hash=registration_hash,
            panel_id=panel_id,
            panel_hash=panel_hash,
            outcomes_opened=outcomes_opened,
            records=records,
        )


def load_regime_evidence_manifest(
    path: Path,
    *,
    dataset: MarketRegimeDataset,
    validated_panel: ValidatedRegimePanel,
    registration: RegimeStudyRegistration,
) -> RegimeEvidenceManifest:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "regime evidence manifest")
    _validate_json_schema(payload, "regime-evidence-manifest.schema.json")
    if _string(payload, "schema_version") != REGIME_EVIDENCE_MANIFEST_SCHEMA:
        raise ValueError("unsupported regime evidence manifest schema_version")
    if payload.get("licensed_payloads_committed") is not False:
        raise ValueError("regime evidence manifests cannot commit licensed payloads")
    if payload.get("execution_capability") != "none":
        raise ValueError("regime evidence manifests cannot grant execution capability")
    manifest = RegimeEvidenceManifest(
        manifest_id=_string(payload, "manifest_id"),
        dataset_id=_string(payload, "dataset_id"),
        dataset_hash=_string(payload, "dataset_hash"),
        registration_id=_string(payload, "registration_id"),
        registration_hash=_string(payload, "registration_hash"),
        panel_id=_string(payload, "panel_id"),
        panel_hash=_string(payload, "panel_hash"),
        outcomes_opened=_boolean(payload, "outcomes_opened"),
        records=tuple(_record_from_dict(item) for item in _object_list(payload, "records")),
    )
    if manifest.to_dict() != payload:
        raise ValueError("regime evidence manifest does not match canonical contract")
    _validate_bindings(dataset, validated_panel, registration, manifest)
    return manifest


def load_regime_evidence_record(path: Path) -> RegimeEvidenceRecord:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "regime evidence record")
    record = _record_from_dict(payload)
    if record.to_dict() != payload:
        raise ValueError("regime evidence record does not match canonical contract")
    return record


def write_regime_evidence_record(
    record: RegimeEvidenceRecord,
    *,
    root: Path = _PRIVATE_EVIDENCE_ROOT / "records",
) -> Path:
    destination = root / f"{record.record_id}.json"
    _write_private_json(destination, record.to_dict())
    return destination


def write_regime_evidence_manifest(
    manifest: RegimeEvidenceManifest,
    *,
    root: Path = _PRIVATE_EVIDENCE_ROOT / "manifests",
) -> Path:
    payload = manifest.to_dict()
    _validate_json_schema(payload, "regime-evidence-manifest.schema.json")
    destination = root / f"{manifest.manifest_id}.json"
    _write_private_json(destination, payload)
    return destination


def write_regime_evidence_qualification_report(
    report: dict[str, object],
    *,
    root: Path = _PRIVATE_EVIDENCE_ROOT / "qualifications",
) -> Path:
    _validate_json_schema(report, "regime-evidence-qualification-report.schema.json")
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected_id = f"regime-evidence-qualification-report-{canonical_hash(core)}"
    if report_id != expected_id:
        raise ValueError("regime evidence qualification report_id does not match content")
    destination = root / f"{report_id}.json"
    _write_private_json(destination, report)
    return destination


def _record_from_dict(payload: dict[str, object]) -> RegimeEvidenceRecord:
    return RegimeEvidenceRecord(
        record_id=_string(payload, "record_id"),
        case_keys=_string_tuple(payload, "case_keys"),
        category=_string(payload, "category"),
        source_id=_string(payload, "source_id"),
        provider_id=_string(payload, "provider_id"),
        publisher_id=_string(payload, "publisher_id"),
        source_ref=_string(payload, "source_ref"),
        claim_id=_string(payload, "claim_id"),
        lineage_id=_string(payload, "lineage_id"),
        title=_string(payload, "title"),
        occurred_at=_nullable_datetime(payload, "occurred_at"),
        published_at=_datetime(payload, "published_at"),
        source_updated_at=_nullable_datetime(payload, "source_updated_at"),
        available_at=_datetime(payload, "available_at"),
        availability_basis=RegimeEvidenceAvailabilityBasis(_string(payload, "availability_basis")),
        latency_model_id=_nullable_string(payload, "latency_model_id"),
        latency_model_hash=_nullable_string(payload, "latency_model_hash"),
        authority_kind=RegimeEvidenceAuthorityKind(_string(payload, "authority_kind")),
        authority_id=_string(payload, "authority_id"),
        authority_at=_datetime(payload, "authority_at"),
        authority_hash=_string(payload, "authority_hash"),
        content_hash=_string(payload, "content_hash"),
        supersedes_id=_nullable_string(payload, "supersedes_id"),
        license_scope=_string(payload, "license_scope"),
    )


def generate_regime_checkpoints(
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    *,
    protocol: RegimeCheckpointProtocol,
    trading_dates: tuple[date, ...],
) -> tuple[RegimeCheckpoint, ...]:
    if market_case.case_key != study_case.case_key:
        raise ValueError("market and study cases must have the same case_key")
    eligible_dates = tuple(
        item
        for item in sorted(set(trading_dates))
        if market_case.tradable_start <= item <= market_case.end
    )
    if not eligible_dates:
        return ()
    selected: list[date] = []
    seen_buckets: set[tuple[int, int]] = set()
    for item in eligible_dates:
        if study_case.decision_schedule == "monthly":
            bucket = (item.year, item.month)
        elif study_case.decision_schedule in {"weekly", "event_then_weekly"}:
            iso_year, iso_week, _weekday = item.isocalendar()
            bucket = (iso_year, iso_week)
        else:
            raise ValueError("unsupported regime study decision_schedule")
        if bucket not in seen_buckets:
            seen_buckets.add(bucket)
            selected.append(item)
    local_time = _local_time(protocol.decision_time_local)
    zone = ZoneInfo(protocol.timezone)
    return tuple(
        RegimeCheckpoint(
            case_key=market_case.case_key,
            session_date=item,
            cutoff_at=datetime.combine(item, local_time, tzinfo=zone).astimezone(UTC),
        )
        for item in selected
    )


def qualify_regime_evidence(
    dataset: MarketRegimeDataset,
    validated_panel: ValidatedRegimePanel,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
) -> dict[str, object]:
    panel = validated_panel.panel
    _validate_bindings(dataset, validated_panel, registration, manifest)
    source_by_id = {item.source_id: item for item in registration.source_catalog}
    study_by_case = {item.case_key: item for item in registration.cases}
    series_by_id = _series_by_id(panel)
    results: list[dict[str, object]] = []
    all_ready = True
    for market_case in dataset.cases:
        study_case = study_by_case[market_case.case_key]
        primary = series_by_id.get(market_case.primary_market_index)
        if primary is None:
            checkpoints: tuple[RegimeCheckpoint, ...] = ()
        else:
            checkpoints = generate_regime_checkpoints(
                market_case,
                study_case,
                protocol=registration.checkpoint_protocol,
                trading_dates=tuple(_row_date(row) for row in primary.rows),
            )
        checkpoint_results: list[dict[str, object]] = []
        for checkpoint in checkpoints:
            requirement_results = [
                _qualify_requirement(
                    requirement,
                    checkpoint=checkpoint,
                    market_case=market_case,
                    study_case=study_case,
                    protocol=registration.checkpoint_protocol,
                    panel=panel,
                    series_by_id=series_by_id,
                    source_by_id=source_by_id,
                    records=manifest.records,
                )
                for requirement in study_case.source_requirements
            ]
            event_revelation = _qualify_event_revelation(
                checkpoint=checkpoint,
                market_case=market_case,
                study_case=study_case,
                source_by_id=source_by_id,
                records=manifest.records,
            )
            ready = all(item["ready"] is True for item in requirement_results) and bool(
                event_revelation["ready"]
            )
            all_ready = all_ready and ready
            checkpoint_results.append(
                {
                    "session_date": checkpoint.session_date.isoformat(),
                    "cutoff_at": _timestamp(checkpoint.cutoff_at),
                    "ready": ready,
                    "event_revelation": event_revelation,
                    "requirements": requirement_results,
                }
            )
        if not checkpoints:
            all_ready = False
        results.append(
            {
                "case_key": market_case.case_key,
                "checkpoint_count": len(checkpoints),
                "all_checkpoints_ready": bool(checkpoints)
                and all(item["ready"] is True for item in checkpoint_results),
                "checkpoints": checkpoint_results,
            }
        )
    core: dict[str, object] = {
        "schema_version": REGIME_EVIDENCE_QUALIFICATION_SCHEMA,
        "dataset_id": dataset.dataset_id,
        "registration_id": registration.registration_id,
        "panel_id": validated_panel.panel_id,
        "manifest_id": manifest.manifest_id,
        "outcomes_opened": registration.outcomes_opened,
        "case_count": len(results),
        "all_source_requirements_ready": all_ready,
        "diagnostic_agent_run_eligible": all_ready,
        "agent_effectiveness_claim_eligible": all_ready and not registration.outcomes_opened,
        "cases": results,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-evidence-qualification-report-{canonical_hash(core)}",
    }


def _qualify_event_revelation(
    *,
    checkpoint: RegimeCheckpoint,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    source_by_id: dict[str, RegimeStudySource],
    records: tuple[RegimeEvidenceRecord, ...],
) -> dict[str, object]:
    anchor = market_case.event_anchor
    required = bool(
        anchor is not None
        and study_case.decision_schedule == "event_then_weekly"
        and checkpoint.session_date == market_case.tradable_start
    )
    if not required:
        return {"required": False, "ready": True, "record_ids": [], "blockers": []}
    if anchor is None:  # pragma: no cover - narrowed by required
        raise AssertionError("required event revelation must have an event anchor")
    registered: dict[str, str] = {}
    for requirement in study_case.source_requirements:
        if requirement.category not in {"official_context", "established_news"}:
            continue
        for source_id in requirement.source_ids:
            registered[source_id] = source_by_id[source_id].provider_id
    candidates = tuple(
        record
        for record in records
        if market_case.case_key in record.case_keys
        and record.category in {"official_context", "established_news"}
        and registered.get(record.source_id) == record.provider_id
        and (record.occurred_at or record.published_at) >= anchor.observed_at
        and record.published_at < checkpoint.cutoff_at
        and record.available_at <= checkpoint.cutoff_at
        and (record.source_updated_at is None or record.source_updated_at <= checkpoint.cutoff_at)
        and _has_integrity_authority(record)
    )
    latest = _latest_versions(candidates)
    record_ids = sorted(record.record_id for record in latest)
    ready = bool(record_ids)
    return {
        "required": True,
        "ready": ready,
        "record_ids": record_ids,
        "blockers": [] if ready else ["missing_event_revelation"],
    }


def _qualify_requirement(
    requirement: RegimeSourceRequirement,
    *,
    checkpoint: RegimeCheckpoint,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    protocol: RegimeCheckpointProtocol,
    panel: RegimePanel,
    series_by_id: dict[str, RegimeSeries],
    source_by_id: dict[str, RegimeStudySource],
    records: tuple[RegimeEvidenceRecord, ...],
) -> dict[str, object]:
    if requirement.category == "market_price":
        series = series_by_id.get(market_case.primary_market_index)
        count = (
            0
            if series is None
            else sum(_row_date(row) < checkpoint.session_date for row in series.rows)
        )
        distinct = int(series is not None and count > 0)
        content_complete = (
            count >= requirement.minimum_records_per_checkpoint
            and distinct >= requirement.minimum_distinct_sources
        )
        authority_records = _price_authority_records(
            requirement,
            checkpoint=checkpoint,
            market_case=market_case,
            source_by_id=source_by_id,
            records=records,
            series=() if series is None else (series,),
        )
        authority = bool(series is not None and authority_records)
        return _requirement_result(
            requirement,
            count,
            distinct,
            content_complete,
            authority,
            authority_record_count=count if authority else 0,
        )
    if requirement.category == "industry_price":
        available = 0
        for proxy_id in market_case.required_industry_proxies:
            series = series_by_id.get(proxy_id)
            if series is not None and any(
                _row_date(row) < checkpoint.session_date for row in series.rows
            ):
                available += 1
        content_complete = (
            available >= requirement.minimum_records_per_checkpoint
            and int(available > 0) >= requirement.minimum_distinct_sources
        )
        authority_records = _price_authority_records(
            requirement,
            checkpoint=checkpoint,
            market_case=market_case,
            source_by_id=source_by_id,
            records=records,
            series=tuple(
                series_by_id[proxy_id]
                for proxy_id in market_case.required_industry_proxies
                if proxy_id in series_by_id
            ),
        )
        authority = available > 0 and len(authority_records) == available
        return _requirement_result(
            requirement,
            available,
            int(available > 0),
            content_complete,
            authority,
            authority_record_count=len(authority_records),
        )

    registered_ids = set(requirement.source_ids)
    candidates = tuple(
        item
        for item in records
        if market_case.case_key in item.case_keys
        and item.category == requirement.category
        and item.source_id in registered_ids
        and item.provider_id == source_by_id[item.source_id].provider_id
        and _inside_window(item, checkpoint, study_case, protocol)
    )
    latest = _latest_versions(candidates)
    content_count = len(latest)
    content_distinct = _distinct_sources(latest, requirement.category)
    verified = tuple(item for item in latest if _has_integrity_authority(item))
    verified_count = len(verified)
    verified_distinct = _distinct_sources(verified, requirement.category)
    content_complete = (
        content_count >= requirement.minimum_records_per_checkpoint
        and content_distinct >= requirement.minimum_distinct_sources
    )
    authority = (
        verified_count >= requirement.minimum_records_per_checkpoint
        and verified_distinct >= requirement.minimum_distinct_sources
    )
    return _requirement_result(
        requirement,
        content_count,
        content_distinct,
        content_complete,
        authority,
        authority_record_count=verified_count,
    )


def _price_authority_records(
    requirement: RegimeSourceRequirement,
    *,
    checkpoint: RegimeCheckpoint,
    market_case: MarketRegimeCase,
    source_by_id: dict[str, RegimeStudySource],
    records: tuple[RegimeEvidenceRecord, ...],
    series: tuple[RegimeSeries, ...],
) -> tuple[RegimeEvidenceRecord, ...]:
    from market_impact_agent.regime_market_evidence import (
        panel_authority_source_ref,
        panel_series_as_of_hash,
    )

    registered_ids = set(requirement.source_ids)
    matched: list[RegimeEvidenceRecord] = []
    for item in series:
        expected_ref = panel_authority_source_ref(
            source=item.source,
            tushare_code=item.tushare_code,
            case_key=market_case.case_key,
            checkpoint_date=checkpoint.session_date,
        )
        expected_hash = panel_series_as_of_hash(item, checkpoint.session_date)
        candidates = tuple(
            record
            for record in records
            if market_case.case_key in record.case_keys
            and record.category == requirement.category
            and record.source_id in registered_ids
            and record.provider_id == source_by_id[record.source_id].provider_id
            and record.source_ref == expected_ref
            and record.content_hash == expected_hash
            and record.available_at <= checkpoint.cutoff_at
            and _has_integrity_authority(record)
        )
        if len(candidates) == 1:
            matched.append(candidates[0])
    return tuple(matched)


def _has_integrity_authority(record: RegimeEvidenceRecord) -> bool:
    return record.authority_kind in {
        RegimeEvidenceAuthorityKind.ACTUAL_RECEIPT,
        RegimeEvidenceAuthorityKind.PROVIDER_VERSION,
        RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
    }


def _requirement_result(
    requirement: RegimeSourceRequirement,
    count: int,
    distinct: int,
    content_complete: bool,
    authority: bool,
    *,
    authority_record_count: int | None = None,
) -> dict[str, object]:
    ready = content_complete and (authority or not requirement.authenticated_availability_required)
    blockers: list[str] = []
    if count < requirement.minimum_records_per_checkpoint:
        blockers.append("insufficient_records")
    if distinct < requirement.minimum_distinct_sources:
        blockers.append("insufficient_distinct_sources")
    if requirement.authenticated_availability_required and not authority:
        blockers.append("no_point_in_time_authority")
    return {
        "category": requirement.category,
        "record_count": count,
        "authority_record_count": count
        if authority_record_count is None and authority
        else (0 if authority_record_count is None else authority_record_count),
        "distinct_source_count": distinct,
        "minimum_records": requirement.minimum_records_per_checkpoint,
        "minimum_distinct_sources": requirement.minimum_distinct_sources,
        "content_complete": content_complete,
        "point_in_time_authority": authority,
        "ready": ready,
        "blockers": blockers,
    }


def _inside_window(
    record: RegimeEvidenceRecord,
    checkpoint: RegimeCheckpoint,
    study_case: RegimeStudyCase,
    protocol: RegimeCheckpointProtocol,
) -> bool:
    if record.published_at >= checkpoint.cutoff_at or record.available_at > checkpoint.cutoff_at:
        return False
    if record.source_updated_at is not None and record.source_updated_at > checkpoint.cutoff_at:
        return False
    if record.category == "established_news":
        lookback = dict(protocol.news_lookback_calendar_days)[study_case.decision_schedule]
    else:
        lookback = dict(protocol.maximum_age_calendar_days).get(record.category)
    return lookback is None or record.available_at >= checkpoint.cutoff_at - timedelta(
        days=lookback
    )


def _latest_versions(
    records: tuple[RegimeEvidenceRecord, ...],
) -> tuple[RegimeEvidenceRecord, ...]:
    by_lineage: dict[str, RegimeEvidenceRecord] = {}
    for item in sorted(records, key=lambda value: (value.available_at, value.record_id)):
        by_lineage[item.lineage_id] = item
    return tuple(sorted(by_lineage.values(), key=lambda item: item.record_id))


def _distinct_sources(records: tuple[RegimeEvidenceRecord, ...], category: str) -> int:
    if category == "established_news":
        return len({item.publisher_id for item in records})
    return len({item.source_id for item in records})


def _series_by_id(panel: RegimePanel) -> dict[str, RegimeSeries]:
    result = {item.series_id: item for item in panel.series}
    by_code = {item.tushare_code: item for item in panel.series}
    for proxy_id, code in panel.proxy_resolution:
        if code in by_code:
            result[proxy_id] = by_code[code]
    return result


def _row_date(row: dict[str, object]) -> date:
    value = row.get("trade_date")
    if not isinstance(value, str):
        raise TypeError("regime price row trade_date must be a string")
    return date.fromisoformat(value) if "-" in value else datetime.strptime(value, "%Y%m%d").date()


def _validate_bindings(
    dataset: MarketRegimeDataset,
    panel: ValidatedRegimePanel,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
) -> None:
    expected = (
        manifest.dataset_id == dataset.dataset_id,
        manifest.dataset_hash == dataset.dataset_hash,
        manifest.registration_id == registration.registration_id,
        manifest.registration_hash == registration.registration_hash,
        manifest.panel_id == panel.panel_id,
        manifest.panel_hash == panel.panel_hash,
        manifest.outcomes_opened == registration.outcomes_opened,
    )
    if not all(expected):
        raise ValueError("regime evidence manifest bindings do not match the study inputs")
    case_keys = {item.case_key for item in dataset.cases}
    source_by_id = {item.source_id: item for item in registration.source_catalog}
    for record in manifest.records:
        if not set(record.case_keys) <= case_keys:
            raise ValueError("regime evidence record references an unknown case")
        source = source_by_id.get(record.source_id)
        if source is None:
            raise ValueError("regime evidence record references an unregistered source")
        if source.category != record.category or source.provider_id != record.provider_id:
            raise ValueError("regime evidence record does not match its registered source")


def _validate_revision_lineage(records: tuple[RegimeEvidenceRecord, ...]) -> None:
    by_id = {item.record_id: item for item in records}
    successors: defaultdict[str, int] = defaultdict(int)
    for item in records:
        if item.supersedes_id is None:
            continue
        previous = by_id.get(item.supersedes_id)
        if previous is None:
            raise ValueError("regime evidence revision supersedes an unknown record")
        if item.lineage_id != previous.lineage_id or item.claim_id != previous.claim_id:
            raise ValueError("regime evidence revisions must retain lineage and claim identity")
        if item.available_at <= previous.available_at:
            raise ValueError("regime evidence revisions must advance availability")
        successors[previous.record_id] += 1
        if successors[previous.record_id] > 1:
            raise ValueError("regime evidence revisions cannot fork")


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    if path.is_symlink():
        raise ValueError("regime evidence destination must not be a symlink")
    encoded = (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        path.chmod(0o600)
    except Exception:
        with suppress(OSError):
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise ValueError("private regime evidence artifact must use mode 0600")


def _local_time(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("decision_time_local must use HH:MM:SS") from exc


def _timestamp(value: datetime) -> str:
    _aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if not values or len(values) != len(set(values)):
        raise ValueError(f"{name} must contain unique values")
    for item in values:
        _nonempty(item, name)


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 hex digest")


def _validate_json_schema(payload: dict[str, object], schema_name: str) -> None:
    package_root = Path(__file__).resolve().parent
    installed = package_root / "schemas" / schema_name
    path = installed if installed.is_file() else package_root.parents[1] / "schemas" / schema_name
    schema = json.loads(path.read_text(encoding="utf-8"))
    validator = cast(
        _JsonSchemaValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
    errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.path))
    if errors:
        raise ValueError(
            f"{schema_name} validation failed: " + "; ".join(error.message for error in errors)
        )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], value)


def _object_list(payload: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, name) for item in cast(list[object], value))


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _nullable_string(payload: dict[str, object], name: str) -> str | None:
    return None if payload.get(name) is None else _string(payload, name)


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _string_tuple(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], value))


def _datetime(payload: dict[str, object], name: str) -> datetime:
    value = _string(payload, name)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _aware(parsed, name)
    return parsed


def _nullable_datetime(payload: dict[str, object], name: str) -> datetime | None:
    return None if payload.get(name) is None else _datetime(payload, name)
