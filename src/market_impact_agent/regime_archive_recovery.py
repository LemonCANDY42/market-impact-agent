from __future__ import annotations

import gzip
import io
import json
import os
import stat
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import urlparse

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.internet_archive import (
    InternetArchiveAdapter,
    InternetArchiveIndexAdapter,
    InternetArchiveLocator,
    VerifiedInternetArchiveRecord,
)
from market_impact_agent.publisher_evidence import extract_publisher_news_snapshot
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceManifest,
    RegimeEvidenceRecord,
    has_point_in_time_authority,
)
from market_impact_agent.regime_study import RegimeStudyRegistration

REGIME_PUBLISHER_ARCHIVE_RECOVERY_SCHEMA = (
    "market-impact.regime-publisher-archive-recovery-report.v1"
)
_PRIVATE_RECOVERY_ROOT = Path(".market-impact") / "regime" / "archive-recovery"
_MAX_PUBLISHER_HTML_BYTES = 20 * 1024 * 1024


class PublisherArchiveIndex(Protocol):
    def locate_latest(
        self,
        *,
        target_url: str,
        not_after: datetime,
    ) -> InternetArchiveLocator | None: ...


ArchiveFetch = Callable[[InternetArchiveLocator], VerifiedInternetArchiveRecord]


@dataclass(frozen=True, slots=True)
class RecoveredPublisherArchiveSnapshot:
    record: RegimeEvidenceRecord
    research_document: dict[str, object]


def audit_publisher_archive_recovery(
    manifest: RegimeEvidenceManifest,
    registration: RegimeStudyRegistration,
    qualification_report: dict[str, object],
    *,
    case_keys: tuple[str, ...] | None = None,
    index: PublisherArchiveIndex | None = None,
    max_lookups: int = 500,
) -> dict[str, object]:
    """Locate exact pre-cutoff publisher captures without promoting unverified content."""
    _validate_inputs(manifest, registration, qualification_report)
    if max_lookups < 1:
        raise ValueError("publisher archive max_lookups must be positive")
    case_by_key = {item.case_key: item for item in registration.cases}
    selected = tuple(sorted(case_by_key)) if case_keys is None else _unique_case_keys(case_keys)
    unknown = sorted(set(selected) - set(case_by_key))
    if unknown:
        raise ValueError(f"publisher archive audit references unknown cases: {unknown}")
    report_cases = {
        _required_string(item, "case_key"): item
        for item in _object_list(qualification_report, "cases")
    }
    locator = InternetArchiveIndexAdapter() if index is None else index
    source_by_id = {item.source_id: item for item in registration.source_catalog}
    checkpoints: list[dict[str, object]] = []
    lookup_count = 0
    found_count = 0
    not_found_count = 0
    source_error_count = 0
    ready_if_verified_count = 0
    for case_key in selected:
        study_case = case_by_key[case_key]
        report_case = report_cases.get(case_key)
        if report_case is None:
            raise ValueError("qualification report does not cover every selected case")
        news_requirement = next(
            (
                item
                for item in study_case.source_requirements
                if item.category == "established_news"
            ),
            None,
        )
        if news_requirement is None:
            raise ValueError("selected regime case lacks an established-news requirement")
        registered_providers = {
            source_id: source_by_id[source_id].provider_id
            for source_id in news_requirement.source_ids
        }
        lookback_days = dict(registration.checkpoint_protocol.news_lookback_calendar_days)[
            study_case.decision_schedule
        ]
        for raw_checkpoint in _object_list(report_case, "checkpoints"):
            cutoff = _timestamp(_required_string(raw_checkpoint, "cutoff_at"))
            window_start = cutoff - timedelta(days=lookback_days)
            candidates = _latest_lineages(
                tuple(
                    record
                    for record in manifest.records
                    if case_key in record.case_keys
                    and record.category == "established_news"
                    and registered_providers.get(record.source_id) == record.provider_id
                    and record.published_at < cutoff
                    and _initial_version_available_at(record) <= cutoff
                    and _initial_version_available_at(record) >= window_start
                )
            )
            current_authorized = tuple(
                record for record in candidates if has_point_in_time_authority(record, cutoff)
            )
            recovered_candidates: list[dict[str, object]] = []
            potential_records = list(current_authorized)
            for record in candidates:
                if record in current_authorized:
                    continue
                if lookup_count >= max_lookups:
                    raise ValueError("publisher archive audit exceeded max_lookups")
                lookup_count += 1
                status = "source_error"
                found_locator: InternetArchiveLocator | None = None
                error_type: str | None = None
                try:
                    found_locator = locator.locate_latest(
                        target_url=record.source_ref,
                        not_after=cutoff,
                    )
                except (OSError, RuntimeError, TypeError, ValueError) as exc:
                    error_type = type(exc).__name__
                    source_error_count += 1
                else:
                    if found_locator is None:
                        status = "not_found"
                        not_found_count += 1
                    else:
                        status = "capture_found_unverified"
                        found_count += 1
                        potential_records.append(record)
                recovered_candidates.append(
                    {
                        "record_id": record.record_id,
                        "publisher_id": record.publisher_id,
                        "source_ref": record.source_ref,
                        "status": status,
                        "locator": (None if found_locator is None else found_locator.to_dict()),
                        "error_type": error_type,
                    }
                )
            projected_count = len(potential_records)
            projected_publishers = len({record.publisher_id for record in potential_records})
            ready_if_verified = (
                projected_count >= news_requirement.minimum_records_per_checkpoint
                and projected_publishers >= news_requirement.minimum_distinct_sources
            )
            if ready_if_verified:
                ready_if_verified_count += 1
            checkpoints.append(
                {
                    "case_key": case_key,
                    "session_date": _required_string(raw_checkpoint, "session_date"),
                    "cutoff_at": _format_timestamp(cutoff),
                    "minimum_records": news_requirement.minimum_records_per_checkpoint,
                    "minimum_distinct_publishers": (news_requirement.minimum_distinct_sources),
                    "current_authority_record_count": len(current_authorized),
                    "current_distinct_publisher_count": len(
                        {record.publisher_id for record in current_authorized}
                    ),
                    "found_unverified_record_count": projected_count - len(current_authorized),
                    "news_ready_if_found_captures_verify": ready_if_verified,
                    "candidates": recovered_candidates,
                }
            )
    core: dict[str, object] = {
        "schema_version": REGIME_PUBLISHER_ARCHIVE_RECOVERY_SCHEMA,
        "manifest_id": manifest.manifest_id,
        "registration_id": registration.registration_id,
        "qualification_report_id": _required_string(qualification_report, "report_id"),
        "case_keys": list(selected),
        "checkpoint_count": len(checkpoints),
        "lookup_count": lookup_count,
        "found_count": found_count,
        "not_found_count": not_found_count,
        "source_error_count": source_error_count,
        "news_ready_if_found_captures_verify_count": ready_if_verified_count,
        "checkpoints": checkpoints,
        "candidate_only": True,
        "licensed_payloads_committed": False,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-publisher-archive-recovery-report-{canonical_hash(core)}",
    }


def recover_publisher_archive_snapshot(
    original: RegimeEvidenceRecord,
    locator: InternetArchiveLocator,
    *,
    not_after: datetime,
    supersedes: RegimeEvidenceRecord | None = None,
    fetch: ArchiveFetch | None = None,
) -> RecoveredPublisherArchiveSnapshot:
    """Verify one historical publisher version and materialize its private research view."""
    if original.category != "established_news":
        raise ValueError("publisher archive recovery requires established-news evidence")
    if original.provider_id != "publisher-https-snapshot":
        raise ValueError("publisher archive recovery requires the registered publisher adapter")
    _aware(not_after, "publisher archive recovery cutoff")
    if locator.captured_at > not_after:
        raise ValueError("publisher archive capture follows the recovery cutoff")
    if not _same_target(original.source_ref, locator.target_url):
        raise ValueError("publisher archive locator does not match the evidence source_ref")
    archive = (InternetArchiveAdapter().fetch if fetch is None else fetch)(locator)
    if archive.locator != locator or not archive.archive_capture_accepted:
        raise ValueError("publisher archive recovery requires the exact accepted locator")
    parsed_snapshot = extract_publisher_news_snapshot(
        url=original.source_ref,
        payload=_publisher_html_payload(archive.payload),
        retrieved_at=archive.captured_at,
        case_keys=original.case_keys,
        claim_id=original.claim_id,
        lineage_id=original.lineage_id,
    )
    parsed = parsed_snapshot.record
    if (
        parsed.source_id != original.source_id
        or parsed.provider_id != original.provider_id
        or parsed.publisher_id != original.publisher_id
        or parsed.published_at != original.published_at
    ):
        raise ValueError(
            "archived publisher version does not match source identity and publication"
        )
    if parsed.available_at > not_after:
        raise ValueError("archived publisher version was not available by the recovery cutoff")
    if parsed.source_updated_at is not None and parsed.source_updated_at > not_after:
        raise ValueError("archived publisher update follows the recovery cutoff")
    if supersedes is not None:
        same_revision_identity = (
            parsed.lineage_id == supersedes.lineage_id
            and parsed.claim_id == supersedes.claim_id
            and parsed.category == supersedes.category
            and parsed.source_id == supersedes.source_id
            and parsed.provider_id == supersedes.provider_id
            and parsed.publisher_id == supersedes.publisher_id
            and parsed.source_ref == supersedes.source_ref
        )
        if not same_revision_identity:
            raise ValueError("publisher archive revision changes source or lineage identity")
        if parsed.available_at <= supersedes.available_at:
            raise ValueError("publisher archive revision must advance availability")
    authority_hash = canonical_hash(
        {
            "archive_id": archive.archive_id,
            "archive_provider_id": archive.provider_id,
            "adapter_version": archive.adapter_version,
            "source_version_id": locator.source_version_id,
            "captured_at": _format_timestamp(archive.captured_at),
            "payload_sha256": archive.payload_sha256,
            "payload_digest": archive.payload_digest,
        }
    )
    recovered = RegimeEvidenceRecord.build(
        case_keys=parsed.case_keys,
        category=parsed.category,
        source_id=parsed.source_id,
        provider_id=parsed.provider_id,
        publisher_id=parsed.publisher_id,
        source_ref=parsed.source_ref,
        claim_id=parsed.claim_id,
        lineage_id=parsed.lineage_id,
        title=parsed.title,
        occurred_at=parsed.occurred_at,
        published_at=parsed.published_at,
        source_updated_at=parsed.source_updated_at,
        available_at=parsed.available_at,
        availability_basis=parsed.availability_basis,
        latency_model_id=parsed.latency_model_id,
        latency_model_hash=parsed.latency_model_hash,
        authority_kind=RegimeEvidenceAuthorityKind.VERIFIED_ARCHIVE,
        authority_id=locator.source_version_id,
        authority_at=archive.captured_at,
        authority_hash=authority_hash,
        content_hash=archive.payload_sha256,
        supersedes_id=None if supersedes is None else supersedes.record_id,
        license_scope=original.license_scope,
    )
    research_document = parsed_snapshot.to_research_document()
    research_document["content_hash"] = recovered.content_hash
    return RecoveredPublisherArchiveSnapshot(
        record=recovered,
        research_document=research_document,
    )


def recover_publisher_archive_record(
    original: RegimeEvidenceRecord,
    locator: InternetArchiveLocator,
    *,
    not_after: datetime,
    supersedes: RegimeEvidenceRecord | None = None,
    fetch: ArchiveFetch | None = None,
) -> RegimeEvidenceRecord:
    """Verify and materialize one historical publisher version as PIT evidence."""
    return recover_publisher_archive_snapshot(
        original,
        locator,
        not_after=not_after,
        supersedes=supersedes,
        fetch=fetch,
    ).record


def write_publisher_archive_research_document(
    snapshot: RecoveredPublisherArchiveSnapshot,
    *,
    root: Path = Path(".market-impact") / "regime" / "evidence" / "documents",
) -> Path:
    if snapshot.research_document.get("content_hash") != snapshot.record.content_hash:
        raise ValueError("publisher archive research document does not bind its record")
    destination = root / f"{snapshot.record.content_hash}.json"
    _write_private_json(destination, snapshot.research_document)
    return destination


def write_publisher_archive_recovery_report(
    report: dict[str, object],
    *,
    root: Path = _PRIVATE_RECOVERY_ROOT,
) -> Path:
    errors = validate_agent_contract(
        report,
        "regime-publisher-archive-recovery-report.schema.json",
    )
    if errors:
        raise ValueError("; ".join(errors))
    report_id = _required_string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected = f"regime-publisher-archive-recovery-report-{canonical_hash(core)}"
    if report_id != expected:
        raise ValueError("publisher archive recovery report_id does not match content")
    destination = root / f"{report_id}.json"
    _write_private_json(destination, report)
    return destination


def load_qualification_report(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("regime evidence qualification report must be an object")
    report = cast(dict[str, object], payload)
    report_id = _required_string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected = f"regime-evidence-qualification-report-{canonical_hash(core)}"
    if report_id != expected:
        raise ValueError("regime evidence qualification report_id does not match content")
    return report


def _validate_inputs(
    manifest: RegimeEvidenceManifest,
    registration: RegimeStudyRegistration,
    report: dict[str, object],
) -> None:
    report_id = _required_string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    if report_id != f"regime-evidence-qualification-report-{canonical_hash(core)}":
        raise ValueError("qualification report_id does not match content")
    if (
        manifest.registration_id != registration.registration_id
        or report.get("registration_id") != registration.registration_id
        or report.get("manifest_id") != manifest.manifest_id
        or report.get("dataset_id") != manifest.dataset_id
        or report.get("panel_id") != manifest.panel_id
    ):
        raise ValueError("publisher archive recovery inputs do not share one frozen study")


def _latest_lineages(
    records: tuple[RegimeEvidenceRecord, ...],
) -> tuple[RegimeEvidenceRecord, ...]:
    by_lineage: dict[str, RegimeEvidenceRecord] = {}
    for record in sorted(records, key=lambda item: (item.available_at, item.record_id)):
        by_lineage[record.lineage_id] = record
    return tuple(sorted(by_lineage.values(), key=lambda item: item.record_id))


def _initial_version_available_at(record: RegimeEvidenceRecord) -> datetime:
    """Infer the original-version latency without admitting the current revision."""
    current_version_time = record.source_updated_at or record.published_at
    latency = record.available_at - current_version_time
    if latency < timedelta(0):  # pragma: no cover - rejected by RegimeEvidenceRecord
        raise ValueError("publisher evidence latency cannot be negative")
    return record.published_at + latency


def _same_target(first: str, second: str) -> bool:
    first_url = urlparse(first)
    second_url = urlparse(second)
    if first_url.scheme not in {"http", "https"} or second_url.scheme not in {"http", "https"}:
        return False
    try:
        first_port = first_url.port
        second_port = second_url.port
    except ValueError:
        return False
    if (
        (first_port is not None and first_port != (80 if first_url.scheme == "http" else 443))
        or (second_port is not None and second_port != (80 if second_url.scheme == "http" else 443))
        or first_url.username is not None
        or first_url.password is not None
        or second_url.username is not None
        or second_url.password is not None
    ):
        return False
    # Wayback may canonicalize an HTTPS query to the publisher's historical HTTP
    # URL, including an explicit default port. Only those scheme/port substitutions
    # are tolerated; the HTTP request target is otherwise exact. Fragments are
    # excluded because they are never sent upstream.
    return (
        first_url.hostname,
        first_url.path or "/",
        first_url.params,
        first_url.query,
    ) == (
        second_url.hostname,
        second_url.path or "/",
        second_url.params,
        second_url.query,
    )


def _publisher_html_payload(payload: bytes) -> bytes:
    """Decode a gzip HTTP entity only after the archive digest has been verified."""
    if not payload.startswith(b"\x1f\x8b"):
        return payload
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as compressed:
            decoded = compressed.read(_MAX_PUBLISHER_HTML_BYTES + 1)
    except (EOFError, OSError) as exc:
        raise ValueError("publisher archive gzip payload is invalid") from exc
    if not decoded:
        raise ValueError("publisher archive gzip payload must not be empty")
    if len(decoded) > _MAX_PUBLISHER_HTML_BYTES:
        raise ValueError("publisher archive gzip payload exceeds the bounded contract")
    return decoded


def _unique_case_keys(values: tuple[str, ...]) -> tuple[str, ...]:
    if not values or len(values) != len(set(values)):
        raise ValueError("publisher archive case_keys must contain unique values")
    if any(not value or value != value.strip() for value in values):
        raise ValueError("publisher archive case_keys must be non-empty trimmed strings")
    return tuple(values)


def _object_list(payload: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    result: list[dict[str, object]] = []
    for item in cast(list[object], value):
        if not isinstance(item, dict):
            raise TypeError(f"{name} items must be objects")
        result.append(cast(dict[str, object], item))
    return tuple(result)


def _required_string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("publisher archive cutoff is not a valid timestamp") from exc
    _aware(parsed, "publisher archive cutoff")
    return parsed.astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    _aware(value, "publisher archive timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    if path.is_symlink():
        raise ValueError("publisher archive recovery destination must not be a symlink")
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
        raise ValueError("private publisher archive recovery report must use mode 0600")
