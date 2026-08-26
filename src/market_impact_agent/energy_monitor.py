from __future__ import annotations

import json
import os
import tempfile
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from market_impact_agent.accrual import (
    CandidateEventObservation,
    EventNature,
    LossUnit,
    candidate_event_observation_from_dict,
)
from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import AvailabilityBasis
from market_impact_agent.runtime_store import ArtifactStore
from market_impact_agent.source_coverage import (
    CoverageAttempt,
    CoverageReceipt,
    CoverageSource,
    SourceCoverageRegistration,
)

GDELT_ENERGY_QUERY = (
    '("oil production" OR "gas production" OR pipeline OR LNG) '
    '(outage OR shutdown OR disruption OR explosion OR attack OR "force majeure")'
)
_MAX_RESPONSE_BYTES = 20 * 1024 * 1024
_BOE_BTU = Decimal("5800000")
_KWH_BTU = Decimal("3412")
_BOE_QUANTUM = Decimal("0.000001")


class BytesGetTransport(Protocol):
    def __call__(
        self,
        endpoint: str,
        params: Mapping[str, object],
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class EnergyMonitorCycle:
    receipt: CoverageReceipt
    receipt_path: Path
    artifact_root: Path
    candidates: tuple[CandidateEventObservation, ...]
    raw_by_provider: Mapping[str, bytes]

    def raw_source_for(self, observation: CandidateEventObservation) -> bytes:
        payload = self.raw_by_provider.get(observation.source.provider_id)
        if payload is None:
            raise KeyError(f"cycle has no raw source for {observation.source.provider_id}")
        return payload


@dataclass(frozen=True, slots=True)
class _PollOutput:
    raw_body: bytes
    record_count: int
    entsog_records: tuple[dict[str, object], ...] = ()


class EnergySourceMonitor:
    def __init__(
        self,
        *,
        registration: SourceCoverageRegistration,
        root: Path,
        transport: BytesGetTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        timeout_seconds: float = 25.0,
    ) -> None:
        if not 0 < timeout_seconds <= registration.maximum_cycle_seconds:
            raise ValueError("monitor timeout must fit within maximum coverage cycle")
        self.registration = registration
        self.root = root.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.artifacts = ArtifactStore(self.root / "raw")
        self.receipt_root = self.root / "receipts"
        self.receipt_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.receipt_root, 0o700)
        self._transport = _get_bytes if transport is None else transport
        self._clock = clock
        self._timeout_seconds = timeout_seconds

    def poll(
        self,
        *,
        latest_observations: Mapping[str, CandidateEventObservation] | None = None,
    ) -> EnergyMonitorCycle:
        cycle_started_at = _clock(self._clock)
        attempts: list[CoverageAttempt] = []
        raw_by_provider: dict[str, bytes] = {}
        records_by_provider: dict[str, tuple[dict[str, object], ...]] = {}
        for source in self.registration.sources:
            requested_at = _clock(self._clock)
            try:
                output = self._poll_source(source, requested_at=requested_at)
                retrieved_at = _clock(self._clock)
                if source.provider_id == "entsog-umm":
                    _validate_entsog_records(
                        output.entsog_records,
                        retrieved_at=retrieved_at,
                    )
                artifact = self.artifacts.put_bytes(
                    output.raw_body,
                    media_type="application/octet-stream",
                )
                attempts.append(
                    CoverageAttempt(
                        provider_id=source.provider_id,
                        requested_at=requested_at,
                        retrieved_at=retrieved_at,
                        succeeded=True,
                        content_hash=artifact.content_hash,
                        record_count=output.record_count,
                        error_class=None,
                        error_summary=None,
                    )
                )
                raw_by_provider[source.provider_id] = output.raw_body
                records_by_provider[source.provider_id] = output.entsog_records
            except (HTTPError, URLError, TimeoutError, OSError, TypeError, ValueError) as exc:
                attempts.append(
                    CoverageAttempt(
                        provider_id=source.provider_id,
                        requested_at=requested_at,
                        retrieved_at=None,
                        succeeded=False,
                        content_hash=None,
                        record_count=None,
                        error_class=type(exc).__name__,
                        error_summary=_safe_error(exc),
                    )
                )
        cycle_completed_at = _clock(self._clock)
        receipt_core = {
            "schema_version": "market-impact.coverage-receipt.v1",
            "coverage_registration_id": self.registration.coverage_registration_id,
            "coverage_registration_hash": self.registration.coverage_registration_hash,
            "cycle_started_at": _timestamp(cycle_started_at),
            "cycle_completed_at": _timestamp(cycle_completed_at),
            "attempts": [item.to_dict() for item in attempts],
        }
        receipt = CoverageReceipt(
            receipt_id=f"coverage-receipt-{canonical_hash(receipt_core)}",
            coverage_registration_id=self.registration.coverage_registration_id,
            coverage_registration_hash=self.registration.coverage_registration_hash,
            cycle_started_at=cycle_started_at,
            cycle_completed_at=cycle_completed_at,
            attempts=tuple(attempts),
        )
        receipt.validate_against(self.registration)
        receipt_path = self.receipt_root / f"{receipt.receipt_id}.json"
        _write_private_json(receipt_path, receipt.to_dict())
        latest = {} if latest_observations is None else dict(latest_observations)
        candidates: list[CandidateEventObservation] = []
        for source in self.registration.sources:
            if not source.occurrence_eligible:
                continue
            attempt = receipt.attempt(source.provider_id)
            if not attempt.succeeded:
                continue
            raw_body = raw_by_provider[source.provider_id]
            for record in records_by_provider.get(source.provider_id, ()):
                observation = _entsog_candidate(
                    source=source,
                    registration=self.registration,
                    receipt=receipt,
                    attempt=attempt,
                    raw_body=raw_body,
                    record=record,
                    latest=latest.get(_entsog_event_id(record)),
                )
                if observation is None:
                    continue
                candidates.append(observation)
                latest[observation.event_id] = observation
        return EnergyMonitorCycle(
            receipt=receipt,
            receipt_path=receipt_path,
            artifact_root=self.artifacts.root,
            candidates=tuple(
                sorted(
                    candidates,
                    key=lambda item: (item.source.available_at, item.event_id),
                )
            ),
            raw_by_provider=raw_by_provider,
        )

    def _poll_source(self, source: CoverageSource, *, requested_at: datetime) -> _PollOutput:
        if source.provider_id == "gdelt-energy-discovery":
            raw = self._transport(
                source.endpoint,
                {
                    "query": GDELT_ENERGY_QUERY,
                    "mode": "artlist",
                    "format": "json",
                    "maxrecords": 250,
                    "timespan": "30min",
                    "sort": "datedesc",
                },
                self._timeout_seconds,
            )
            payload = _json_object(raw, "GDELT response")
            articles = _object_list_value(payload.get("articles"), "GDELT articles")
            return _PollOutput(raw_body=raw, record_count=len(articles))
        if source.provider_id == "eia-today-in-energy":
            raw = self._transport(source.endpoint, {}, self._timeout_seconds)
            return _PollOutput(raw_body=raw, record_count=_rss_item_count(raw))
        if source.provider_id == "entsog-umm":
            raw = self._transport(
                source.endpoint,
                {
                    "from": (requested_at - timedelta(days=2)).date().isoformat(),
                    "to": (requested_at + timedelta(days=365)).date().isoformat(),
                    "limit": 1000,
                    "timeZone": "WET",
                },
                self._timeout_seconds,
            )
            payload = _json_object(raw, "ENTSOG response")
            records = _object_list_value(
                payload.get("urgentMarketMessages"),
                "ENTSOG urgentMarketMessages",
            )
            current = _latest_prospective_entsog_records(
                records,
                registered_at=self.registration.registered_at,
            )
            return _PollOutput(
                raw_body=raw,
                record_count=len(records),
                entsog_records=current,
            )
        raise ValueError(f"unsupported registered energy source: {source.provider_id}")


def _latest_prospective_entsog_records(
    records: tuple[dict[str, object], ...],
    *,
    registered_at: datetime,
) -> tuple[dict[str, object], ...]:
    latest: dict[str, tuple[datetime, str, dict[str, object]]] = {}
    for record in records:
        if _optional_string(record.get("messageType")) != "Gas":
            continue
        publication = _source_timestamp(record.get("publicationDateTime"), "publicationDateTime")
        if publication < registered_at:
            continue
        thread_id = _required_string(record.get("threadId"), "threadId")
        version = _required_string(record.get("versionNumber"), "versionNumber")
        candidate = (publication, version, record)
        previous = latest.get(thread_id)
        if previous is None or candidate[:2] > previous[:2]:
            latest[thread_id] = candidate
    return tuple(
        item[2]
        for item in sorted(
            latest.values(),
            key=lambda item: (item[0], item[1]),
        )
    )


def _validate_entsog_records(
    records: tuple[dict[str, object], ...],
    *,
    retrieved_at: datetime,
) -> None:
    for record in records:
        _required_string(record.get("messageId"), "messageId")
        _entsog_event_id(record)
        publication = _source_timestamp(
            record.get("publicationDateTime"),
            "publicationDateTime",
        )
        if publication > retrieved_at:
            raise ValueError("ENTSOG publication time is after actual receipt")
        updated = _optional_source_timestamp(
            record.get("lastUpdateDateTime"),
            "lastUpdateDateTime",
        )
        if updated is not None and not (publication <= updated <= retrieved_at):
            raise ValueError("ENTSOG update time is outside publication and receipt")
        event_start = _optional_source_timestamp(record.get("eventStart"), "eventStart")
        event_stop = _optional_source_timestamp(record.get("eventStop"), "eventStop")
        if event_start is not None and event_stop is not None:
            _duration_hours(event_start, event_stop)
        _required_string(record.get("marketParticipantName"), "marketParticipantName")
        _required_string(record.get("marketParticipantKey"), "marketParticipantKey")
        _entsog_event_nature(record)
        _entsog_loss(record)


def _entsog_candidate(
    *,
    source: CoverageSource,
    registration: SourceCoverageRegistration,
    receipt: CoverageReceipt,
    attempt: CoverageAttempt,
    raw_body: bytes,
    record: dict[str, object],
    latest: CandidateEventObservation | None,
) -> CandidateEventObservation | None:
    if attempt.retrieved_at is None or attempt.content_hash is None:
        raise ValueError("successful ENTSOG attempt is incomplete")
    message_id = _required_string(record.get("messageId"), "messageId")
    if latest is not None and latest.source.upstream_record_id == message_id:
        return None
    event_id = _entsog_event_id(record)
    publication = _source_timestamp(record.get("publicationDateTime"), "publicationDateTime")
    if publication > attempt.retrieved_at:
        raise ValueError("ENTSOG publication time is after actual receipt")
    updated = _optional_source_timestamp(record.get("lastUpdateDateTime"), "lastUpdateDateTime")
    if updated is not None and not (publication <= updated <= attempt.retrieved_at):
        raise ValueError("ENTSOG update time is outside publication and receipt")
    event_start = _optional_source_timestamp(record.get("eventStart"), "eventStart")
    event_stop = _optional_source_timestamp(record.get("eventStop"), "eventStop")
    occurred_at = (
        event_start if event_start is not None and event_start <= attempt.retrieved_at else None
    )
    duration = (
        _duration_hours(event_start, event_stop)
        if event_start is not None and event_stop is not None
        else None
    )
    event_nature = _entsog_event_nature(record)
    loss_amount, loss_unit = _entsog_loss(record)
    participant = _required_string(record.get("marketParticipantName"), "marketParticipantName")
    participant_key = _required_string(record.get("marketParticipantKey"), "marketParticipantKey")
    event_type = _optional_string(record.get("eventType")) or "unclassified gas event"
    asset = _optional_string(record.get("affectedAssetName")) or "unnamed asset"
    claim_summary = (
        f"{participant} reports {event_type} affecting {asset}; "
        "magnitude and duration use the captured UMM fields."
    )
    payload: dict[str, object] = {
        "schema_version": "market-impact.candidate-event-observation.v1",
        "event_id": event_id,
        "source_coverage_registration_id": registration.coverage_registration_id,
        "source_coverage_registration_hash": registration.coverage_registration_hash,
        "coverage_receipt_id": receipt.receipt_id,
        "coverage_receipt_hash": receipt.receipt_hash,
        "event_nature": event_nature.value,
        "affected_commodity": "natural_gas",
        "loss_amount": None if loss_amount is None else str(loss_amount),
        "loss_unit": None if loss_unit is None else loss_unit.value,
        "regional_denominator_source_ref": None,
        "regional_denominator_source_tier": None,
        "regional_denominator_available_at": None,
        "regional_denominator_raw_content_hash": None,
        "expected_duration_hours": None if duration is None else str(duration),
        "source": {
            "provider_id": source.provider_id,
            "upstream_source": participant_key,
            "upstream_record_id": message_id,
            "source_ref": f"{source.endpoint}?messageId={quote(message_id, safe='')}",
            "source_tier": source.source_tier.value,
            "occurred_at": _optional_timestamp(occurred_at),
            "published_at": _timestamp(publication),
            "source_updated_at": _optional_timestamp(updated),
            "available_at": _timestamp(attempt.retrieved_at),
            "retrieved_at": _timestamp(attempt.retrieved_at),
            "availability_basis": AvailabilityBasis.ACTUAL_RECEIPT.value,
            "raw_content_hash": sha256(raw_body).hexdigest(),
            "claim_summary": claim_summary,
            "claim_hash": sha256(claim_summary.encode()).hexdigest(),
        },
        "supersedes_observation_id": None if latest is None else latest.observation_id,
    }
    payload["observation_id"] = f"candidate-observation-{canonical_hash(payload)}"
    return candidate_event_observation_from_dict(payload)


def _entsog_event_id(record: Mapping[str, object]) -> str:
    thread_id = _required_string(record.get("threadId"), "threadId")
    return f"entsog-umm-{sha256(thread_id.encode()).hexdigest()}"


def _entsog_event_nature(record: Mapping[str, object]) -> EventNature:
    unavailability_type = (_optional_string(record.get("unavailabilityType")) or "").casefold()
    if unavailability_type == "planned":
        return EventNature.PLANNED_MAINTENANCE
    if unavailability_type != "unplanned":
        return EventNature.UNCLASSIFIED
    event_type = (_optional_string(record.get("eventType")) or "").casefold()
    if "production field" in event_type or "gas treatment plant" in event_type:
        return EventNature.PHYSICAL_PRODUCTION_LOSS
    if any(word in event_type for word in ("storage", "injection", "withdrawal")):
        return EventNature.PHYSICAL_STORAGE_LOSS
    if any(
        word in event_type
        for word in (
            "pipeline",
            "transmission",
            "regasification",
            "compressor",
        )
    ):
        return EventNature.PHYSICAL_TRANSPORT_LOSS
    return EventNature.UNCLASSIFIED


def _entsog_loss(record: Mapping[str, object]) -> tuple[Decimal | None, LossUnit | None]:
    capacity = _optional_decimal(record.get("unavailableCapacity"), "unavailableCapacity")
    unit = (_optional_string(record.get("unitMeasure")) or "").casefold()
    if capacity is None or capacity <= 0:
        return None, None
    if unit == "kwh/d":
        daily_kwh = capacity
    elif unit == "kwh/h":
        daily_kwh = capacity * Decimal("24")
    else:
        return None, None
    boe_per_day = (daily_kwh * _KWH_BTU / _BOE_BTU).quantize(
        _BOE_QUANTUM,
        rounding=ROUND_DOWN,
    )
    return boe_per_day, LossUnit.BOE_PER_DAY


def _duration_hours(start: datetime, stop: datetime) -> Decimal | None:
    if stop <= start:
        return None
    seconds = Decimal(str((stop - start).total_seconds()))
    return (seconds / Decimal("3600")).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)


def _rss_item_count(raw: bytes) -> int:
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("EIA RSS response is invalid XML") from exc
    return len(root.findall(".//item"))


def _json_object(raw: bytes, name: str) -> dict[str, object]:
    try:
        payload: object = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise TypeError(f"{name} must be an object")
    value = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], payload)


def _object_list_value(value: object, name: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    items = cast(list[object], value)
    result: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            raise TypeError(f"{name} entries must be objects")
        raw = cast(dict[object, object], item)
        if any(not isinstance(key, str) for key in raw):
            raise TypeError(f"{name} keys must be strings")
        result.append(cast(dict[str, object], item))
    return tuple(result)


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("optional source string has invalid type")
    stripped = value.strip()
    return stripped or None


def _optional_decimal(value: object, name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a decimal string")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must be finite")
    return parsed


def _source_timestamp(value: object, name: str) -> datetime:
    raw = _required_string(value, name)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _optional_source_timestamp(value: object, name: str) -> datetime | None:
    if value is None or value == "":
        return None
    return _source_timestamp(value, name)


def _clock(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    require_aware(value, "monitor clock")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _safe_error(error: BaseException) -> str:
    if isinstance(error, HTTPError):
        return f"HTTP status {error.code}"
    if isinstance(error, URLError):
        return "network request failed"
    text = str(error).replace("\n", " ").strip()
    return (text or type(error).__name__)[:200]


def _write_private_json(path: Path, payload: object) -> None:
    encoded = canonical_json_bytes(payload)
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != encoded:
            raise ValueError("existing Coverage Receipt path has different content")
        os.chmod(path, 0o600)
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-receipt-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


def _get_bytes(
    endpoint: str,
    params: Mapping[str, object],
    timeout_seconds: float,
) -> bytes:
    query = urlencode([(key, str(value)) for key, value in params.items()])
    url = endpoint if not query else f"{endpoint}?{query}"
    request = Request(
        url,
        headers={
            "Accept": "application/json, application/rss+xml, application/xml, text/xml",
            "User-Agent": "market-impact-agent/0.1 source-monitor",
        },
        method="GET",
    )
    opener = build_opener(_NoRedirect())
    with opener.open(request, timeout=timeout_seconds) as response:
        payload = response.read(_MAX_RESPONSE_BYTES + 1)
    if len(payload) > _MAX_RESPONSE_BYTES:
        raise ValueError("source response exceeds 20 MiB")
    if not payload:
        raise ValueError("source response is empty")
    return payload
