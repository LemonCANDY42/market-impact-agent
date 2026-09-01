from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import (
    ExecutionReceipt,
    ExecutionStatus,
    OrderKind,
    Side,
    TradingEnvironment,
    require_aware,
)
from market_impact_agent.ibkr_nautilus_paper import (
    IBKR_NAUTILUS_PAPER_PROVIDER_ID,
    IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
    IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
    IBKR_NAUTILUS_VERSION,
)
from market_impact_agent.providers import (
    CancellationCapability,
    CancellationCapabilityRejected,
    CancellationCommandReceipt,
    CancellationCommandStatus,
    Capability,
    ProviderManifest,
    ProviderTransport,
    ReconciliationSnapshot,
    SubmissionCapability,
    SubmissionCapabilityRejected,
    TrustTier,
)

IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA = "market-impact.ibkr-nautilus-paper-provider-acceptance.v3"
IBKR_NAUTILUS_PAPER_SCENARIO_RESULT_SCHEMA = "market-impact.ibkr-nautilus-paper-scenario-result.v1"
_REQUIRED_ACCEPTANCE_SCENARIOS = frozenset(
    {
        "account_reconciliation",
        "ambiguous_acknowledgement",
        "cancel",
        "disconnect",
        "duplicate_fill",
        "external_order",
        "gateway_restart",
        "partial_fill",
        "process_restart",
        "replace",
        "submit",
    }
)
_HASH = re.compile(r"[0-9a-f]{64}")
_ACCOUNT_REFERENCE_HASH = re.compile(r"account-ref-[0-9a-f]{64}")
_AUTHORITY_ID = re.compile(r"ibkr-nautilus-paper-authority-[0-9a-f]{64}")
_SCENARIO_OBSERVATION_SEAL = object()
_ACCEPTANCE_SEAL = object()
_VERIFIER_SEAL = object()
_PROVIDER_FACTORY_SEAL = object()
_CANONICAL_RUNTIME_HANDLES: dict[tuple[str, str], NautilusPaperExecutionRuntime] = {}


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperScenarioEvidence:
    """Anonymous content evidence for one acceptance scenario."""

    scenario: str
    evidence_hash: str
    passed: bool

    def __post_init__(self) -> None:
        if not self.scenario or self.scenario != self.scenario.strip():
            raise ValueError("acceptance scenario must be non-empty and trimmed")
        if _HASH.fullmatch(self.evidence_hash) is None:
            raise ValueError("scenario evidence must be a SHA-256 hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario": self.scenario,
            "evidence_hash": self.evidence_hash,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperScenarioObservation:
    observation_id: str
    scenario: str
    artifact_hash: str
    result_hash: str
    runner_id: str
    runner_seal: str
    configuration_hash: str
    account_reference_hash: str
    instrument_routes_hash: str
    markets: tuple[str, ...]
    order_types: tuple[str, ...]
    time_in_force: tuple[str, ...]
    nautilus_ibapi_version: str
    effective_client_id: int
    client_id_collision: bool
    manual_order_auto_bind_observed: bool
    exclusive_api_client_scope_observed: bool
    passed: bool
    observed_at: datetime
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _SCENARIO_OBSERVATION_SEAL:
            raise TypeError("scenario observations must be issued by the Harness evidence resolver")
        if self.scenario not in _REQUIRED_ACCEPTANCE_SCENARIOS:
            raise ValueError("scenario is not an accepted IBKR Paper capability scenario")
        for value in (
            self.artifact_hash,
            self.result_hash,
            self.configuration_hash,
            self.instrument_routes_hash,
            self.runner_seal,
        ):
            if _HASH.fullmatch(value) is None:
                raise ValueError("scenario observation hashes must be SHA-256")
        if _ACCOUNT_REFERENCE_HASH.fullmatch(self.account_reference_hash) is None:
            raise ValueError("scenario observation account scope must be opaque")
        if not self.runner_id or self.runner_id != self.runner_id.strip():
            raise ValueError("scenario observation runner identity is invalid")
        require_aware(self.observed_at, "scenario observed_at")
        expected_id = "ibkr-nautilus-paper-observation-" + canonical_hash(self.core_dict())
        if self.observation_id != expected_id:
            raise ValueError("scenario observation identity does not match content")

    def core_dict(self) -> dict[str, object]:
        return {
            "scenario": self.scenario,
            "artifact_hash": self.artifact_hash,
            "result_hash": self.result_hash,
            "runner_id": self.runner_id,
            "runner_seal": self.runner_seal,
            "configuration_hash": self.configuration_hash,
            "account_reference_hash": self.account_reference_hash,
            "instrument_routes_hash": self.instrument_routes_hash,
            "markets": list(self.markets),
            "order_types": list(self.order_types),
            "time_in_force": list(self.time_in_force),
            "nautilus_ibapi_version": self.nautilus_ibapi_version,
            "effective_client_id": self.effective_client_id,
            "client_id_collision": self.client_id_collision,
            "manual_order_auto_bind_observed": self.manual_order_auto_bind_observed,
            "exclusive_api_client_scope_observed": self.exclusive_api_client_scope_observed,
            "passed": self.passed,
            "observed_at": _timestamp(self.observed_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {"observation_id": self.observation_id, **self.core_dict()}


def _issue_scenario_observation(
    **fields: Any,
) -> IbkrNautilusPaperScenarioObservation:
    core = dict(fields)
    core["markets"] = list(cast(tuple[str, ...], fields["markets"]))
    core["order_types"] = list(cast(tuple[str, ...], fields["order_types"]))
    core["time_in_force"] = list(cast(tuple[str, ...], fields["time_in_force"]))
    core["observed_at"] = _timestamp(cast(datetime, fields["observed_at"]))
    return IbkrNautilusPaperScenarioObservation(
        observation_id="ibkr-nautilus-paper-observation-" + canonical_hash(core),
        _seal=_SCENARIO_OBSERVATION_SEAL,
        **fields,
    )


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperProviderAcceptance:
    """Harness-owned evidence that may enable one exact external Paper provider build."""

    acceptance_id: str
    authority_id: str
    provider_id: str
    provider_version: str
    runtime_version: str
    nautilus_version: str
    nautilus_ibapi_version: str
    environment: str
    configuration_hash: str
    account_reference_hash: str
    instrument_routes_hash: str
    markets: tuple[str, ...]
    order_types: tuple[str, ...]
    time_in_force: tuple[str, ...]
    exclusive_api_client_scope: bool
    manual_order_auto_bind: bool
    scenario_evidence: tuple[IbkrNautilusPaperScenarioEvidence, ...]
    accepted_at: datetime
    valid_until: datetime
    complete: bool
    gaps: tuple[str, ...]
    _seal: object = field(repr=False, compare=False)
    schema_version: str = IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA

    def __post_init__(self) -> None:
        if self._seal is not _ACCEPTANCE_SEAL:
            raise TypeError("Provider Acceptance must be issued by its durable evidence authority")
        if _AUTHORITY_ID.fullmatch(self.authority_id) is None:
            raise ValueError("Provider Acceptance authority identity is invalid")
        if self.schema_version != IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA:
            raise ValueError("unsupported IBKR Nautilus Paper acceptance schema")
        if self.provider_id != IBKR_NAUTILUS_PAPER_PROVIDER_ID:
            raise ValueError("acceptance targets another Provider")
        if self.provider_version != IBKR_NAUTILUS_PAPER_PROVIDER_VERSION:
            raise ValueError("acceptance targets another Provider version")
        if self.runtime_version != IBKR_NAUTILUS_PAPER_RUNTIME_VERSION:
            raise ValueError("acceptance targets another runtime version")
        if self.nautilus_version != IBKR_NAUTILUS_VERSION:
            raise ValueError("acceptance targets another NautilusTrader version")
        if not self.nautilus_ibapi_version or (
            self.nautilus_ibapi_version != self.nautilus_ibapi_version.strip()
        ):
            raise ValueError("acceptance Nautilus IB API version is invalid")
        if self.environment != TradingEnvironment.PAPER.value:
            raise ValueError("IBKR Nautilus acceptance must be Paper-only")
        if _HASH.fullmatch(self.configuration_hash) is None:
            raise ValueError("configuration_hash must be a SHA-256 hash")
        if _ACCOUNT_REFERENCE_HASH.fullmatch(self.account_reference_hash) is None:
            raise ValueError("account_reference_hash must be opaque")
        if _HASH.fullmatch(self.instrument_routes_hash) is None:
            raise ValueError("instrument_routes_hash must be a SHA-256 hash")
        _sorted_unique(self.markets, "acceptance markets")
        _sorted_unique(self.order_types, "acceptance order_types")
        _sorted_unique(self.time_in_force, "acceptance time_in_force")
        _sorted_unique(self.gaps, "acceptance gaps")
        scenarios = tuple(item.scenario for item in self.scenario_evidence)
        if scenarios != tuple(sorted(set(scenarios))):
            raise ValueError("scenario evidence must be sorted and unique by scenario")
        evidence_hashes = tuple(item.evidence_hash for item in self.scenario_evidence)
        if len(evidence_hashes) != len(set(evidence_hashes)):
            raise ValueError("scenario evidence hashes must be unique")
        if not self.markets or not self.order_types or not self.time_in_force:
            raise ValueError("acceptance requires markets, order types, and time-in-force")
        if not self.scenario_evidence:
            raise ValueError("acceptance requires scenario evidence")
        if any(item != item.upper() for item in self.markets):
            raise ValueError("acceptance markets must use uppercase canonical identifiers")
        if any(item not in {"market", "limit"} for item in self.order_types):
            raise ValueError("acceptance contains an unsupported order type")
        if any(item != "DAY" for item in self.time_in_force):
            raise ValueError("acceptance contains an unsupported time-in-force")
        require_aware(self.accepted_at, "acceptance accepted_at")
        require_aware(self.valid_until, "acceptance valid_until")
        if self.valid_until <= self.accepted_at:
            raise ValueError("acceptance valid_until must be after accepted_at")
        expected_id = "ibkr-nautilus-paper-acceptance-" + canonical_hash(self.core_dict())
        if self.acceptance_id != expected_id:
            raise ValueError("acceptance_id does not match content")

    @property
    def accepted_scenarios(self) -> tuple[str, ...]:
        return tuple(item.scenario for item in self.scenario_evidence if item.passed)

    @property
    def evidence_hashes(self) -> tuple[str, ...]:
        return tuple(sorted(item.evidence_hash for item in self.scenario_evidence))

    @property
    def execution_accepted(self) -> bool:
        return (
            self.complete
            and not self.gaps
            and self.exclusive_api_client_scope
            and self.manual_order_auto_bind
            and set(self.accepted_scenarios) >= _REQUIRED_ACCEPTANCE_SCENARIOS
        )

    def is_current(self, now: datetime) -> bool:
        require_aware(now, "acceptance evaluation time")
        return self.execution_accepted and self.accepted_at <= now < self.valid_until

    def allows_risk_reduction(self, now: datetime) -> bool:
        """Keep exact-scope cancel available after submit admission expires."""

        require_aware(now, "acceptance evaluation time")
        return self.execution_accepted and self.accepted_at <= now

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "authority_id": self.authority_id,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "runtime_version": self.runtime_version,
            "nautilus_version": self.nautilus_version,
            "nautilus_ibapi_version": self.nautilus_ibapi_version,
            "environment": self.environment,
            "configuration_hash": self.configuration_hash,
            "account_reference_hash": self.account_reference_hash,
            "instrument_routes_hash": self.instrument_routes_hash,
            "markets": list(self.markets),
            "order_types": list(self.order_types),
            "time_in_force": list(self.time_in_force),
            "exclusive_api_client_scope": self.exclusive_api_client_scope,
            "manual_order_auto_bind": self.manual_order_auto_bind,
            "scenario_evidence": [item.to_dict() for item in self.scenario_evidence],
            "accepted_at": _timestamp(self.accepted_at),
            "valid_until": _timestamp(self.valid_until),
            "complete": self.complete,
            "gaps": list(self.gaps),
        }

    def to_dict(self) -> dict[str, object]:
        return {"acceptance_id": self.acceptance_id, **self.core_dict()}

    @classmethod
    def build_from_authority(
        cls,
        *,
        authority_id: str,
        configuration_hash: str,
        account_reference_hash: str,
        instrument_routes_hash: str,
        markets: tuple[str, ...],
        order_types: tuple[str, ...],
        time_in_force: tuple[str, ...],
        nautilus_ibapi_version: str,
        exclusive_api_client_scope: bool,
        manual_order_auto_bind: bool,
        scenario_evidence: tuple[IbkrNautilusPaperScenarioEvidence, ...],
        accepted_at: datetime,
        valid_until: datetime,
        complete: bool,
        gaps: tuple[str, ...] = (),
        _seal: object,
    ) -> IbkrNautilusPaperProviderAcceptance:
        core = {
            "schema_version": IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA,
            "authority_id": authority_id,
            "provider_id": IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            "provider_version": IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
            "runtime_version": IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
            "nautilus_version": IBKR_NAUTILUS_VERSION,
            "nautilus_ibapi_version": nautilus_ibapi_version,
            "environment": TradingEnvironment.PAPER.value,
            "configuration_hash": configuration_hash,
            "account_reference_hash": account_reference_hash,
            "instrument_routes_hash": instrument_routes_hash,
            "markets": list(markets),
            "order_types": list(order_types),
            "time_in_force": list(time_in_force),
            "exclusive_api_client_scope": exclusive_api_client_scope,
            "manual_order_auto_bind": manual_order_auto_bind,
            "scenario_evidence": [item.to_dict() for item in scenario_evidence],
            "accepted_at": _timestamp(accepted_at),
            "valid_until": _timestamp(valid_until),
            "complete": complete,
            "gaps": list(gaps),
        }
        return cls(
            acceptance_id="ibkr-nautilus-paper-acceptance-" + canonical_hash(core),
            authority_id=authority_id,
            provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
            runtime_version=IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
            nautilus_version=IBKR_NAUTILUS_VERSION,
            nautilus_ibapi_version=nautilus_ibapi_version,
            environment=TradingEnvironment.PAPER.value,
            configuration_hash=configuration_hash,
            account_reference_hash=account_reference_hash,
            instrument_routes_hash=instrument_routes_hash,
            markets=markets,
            order_types=order_types,
            time_in_force=time_in_force,
            exclusive_api_client_scope=exclusive_api_client_scope,
            manual_order_auto_bind=manual_order_auto_bind,
            scenario_evidence=scenario_evidence,
            accepted_at=accepted_at,
            valid_until=valid_until,
            complete=complete,
            gaps=gaps,
            _seal=_seal,
        )

    @classmethod
    def from_dict(
        cls,
        payload: object,
        *,
        authority: IbkrNautilusPaperAcceptanceVerifier,
    ) -> IbkrNautilusPaperProviderAcceptance:
        return authority.resolve_payload(payload)

    @classmethod
    def from_authority_dict(
        cls,
        payload: object,
        *,
        _seal: object,
    ) -> IbkrNautilusPaperProviderAcceptance:
        if not isinstance(payload, dict):
            raise TypeError("IBKR Nautilus Paper acceptance must be an object")
        fields = cast(dict[str, Any], payload)
        expected = {
            "schema_version",
            "authority_id",
            "acceptance_id",
            "provider_id",
            "provider_version",
            "runtime_version",
            "nautilus_version",
            "nautilus_ibapi_version",
            "environment",
            "configuration_hash",
            "account_reference_hash",
            "instrument_routes_hash",
            "markets",
            "order_types",
            "time_in_force",
            "exclusive_api_client_scope",
            "manual_order_auto_bind",
            "scenario_evidence",
            "accepted_at",
            "valid_until",
            "complete",
            "gaps",
        }
        if set(fields) != expected:
            raise ValueError("IBKR Nautilus Paper acceptance fields are invalid")
        for name in (
            "schema_version",
            "authority_id",
            "acceptance_id",
            "provider_id",
            "provider_version",
            "runtime_version",
            "nautilus_version",
            "nautilus_ibapi_version",
            "environment",
            "configuration_hash",
            "account_reference_hash",
            "instrument_routes_hash",
            "accepted_at",
            "valid_until",
        ):
            if not isinstance(fields[name], str):
                raise TypeError(f"acceptance {name} must be a string")
        if not isinstance(fields["complete"], bool):
            raise TypeError("acceptance complete must be a boolean")
        for name in ("exclusive_api_client_scope", "manual_order_auto_bind"):
            if not isinstance(fields[name], bool):
                raise TypeError(f"acceptance {name} must be a boolean")
        scenario_evidence = _scenario_evidence_tuple(fields["scenario_evidence"])
        return cls(
            schema_version=cast(str, fields["schema_version"]),
            authority_id=cast(str, fields["authority_id"]),
            acceptance_id=cast(str, fields["acceptance_id"]),
            provider_id=cast(str, fields["provider_id"]),
            provider_version=cast(str, fields["provider_version"]),
            runtime_version=cast(str, fields["runtime_version"]),
            nautilus_version=cast(str, fields["nautilus_version"]),
            nautilus_ibapi_version=cast(str, fields["nautilus_ibapi_version"]),
            environment=cast(str, fields["environment"]),
            configuration_hash=cast(str, fields["configuration_hash"]),
            account_reference_hash=cast(str, fields["account_reference_hash"]),
            instrument_routes_hash=cast(str, fields["instrument_routes_hash"]),
            markets=_string_tuple(fields["markets"], "acceptance markets"),
            order_types=_string_tuple(fields["order_types"], "acceptance order_types"),
            time_in_force=_string_tuple(fields["time_in_force"], "acceptance time_in_force"),
            exclusive_api_client_scope=fields["exclusive_api_client_scope"],
            manual_order_auto_bind=fields["manual_order_auto_bind"],
            scenario_evidence=scenario_evidence,
            accepted_at=_datetime(cast(str, fields["accepted_at"])),
            valid_until=_datetime(cast(str, fields["valid_until"])),
            complete=fields["complete"],
            gaps=_string_tuple(fields["gaps"], "acceptance gaps"),
            _seal=_seal,
        )


class IbkrNautilusPaperAcceptanceRunner:
    """Secret-bearing capability that authenticates exact acceptance evidence bytes."""

    def __init__(self, runner_id: str, signing_key: bytes) -> None:
        if not runner_id or runner_id != runner_id.strip():
            raise ValueError("acceptance runner identity is invalid")
        if len(signing_key) < 32:
            raise ValueError("acceptance runner signing key must contain at least 32 bytes")
        self._runner_id = runner_id
        self._signing_key = signing_key

    @property
    def runner_id(self) -> str:
        return self._runner_id

    @property
    def authority_id(self) -> str:
        return _acceptance_authority_id(self._runner_id, self._signing_key)

    def seal_evidence(self, *, artifact_path: Path, result_path: Path) -> str:
        return _runner_evidence_seal(
            runner_id=self._runner_id,
            artifact_bytes=artifact_path.read_bytes(),
            result_bytes=result_path.read_bytes(),
            key=self._signing_key,
        )


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperAcceptanceVerifier:
    """Immutable Provider-side verifier for one pinned authority and runner key."""

    _state_path: Path
    _runner_id: str
    _verification_key: bytes = field(repr=False)
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _VERIFIER_SEAL:
            raise TypeError("acceptance verifier must be issued by its configured authority")

    @property
    def authority_id(self) -> str:
        return _acceptance_authority_id(self._runner_id, self._verification_key)

    def resolve_payload(self, payload: object) -> IbkrNautilusPaperProviderAcceptance:
        resolver = IbkrNautilusPaperAcceptanceAuthority(
            self._state_path,
            runner_id=self._runner_id,
            verification_key=self._verification_key,
            _read_only=True,
        )
        return resolver.resolve_payload(payload)

    def verify(self, acceptance: IbkrNautilusPaperProviderAcceptance) -> bool:
        try:
            resolved = self.resolve_payload(acceptance.to_dict())
        except (KeyError, OSError, TypeError, ValueError):
            return False
        return resolved == acceptance


class IbkrNautilusPaperAcceptanceAuthority:
    """Durable resolver for sealed, exact-scope scenario observations and acceptances."""

    def __init__(
        self,
        state_path: Path,
        *,
        runner_id: str,
        verification_key: bytes,
        _read_only: bool = False,
    ) -> None:
        if not runner_id or runner_id != runner_id.strip():
            raise ValueError("acceptance runner identity is invalid")
        if len(verification_key) < 32:
            raise ValueError("acceptance verification key must contain at least 32 bytes")
        self._state_path = state_path.resolve()
        self._runner_id = runner_id
        self._verification_key = verification_key
        self._read_only = _read_only
        if _read_only:
            if not self._state_path.is_file():
                raise FileNotFoundError("configured acceptance authority store does not exist")
            return
        self._state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_scenario_observations (
                    observation_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_scenario_artifacts (
                    observation_id TEXT PRIMARY KEY,
                    artifact_bytes BLOB NOT NULL,
                    result_bytes BLOB NOT NULL,
                    FOREIGN KEY (observation_id)
                        REFERENCES ibkr_nautilus_scenario_observations(observation_id)
                );
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_acceptances (
                    acceptance_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_acceptance_observations (
                    acceptance_id TEXT NOT NULL,
                    observation_id TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    PRIMARY KEY (acceptance_id, observation_id),
                    UNIQUE (acceptance_id, scenario),
                    FOREIGN KEY (acceptance_id)
                        REFERENCES ibkr_nautilus_acceptances(acceptance_id),
                    FOREIGN KEY (observation_id)
                        REFERENCES ibkr_nautilus_scenario_observations(observation_id)
                );
                """
            )
        os.chmod(self._state_path, 0o600)

    @property
    def authority_id(self) -> str:
        return _acceptance_authority_id(self._runner_id, self._verification_key)

    def verifier(self) -> IbkrNautilusPaperAcceptanceVerifier:
        return IbkrNautilusPaperAcceptanceVerifier(
            _state_path=self._state_path,
            _runner_id=self._runner_id,
            _verification_key=self._verification_key,
            _seal=_VERIFIER_SEAL,
        )

    def record_scenario_evidence(
        self,
        *,
        artifact_path: Path,
        result_path: Path,
        runner_seal: str,
    ) -> IbkrNautilusPaperScenarioObservation:
        """Resolve and durably bind one exact observed artifact/result pair."""

        if self._read_only:
            raise RuntimeError("configured acceptance verifier is immutable")
        artifact_bytes = artifact_path.read_bytes()
        result_bytes = result_path.read_bytes()
        expected_runner_seal = _runner_evidence_seal(
            runner_id=self._runner_id,
            artifact_bytes=artifact_bytes,
            result_bytes=result_bytes,
            key=self._verification_key,
        )
        if not hmac.compare_digest(runner_seal, expected_runner_seal):
            raise PermissionError("scenario evidence lacks trusted runner provenance")
        try:
            artifact = cast(object, json.loads(artifact_bytes))
            result = cast(object, json.loads(result_bytes))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("scenario evidence artifacts must contain valid JSON") from error
        if not isinstance(artifact, dict):
            raise ValueError("scenario artifact must identify its observed scenario")
        artifact_fields = cast(dict[str, object], artifact)
        if not isinstance(artifact_fields.get("scenario"), str):
            raise ValueError("scenario artifact must identify its observed scenario")
        observation = _scenario_observation_from_result(
            result,
            artifact_hash=hashlib.sha256(artifact_bytes).hexdigest(),
            result_hash=hashlib.sha256(result_bytes).hexdigest(),
            runner_id=self._runner_id,
            runner_seal=runner_seal,
        )
        if artifact_fields["scenario"] != observation.scenario:
            raise ValueError("scenario artifact/result identity mismatch")
        payload_json = _canonical_json(observation.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT payload_json
                FROM ibkr_nautilus_scenario_observations
                WHERE observation_id = ?
                """,
                (observation.observation_id,),
            ).fetchone()
            if existing is not None and cast(str, existing["payload_json"]) != payload_json:
                raise ValueError("scenario observation identity conflict")
            connection.execute(
                "INSERT OR IGNORE INTO ibkr_nautilus_scenario_observations VALUES (?, ?)",
                (observation.observation_id, payload_json),
            )
            evidence = connection.execute(
                """
                SELECT artifact_bytes, result_bytes
                FROM ibkr_nautilus_scenario_artifacts
                WHERE observation_id = ?
                """,
                (observation.observation_id,),
            ).fetchone()
            if evidence is not None and (
                bytes(evidence["artifact_bytes"]) != artifact_bytes
                or bytes(evidence["result_bytes"]) != result_bytes
            ):
                raise ValueError("scenario evidence artifact identity conflict")
            connection.execute(
                """
                INSERT OR IGNORE INTO ibkr_nautilus_scenario_artifacts
                    (observation_id, artifact_bytes, result_bytes)
                VALUES (?, ?, ?)
                """,
                (observation.observation_id, artifact_bytes, result_bytes),
            )
        return observation

    def build_acceptance(
        self,
        *,
        observation_ids: tuple[str, ...],
        configuration_hash: str,
        account_reference_hash: str,
        instrument_routes_hash: str,
        markets: tuple[str, ...],
        order_types: tuple[str, ...],
        time_in_force: tuple[str, ...],
        nautilus_ibapi_version: str,
        accepted_at: datetime,
        valid_until: datetime,
        gaps: tuple[str, ...] = (),
    ) -> IbkrNautilusPaperProviderAcceptance:
        if self._read_only:
            raise RuntimeError("configured acceptance verifier is immutable")
        if observation_ids != tuple(sorted(set(observation_ids))):
            raise ValueError("scenario observation identities must be sorted and unique")
        observations = tuple(self.resolve_observation(item) for item in observation_ids)
        expected_scope = (
            configuration_hash,
            account_reference_hash,
            instrument_routes_hash,
            markets,
            order_types,
            time_in_force,
            nautilus_ibapi_version,
        )
        for observation in observations:
            observed_scope = (
                observation.configuration_hash,
                observation.account_reference_hash,
                observation.instrument_routes_hash,
                observation.markets,
                observation.order_types,
                observation.time_in_force,
                observation.nautilus_ibapi_version,
            )
            if observed_scope != expected_scope:
                raise ValueError("scenario observation runtime scope mismatch")
        scenarios = tuple(observation.scenario for observation in observations)
        complete = (
            not gaps
            and frozenset(scenarios) == _REQUIRED_ACCEPTANCE_SCENARIOS
            and len(scenarios) == len(set(scenarios))
            and all(observation.passed for observation in observations)
            and all(observation.effective_client_id == 0 for observation in observations)
            and not any(observation.client_id_collision for observation in observations)
            and all(observation.manual_order_auto_bind_observed for observation in observations)
            and all(observation.exclusive_api_client_scope_observed for observation in observations)
        )
        scenario_evidence = tuple(
            sorted(
                (
                    IbkrNautilusPaperScenarioEvidence(
                        scenario=observation.scenario,
                        evidence_hash=canonical_hash(
                            {
                                "observation_id": observation.observation_id,
                                "artifact_hash": observation.artifact_hash,
                                "result_hash": observation.result_hash,
                            }
                        ),
                        passed=observation.passed,
                    )
                    for observation in observations
                ),
                key=lambda item: item.scenario,
            )
        )
        acceptance = IbkrNautilusPaperProviderAcceptance.build_from_authority(
            authority_id=self.authority_id,
            configuration_hash=configuration_hash,
            account_reference_hash=account_reference_hash,
            instrument_routes_hash=instrument_routes_hash,
            markets=markets,
            order_types=order_types,
            time_in_force=time_in_force,
            nautilus_ibapi_version=nautilus_ibapi_version,
            exclusive_api_client_scope=bool(observations)
            and all(item.exclusive_api_client_scope_observed for item in observations),
            manual_order_auto_bind=bool(observations)
            and all(item.manual_order_auto_bind_observed for item in observations),
            scenario_evidence=scenario_evidence,
            accepted_at=accepted_at,
            valid_until=valid_until,
            complete=complete,
            gaps=gaps,
            _seal=_ACCEPTANCE_SEAL,
        )
        payload_json = _canonical_json(acceptance.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT OR REPLACE INTO ibkr_nautilus_acceptances VALUES (?, ?)",
                (acceptance.acceptance_id, payload_json),
            )
            connection.execute(
                "DELETE FROM ibkr_nautilus_acceptance_observations WHERE acceptance_id = ?",
                (acceptance.acceptance_id,),
            )
            connection.executemany(
                """
                INSERT INTO ibkr_nautilus_acceptance_observations
                    (acceptance_id, observation_id, scenario)
                VALUES (?, ?, ?)
                """,
                (
                    (acceptance.acceptance_id, item.observation_id, item.scenario)
                    for item in observations
                ),
            )
        return acceptance

    def resolve_observation(self, observation_id: str) -> IbkrNautilusPaperScenarioObservation:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT observation.payload_json, evidence.artifact_bytes, evidence.result_bytes
                FROM ibkr_nautilus_scenario_observations AS observation
                JOIN ibkr_nautilus_scenario_artifacts AS evidence USING (observation_id)
                WHERE observation.observation_id = ?
                """,
                (observation_id,),
            ).fetchone()
        if row is None:
            raise KeyError("unknown scenario observation")
        stored = _scenario_observation_from_dict(
            json.loads(cast(str, row["payload_json"])),
            _seal=_SCENARIO_OBSERVATION_SEAL,
        )
        resolved = _scenario_observation_from_result(
            json.loads(bytes(row["result_bytes"])),
            artifact_hash=hashlib.sha256(bytes(row["artifact_bytes"])).hexdigest(),
            result_hash=hashlib.sha256(bytes(row["result_bytes"])).hexdigest(),
            runner_id=self._runner_id,
            runner_seal=stored.runner_seal,
        )
        artifact = cast(object, json.loads(bytes(row["artifact_bytes"])))
        artifact_fields = cast(dict[str, object], artifact) if isinstance(artifact, dict) else None
        if (
            artifact_fields is None
            or artifact_fields.get("scenario") != stored.scenario
            or resolved != stored
            or stored.runner_id != self._runner_id
            or not hmac.compare_digest(
                stored.runner_seal,
                _runner_evidence_seal(
                    runner_id=self._runner_id,
                    artifact_bytes=bytes(row["artifact_bytes"]),
                    result_bytes=bytes(row["result_bytes"]),
                    key=self._verification_key,
                ),
            )
        ):
            raise ValueError("durable scenario evidence no longer resolves exactly")
        return stored

    def resolve_payload(self, payload: object) -> IbkrNautilusPaperProviderAcceptance:
        if not isinstance(payload, dict):
            raise TypeError("IBKR Nautilus Paper acceptance must be an identified object")
        fields = cast(dict[str, object], payload)
        if not isinstance(fields.get("acceptance_id"), str):
            raise TypeError("IBKR Nautilus Paper acceptance must be an identified object")
        acceptance_id = cast(str, fields["acceptance_id"])
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json
                FROM ibkr_nautilus_acceptances
                WHERE acceptance_id = ?
                """,
                (acceptance_id,),
            ).fetchone()
        if row is None:
            raise KeyError("unknown Provider Acceptance")
        stored = cast(object, json.loads(cast(str, row["payload_json"])))
        if _canonical_json(fields) != _canonical_json(stored):
            raise ValueError("Provider Acceptance payload does not match durable authority")
        acceptance = IbkrNautilusPaperProviderAcceptance.from_authority_dict(
            stored,
            _seal=_ACCEPTANCE_SEAL,
        )
        if acceptance.authority_id != self.authority_id:
            raise ValueError("Provider Acceptance targets another configured authority")
        with self._connect() as connection:
            links = connection.execute(
                """
                SELECT observation_id, scenario
                FROM ibkr_nautilus_acceptance_observations
                WHERE acceptance_id = ?
                ORDER BY scenario
                """,
                (acceptance_id,),
            ).fetchall()
        observations = tuple(
            self.resolve_observation(cast(str, link["observation_id"])) for link in links
        )
        if tuple(item.scenario for item in observations) != tuple(
            cast(str, link["scenario"]) for link in links
        ):
            raise ValueError("Provider Acceptance observation links are invalid")
        expected_evidence = tuple(
            IbkrNautilusPaperScenarioEvidence(
                scenario=item.scenario,
                evidence_hash=canonical_hash(
                    {
                        "observation_id": item.observation_id,
                        "artifact_hash": item.artifact_hash,
                        "result_hash": item.result_hash,
                    }
                ),
                passed=item.passed,
            )
            for item in observations
        )
        scope = (
            acceptance.configuration_hash,
            acceptance.account_reference_hash,
            acceptance.instrument_routes_hash,
            acceptance.markets,
            acceptance.order_types,
            acceptance.time_in_force,
            acceptance.nautilus_ibapi_version,
        )
        if any(
            (
                item.configuration_hash,
                item.account_reference_hash,
                item.instrument_routes_hash,
                item.markets,
                item.order_types,
                item.time_in_force,
                item.nautilus_ibapi_version,
            )
            != scope
            for item in observations
        ):
            raise ValueError("Provider Acceptance observation scope is invalid")
        expected_exclusive = bool(observations) and all(
            item.exclusive_api_client_scope_observed for item in observations
        )
        expected_auto_bind = bool(observations) and all(
            item.manual_order_auto_bind_observed for item in observations
        )
        scenarios = tuple(item.scenario for item in observations)
        expected_complete = (
            not acceptance.gaps
            and frozenset(scenarios) == _REQUIRED_ACCEPTANCE_SCENARIOS
            and len(scenarios) == len(set(scenarios))
            and all(item.passed for item in observations)
            and all(item.effective_client_id == 0 for item in observations)
            and not any(item.client_id_collision for item in observations)
            and expected_auto_bind
            and expected_exclusive
        )
        if (
            acceptance.scenario_evidence != expected_evidence
            or acceptance.exclusive_api_client_scope != expected_exclusive
            or acceptance.manual_order_auto_bind != expected_auto_bind
            or acceptance.complete != expected_complete
        ):
            raise ValueError("Provider Acceptance does not resolve from durable observations")
        return acceptance

    def verify(self, acceptance: IbkrNautilusPaperProviderAcceptance) -> bool:
        try:
            resolved = self.resolve_payload(acceptance.to_dict())
        except (KeyError, OSError, TypeError, ValueError):
            return False
        return resolved == acceptance

    def _connect(self) -> sqlite3.Connection:
        connection = (
            sqlite3.connect(
                f"file:{self._state_path}?mode=ro",
                uri=True,
                timeout=30,
            )
            if self._read_only
            else sqlite3.connect(self._state_path, timeout=30)
        )
        connection.row_factory = sqlite3.Row
        if not self._read_only:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
        return connection


@dataclass(frozen=True, slots=True)
class IbkrNautilusInstrumentRoute:
    nautilus_instrument_id: str
    market: str

    def __post_init__(self) -> None:
        if not self.nautilus_instrument_id or (
            self.nautilus_instrument_id != self.nautilus_instrument_id.strip()
        ):
            raise ValueError("Nautilus instrument identity must be non-empty and trimmed")
        if not self.market or self.market != self.market.strip().upper():
            raise ValueError("instrument market must be an uppercase canonical identifier")


class NautilusPaperRuntimeStatus(StrEnum):
    ACCEPTED = "accepted"
    PENDING_CANCEL = "pending_cancel"
    CANCELED = "canceled"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class NautilusPaperSubmitCommand:
    submission_id: str
    nautilus_client_order_id: str
    instrument_id: str
    side: Side
    quantity: Decimal
    order_kind: OrderKind
    limit_price: Decimal | None
    created_at: datetime
    expires_at: datetime
    connection_generation: int
    scope_observed_at: datetime
    scope_valid_until: datetime
    last_disconnection_ns: int | None = None

    def __post_init__(self) -> None:
        for value in (self.submission_id, self.nautilus_client_order_id, self.instrument_id):
            if not value or value != value.strip():
                raise ValueError("Nautilus submit command strings must be non-empty and trimmed")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("Nautilus submit quantity must be finite and positive")
        if (self.order_kind is OrderKind.LIMIT) != (self.limit_price is not None):
            raise ValueError("Nautilus limit price does not match order kind")
        require_aware(self.created_at, "Nautilus submit created_at")
        require_aware(self.expires_at, "Nautilus submit expires_at")
        require_aware(self.scope_observed_at, "Nautilus submit scope_observed_at")
        require_aware(self.scope_valid_until, "Nautilus submit scope_valid_until")
        if self.connection_generation <= 0:
            raise ValueError("Nautilus submit requires a positive connection generation")
        if self.last_disconnection_ns is not None and (
            isinstance(self.last_disconnection_ns, bool) or self.last_disconnection_ns < 0
        ):
            raise ValueError("Nautilus submit disconnect marker is invalid")
        if self.scope_valid_until <= self.scope_observed_at:
            raise ValueError("Nautilus submit scope validity must be positive")


@dataclass(frozen=True, slots=True)
class NautilusPaperCancelCommand:
    cancellation_id: str
    nautilus_client_order_id: str
    provider_order_id: str
    connection_generation: int
    scope_observed_at: datetime
    scope_valid_until: datetime
    last_disconnection_ns: int | None = None

    def __post_init__(self) -> None:
        for value in (
            self.cancellation_id,
            self.nautilus_client_order_id,
            self.provider_order_id,
        ):
            if not value or value != value.strip():
                raise ValueError("Nautilus cancel command strings must be non-empty and trimmed")
        require_aware(self.scope_observed_at, "Nautilus cancel scope_observed_at")
        require_aware(self.scope_valid_until, "Nautilus cancel scope_valid_until")
        if self.connection_generation <= 0:
            raise ValueError("Nautilus cancel requires a positive connection generation")
        if self.last_disconnection_ns is not None and (
            isinstance(self.last_disconnection_ns, bool) or self.last_disconnection_ns < 0
        ):
            raise ValueError("Nautilus cancel disconnect marker is invalid")
        if self.scope_valid_until <= self.scope_observed_at:
            raise ValueError("Nautilus cancel scope validity must be positive")


@dataclass(frozen=True, slots=True)
class NautilusPaperMutationReference:
    mutation_id: str
    harness_authority_id: str
    mutation_kind: str

    def __post_init__(self) -> None:
        if not self.mutation_id or self.mutation_id != self.mutation_id.strip():
            raise ValueError("Nautilus mutation identity is invalid")
        if not self.harness_authority_id.startswith("harness-authority-"):
            raise ValueError("Nautilus mutation Harness authority is invalid")
        if self.mutation_kind not in {"submit", "cancel"}:
            raise ValueError("Nautilus mutation kind is invalid")


@dataclass(frozen=True, slots=True)
class NautilusPaperOrderObservation:
    nautilus_client_order_id: str
    provider_order_id: str | None
    status: NautilusPaperRuntimeStatus
    observed_at: datetime
    filled_quantity: Decimal = Decimal(0)
    fill_ids: tuple[str, ...] = ()
    external: bool = False

    def __post_init__(self) -> None:
        if not self.nautilus_client_order_id or (
            self.nautilus_client_order_id != self.nautilus_client_order_id.strip()
        ):
            raise ValueError("Nautilus observation client order identity is invalid")
        if self.provider_order_id is not None and (
            not self.provider_order_id or self.provider_order_id != self.provider_order_id.strip()
        ):
            raise ValueError("Nautilus observation provider order identity is invalid")
        require_aware(self.observed_at, "Nautilus observation time")
        if not self.filled_quantity.is_finite() or self.filled_quantity < 0:
            raise ValueError("Nautilus filled quantity must be finite and non-negative")
        if len(set(self.fill_ids)) != len(self.fill_ids) or any(
            not fill_id or fill_id != fill_id.strip() for fill_id in self.fill_ids
        ):
            raise ValueError("Nautilus fill identities must be unique, non-empty strings")
        object.__setattr__(self, "fill_ids", tuple(sorted(self.fill_ids)))
        if (self.filled_quantity > 0) != bool(self.fill_ids):
            raise ValueError("Nautilus fill quantity and identities must be present together")
        if (
            self.status
            in {
                NautilusPaperRuntimeStatus.ACCEPTED,
                NautilusPaperRuntimeStatus.PENDING_CANCEL,
                NautilusPaperRuntimeStatus.CANCELED,
                NautilusPaperRuntimeStatus.PARTIALLY_FILLED,
                NautilusPaperRuntimeStatus.FILLED,
            }
            and self.provider_order_id is None
        ):
            raise ValueError("broker-observed order state requires provider_order_id")


@dataclass(frozen=True, slots=True)
class NautilusPaperCashBalance:
    currency: str
    total: Decimal
    free: Decimal
    locked: Decimal

    def __post_init__(self) -> None:
        if not self.currency or self.currency != self.currency.strip().upper():
            raise ValueError("Nautilus cash currency must be an uppercase identifier")
        if any(not value.is_finite() for value in (self.total, self.free, self.locked)):
            raise ValueError("Nautilus cash values must be finite")
        if self.locked < 0 or self.free + self.locked != self.total:
            raise ValueError("Nautilus cash balance components are inconsistent")


@dataclass(frozen=True, slots=True)
class NautilusPaperPositionObservation:
    instrument_id: str
    signed_quantity: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.instrument_id or self.instrument_id != self.instrument_id.strip():
            raise ValueError("Nautilus position instrument identity is invalid")
        if not self.signed_quantity.is_finite():
            raise ValueError("Nautilus position quantity must be finite")
        require_aware(self.observed_at, "Nautilus position observation time")


@dataclass(frozen=True, slots=True)
class NautilusPaperExecutionObservation:
    fill_id: str
    nautilus_client_order_id: str
    provider_order_id: str
    quantity: Decimal
    price: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        for value in (
            self.fill_id,
            self.nautilus_client_order_id,
            self.provider_order_id,
        ):
            if not value or value != value.strip():
                raise ValueError("Nautilus execution identities must be non-empty and trimmed")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("Nautilus execution quantity must be finite and positive")
        if not self.price.is_finite() or self.price <= 0:
            raise ValueError("Nautilus execution price must be finite and positive")
        require_aware(self.observed_at, "Nautilus execution observation time")


@dataclass(frozen=True, slots=True)
class NautilusPaperRuntimeSnapshot:
    observed_at: datetime
    connected: bool
    reconciled: bool
    complete: bool
    orders: tuple[NautilusPaperOrderObservation, ...]
    cash: tuple[NautilusPaperCashBalance, ...] = ()
    positions: tuple[NautilusPaperPositionObservation, ...] = ()
    executions: tuple[NautilusPaperExecutionObservation, ...] = ()
    cash_complete: bool = False
    positions_complete: bool = False
    orders_complete: bool = False
    executions_complete: bool = False
    external_order_discovery_complete: bool = False
    effective_client_id: int | None = None
    client_id_collision: bool = False
    connection_generation: int = 0
    last_disconnection_ns: int | None = None
    cash_reconciliation_generation: int = 0
    positions_reconciliation_generation: int = 0
    orders_reconciliation_generation: int = 0
    executions_reconciliation_generation: int = 0
    gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "Nautilus runtime snapshot time")
        _sorted_unique(self.gaps, "Nautilus runtime gaps")
        currencies = tuple(item.currency for item in self.cash)
        if currencies != tuple(sorted(set(currencies))):
            raise ValueError("Nautilus cash balances must be sorted and unique by currency")
        position_ids = tuple(item.instrument_id for item in self.positions)
        if position_ids != tuple(sorted(set(position_ids))):
            raise ValueError("Nautilus positions must be sorted and unique by instrument")
        generations = (
            self.connection_generation,
            self.cash_reconciliation_generation,
            self.positions_reconciliation_generation,
            self.orders_reconciliation_generation,
            self.executions_reconciliation_generation,
        )
        if any(value < 0 for value in generations):
            raise ValueError("Nautilus reconciliation generations must be non-negative")
        if self.last_disconnection_ns is not None and (
            isinstance(self.last_disconnection_ns, bool) or self.last_disconnection_ns < 0
        ):
            raise ValueError("Nautilus disconnect marker is invalid")

    @property
    def all_facets_complete(self) -> bool:
        return all(
            (
                self.complete,
                self.cash_complete,
                self.positions_complete,
                self.orders_complete,
                self.executions_complete,
                self.external_order_discovery_complete,
                self.effective_client_id == 0,
                not self.client_id_collision,
                self.connection_generation > 0,
                self.cash_reconciliation_generation == self.connection_generation,
                self.positions_reconciliation_generation == self.connection_generation,
                self.orders_reconciliation_generation == self.connection_generation,
                self.executions_reconciliation_generation == self.connection_generation,
            )
        )


class NautilusPaperExecutionRuntime(Protocol):
    @property
    def runtime_version(self) -> str: ...

    @property
    def nautilus_version(self) -> str: ...

    @property
    def nautilus_ibapi_version(self) -> str: ...

    @property
    def configuration_hash(self) -> str: ...

    @property
    def account_reference_hash(self) -> str: ...

    @property
    def acceptance_authority_id(self) -> str: ...

    @property
    def time_in_force(self) -> str: ...

    @property
    def session_scope_valid(self) -> bool: ...

    @property
    def activation_runtime_active(self) -> bool: ...

    @property
    def session_scope_generation(self) -> int | None: ...

    @property
    def session_scope_observed_at(self) -> datetime | None: ...

    @property
    def session_scope_last_disconnection_ns(self) -> int | None: ...

    @property
    def session_scope_ttl_seconds(self) -> float: ...

    def submit(
        self, reference: NautilusPaperMutationReference
    ) -> NautilusPaperOrderObservation: ...

    def cancel(
        self, reference: NautilusPaperMutationReference
    ) -> NautilusPaperOrderObservation: ...

    def reconcile(self) -> NautilusPaperRuntimeSnapshot: ...

    def bind_canonical_activation(
        self,
        store: LocalDataSnapshotStore,
        *,
        acceptance_id: str,
        head_id: str,
    ) -> None: ...


def _record_ibkr_nautilus_paper_activation(  # pyright: ignore[reportUnusedFunction]
    *,
    store: LocalDataSnapshotStore,
    authority: IbkrNautilusPaperAcceptanceAuthority,
    acceptance: IbkrNautilusPaperProviderAcceptance,
    runtime: NautilusPaperExecutionRuntime,
    instrument_routes: Mapping[str, IbkrNautilusInstrumentRoute],
    activation_valid_until: datetime,
) -> str:
    """Persist one dependency-closed activation head in the canonical Harness root."""

    require_aware(activation_valid_until, "activation valid_until")
    if authority._read_only:  # pyright: ignore[reportPrivateUsage]
        raise PermissionError("read-only acceptance authority cannot record activation")
    expected_evidence_path = (store.root / "ibkr-nautilus-paper-acceptance.sqlite3").resolve()
    if authority._state_path != expected_evidence_path:  # pyright: ignore[reportPrivateUsage]
        raise PermissionError("acceptance evidence store is outside the canonical Harness root")
    try:
        with store.authority_transaction() as connection:
            registered_authority = connection.execute(
                """
                SELECT evidence_authority_id
                FROM ibkr_nautilus_acceptance_authority
                WHERE singleton = 1 AND harness_authority_id = ?
                """,
                (store.harness_authority_id,),
            ).fetchone()
    except sqlite3.OperationalError as error:
        raise PermissionError(
            "IBKR acceptance runner authority is not registered in this Harness root"
        ) from error
    if (
        registered_authority is None
        or cast(str, registered_authority["evidence_authority_id"]) != authority.authority_id
    ):
        raise PermissionError(
            "IBKR acceptance runner authority is not registered in this Harness root"
        )
    verifier = authority.verifier()
    if not verifier.verify(acceptance):
        raise PermissionError("canonical acceptance evidence cannot be reopened")
    routes = dict(instrument_routes)
    routes_hash = hash_ibkr_nautilus_instrument_routes(routes)
    if not (
        acceptance.execution_accepted
        and acceptance.configuration_hash == runtime.configuration_hash
        and acceptance.account_reference_hash == runtime.account_reference_hash
        and acceptance.instrument_routes_hash == routes_hash
        and acceptance.runtime_version == runtime.runtime_version
        and acceptance.nautilus_version == runtime.nautilus_version
        and acceptance.nautilus_ibapi_version == runtime.nautilus_ibapi_version
        and acceptance.time_in_force == (runtime.time_in_force,)
        and verifier.authority_id == runtime.acceptance_authority_id
        and runtime.session_scope_valid
        and runtime.activation_runtime_active
        and activation_valid_until > acceptance.accepted_at
    ):
        raise PermissionError("canonical activation scope is not accepted")
    authority_id = store.harness_authority_id
    runtime_payload = {
        "runtime_version": runtime.runtime_version,
        "nautilus_version": runtime.nautilus_version,
        "nautilus_ibapi_version": runtime.nautilus_ibapi_version,
        "configuration_hash": runtime.configuration_hash,
        "account_reference_hash": runtime.account_reference_hash,
        "acceptance_authority_id": runtime.acceptance_authority_id,
        "time_in_force": runtime.time_in_force,
    }
    runtime_registration_id = "ibkr-nautilus-runtime-registration-" + canonical_hash(
        {"harness_authority_id": authority_id, **runtime_payload}
    )
    route_payload = {
        key: {
            "nautilus_instrument_id": route.nautilus_instrument_id,
            "market": route.market,
        }
        for key, route in sorted(routes.items())
    }
    route_registration_id = "ibkr-nautilus-route-registration-" + canonical_hash(
        {
            "harness_authority_id": authority_id,
            "instrument_routes_hash": routes_hash,
            "routes": route_payload,
        }
    )
    head_core = {
        "harness_authority_id": authority_id,
        "acceptance_id": acceptance.acceptance_id,
        "evidence_authority_id": acceptance.authority_id,
        "runtime_registration_id": runtime_registration_id,
        "route_registration_id": route_registration_id,
        "activation_valid_until": _timestamp(activation_valid_until),
    }
    head_id = "ibkr-nautilus-activation-head-" + canonical_hash(head_core)
    with store.authority_transaction() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS ibkr_nautilus_runtime_registrations (
                registration_id TEXT PRIMARY KEY,
                harness_authority_id TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ibkr_nautilus_route_registrations (
                registration_id TEXT PRIMARY KEY,
                harness_authority_id TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ibkr_nautilus_accepted_evidence (
                acceptance_id TEXT PRIMARY KEY,
                harness_authority_id TEXT NOT NULL,
                acceptance_payload_json TEXT NOT NULL,
                evidence_state_path TEXT NOT NULL,
                runner_id TEXT NOT NULL,
                verification_key BLOB NOT NULL,
                runtime_registration_id TEXT NOT NULL,
                route_registration_id TEXT NOT NULL,
                activation_head_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ibkr_nautilus_activation_head (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                harness_authority_id TEXT NOT NULL,
                head_id TEXT NOT NULL UNIQUE,
                acceptance_id TEXT NOT NULL,
                activation_valid_until TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ibkr_nautilus_mutation_outbox (
                mutation_id TEXT PRIMARY KEY,
                harness_authority_id TEXT NOT NULL,
                activation_head_id TEXT NOT NULL,
                acceptance_id TEXT NOT NULL,
                mutation_kind TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT OR REPLACE INTO ibkr_nautilus_runtime_registrations VALUES (?, ?, ?)",
            (runtime_registration_id, authority_id, _canonical_json(runtime_payload)),
        )
        connection.execute(
            "INSERT OR REPLACE INTO ibkr_nautilus_route_registrations VALUES (?, ?, ?)",
            (route_registration_id, authority_id, _canonical_json(route_payload)),
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO ibkr_nautilus_accepted_evidence
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                acceptance.acceptance_id,
                authority_id,
                _canonical_json(acceptance.to_dict()),
                expected_evidence_path.as_posix(),
                authority._runner_id,  # pyright: ignore[reportPrivateUsage]
                authority._verification_key,  # pyright: ignore[reportPrivateUsage]
                runtime_registration_id,
                route_registration_id,
                head_id,
            ),
        )
        connection.execute(
            "INSERT OR REPLACE INTO ibkr_nautilus_activation_head VALUES (1, ?, ?, ?, ?)",
            (authority_id, head_id, acceptance.acceptance_id, _timestamp(activation_valid_until)),
        )
    _CANONICAL_RUNTIME_HANDLES[(authority_id, runtime_registration_id)] = runtime
    runtime.bind_canonical_activation(
        store,
        acceptance_id=acceptance.acceptance_id,
        head_id=head_id,
    )
    return head_id


def issue_ibkr_nautilus_paper_provider_from_harness_state(
    *,
    canonical_store: LocalDataSnapshotStore,
    accepted_evidence_content_id: str,
) -> IbkrNautilusPaperExecutionProvider:
    """Reopen one exact Provider exclusively from canonical Harness-owned state."""

    if not accepted_evidence_content_id.startswith("ibkr-nautilus-paper-acceptance-"):
        raise ValueError("IBKR Paper activation requires an accepted evidence content identity")
    authority_id = canonical_store.harness_authority_id
    try:
        with canonical_store.authority_transaction() as connection:
            head = connection.execute(
                """
                SELECT head_id, acceptance_id, activation_valid_until
                FROM ibkr_nautilus_activation_head
                WHERE singleton = 1 AND harness_authority_id = ? AND acceptance_id = ?
                """,
                (authority_id, accepted_evidence_content_id),
            ).fetchone()
            evidence = connection.execute(
                """
                SELECT * FROM ibkr_nautilus_accepted_evidence
                WHERE acceptance_id = ? AND harness_authority_id = ?
                """,
                (accepted_evidence_content_id, authority_id),
            ).fetchone()
            if (
                head is None
                or evidence is None
                or head["head_id"] != evidence["activation_head_id"]
            ):
                raise PermissionError("canonical Harness activation head is missing")
            runtime_row = connection.execute(
                """
                SELECT payload_json FROM ibkr_nautilus_runtime_registrations
                WHERE registration_id = ? AND harness_authority_id = ?
                """,
                (evidence["runtime_registration_id"], authority_id),
            ).fetchone()
            route_row = connection.execute(
                """
                SELECT payload_json FROM ibkr_nautilus_route_registrations
                WHERE registration_id = ? AND harness_authority_id = ?
                """,
                (evidence["route_registration_id"], authority_id),
            ).fetchone()
    except sqlite3.OperationalError as error:
        raise PermissionError("canonical Harness activation head is missing") from error
    if runtime_row is None or route_row is None:
        raise PermissionError("canonical runtime or route registration is missing")
    runtime_registration_id = cast(str, evidence["runtime_registration_id"])
    runtime = _CANONICAL_RUNTIME_HANDLES.get((authority_id, runtime_registration_id))
    if runtime is None:
        raise PermissionError("registered IBKR Nautilus runtime is not active in this process")
    verifier = IbkrNautilusPaperAcceptanceVerifier(
        _state_path=Path(cast(str, evidence["evidence_state_path"])),
        _runner_id=cast(str, evidence["runner_id"]),
        _verification_key=bytes(evidence["verification_key"]),
        _seal=_VERIFIER_SEAL,
    )
    acceptance_payload = json.loads(cast(str, evidence["acceptance_payload_json"]))
    acceptance = verifier.resolve_payload(acceptance_payload)
    runtime_payload = json.loads(cast(str, runtime_row["payload_json"]))
    route_payload = json.loads(cast(str, route_row["payload_json"]))
    if not isinstance(runtime_payload, dict) or not isinstance(route_payload, dict):
        raise TypeError("canonical IBKR activation registration is invalid")
    routes = {
        key: IbkrNautilusInstrumentRoute(
            nautilus_instrument_id=cast(str, value["nautilus_instrument_id"]),
            market=cast(str, value["market"]),
        )
        for key, value in cast(dict[str, dict[str, object]], route_payload).items()
    }
    provider = IbkrNautilusPaperExecutionProvider(
        canonical_store.root / "ibkr-nautilus-paper-provider.sqlite3",
        runtime=runtime,
        instrument_routes=routes,
        acceptance=acceptance,
        _acceptance_verifier=verifier,
        _factory_seal=_PROVIDER_FACTORY_SEAL,
        _activation_store=canonical_store,
        _activation_head_id=cast(str, head["head_id"]),
    )
    return provider


def _reopen_canonical_activation(
    store: LocalDataSnapshotStore,
    *,
    acceptance_id: str,
    head_id: str,
    now: datetime,
) -> tuple[IbkrNautilusPaperProviderAcceptance, dict[str, object], dict[str, object]]:
    require_aware(now, "canonical activation reopen time")
    authority_id = store.harness_authority_id
    with store.authority_transaction() as connection:
        head = connection.execute(
            """
            SELECT activation_valid_until FROM ibkr_nautilus_activation_head
            WHERE singleton = 1 AND harness_authority_id = ?
              AND head_id = ? AND acceptance_id = ?
            """,
            (authority_id, head_id, acceptance_id),
        ).fetchone()
        evidence = connection.execute(
            """
            SELECT * FROM ibkr_nautilus_accepted_evidence
            WHERE acceptance_id = ? AND harness_authority_id = ? AND activation_head_id = ?
            """,
            (acceptance_id, authority_id, head_id),
        ).fetchone()
        if head is None or evidence is None:
            raise PermissionError("canonical Harness activation head is missing")
        runtime_row = connection.execute(
            """
            SELECT payload_json FROM ibkr_nautilus_runtime_registrations
            WHERE registration_id = ? AND harness_authority_id = ?
            """,
            (evidence["runtime_registration_id"], authority_id),
        ).fetchone()
        route_row = connection.execute(
            """
            SELECT payload_json FROM ibkr_nautilus_route_registrations
            WHERE registration_id = ? AND harness_authority_id = ?
            """,
            (evidence["route_registration_id"], authority_id),
        ).fetchone()
    if runtime_row is None or route_row is None:
        raise PermissionError("canonical runtime or route registration is missing")
    if now >= _datetime(cast(str, head["activation_valid_until"])):
        raise PermissionError("canonical Harness activation has expired")
    verifier = IbkrNautilusPaperAcceptanceVerifier(
        _state_path=Path(cast(str, evidence["evidence_state_path"])),
        _runner_id=cast(str, evidence["runner_id"]),
        _verification_key=bytes(evidence["verification_key"]),
        _seal=_VERIFIER_SEAL,
    )
    raw_acceptance = json.loads(cast(str, evidence["acceptance_payload_json"]))
    acceptance = verifier.resolve_payload(raw_acceptance)
    runtime_payload = json.loads(cast(str, runtime_row["payload_json"]))
    route_payload = json.loads(cast(str, route_row["payload_json"]))
    if not isinstance(runtime_payload, dict) or not isinstance(route_payload, dict):
        raise TypeError("canonical IBKR activation registration is invalid")
    return (
        acceptance,
        cast(dict[str, object], runtime_payload),
        cast(dict[str, object], route_payload),
    )


class IbkrNautilusPaperExecutionProvider:
    """Fail-closed Harness adapter over one long-lived Nautilus-to-IBKR Paper runtime."""

    def __init__(
        self,
        state_path: Path,
        *,
        runtime: NautilusPaperExecutionRuntime,
        instrument_routes: Mapping[str, IbkrNautilusInstrumentRoute],
        acceptance: IbkrNautilusPaperProviderAcceptance | None = None,
        _acceptance_verifier: IbkrNautilusPaperAcceptanceVerifier,
        _factory_seal: object,
        _activation_store: LocalDataSnapshotStore | None = None,
        _activation_head_id: str | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if _factory_seal is not _PROVIDER_FACTORY_SEAL:
            raise TypeError("IBKR Nautilus Provider must be built by Harness activation authority")
        routes = dict(instrument_routes)
        if not routes or any(not key or key != key.strip() for key in routes):
            raise ValueError("IBKR Nautilus instrument routes must use explicit Harness IDs")
        if len({route.nautilus_instrument_id for route in routes.values()}) != len(routes):
            raise ValueError("IBKR Nautilus instrument identities must be one-to-one")
        self._state_path = state_path.resolve()
        self._state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._runtime = runtime
        if _HASH.fullmatch(runtime.configuration_hash) is None:
            raise ValueError("Nautilus runtime configuration_hash must be a SHA-256 hash")
        if _ACCOUNT_REFERENCE_HASH.fullmatch(runtime.account_reference_hash) is None:
            raise ValueError("Nautilus runtime account reference must be opaque")
        self._instrument_routes = MappingProxyType(routes)
        self._instrument_routes_hash = hash_ibkr_nautilus_instrument_routes(routes)
        self._acceptance = acceptance
        self._acceptance_verifier = _acceptance_verifier
        self._activation_store = _activation_store
        self._activation_head_id = _activation_head_id
        self._clock = clock or (lambda: datetime.now(UTC))
        self._submission_validator: Callable[[SubmissionCapability], bool] | None = None
        self._cancellation_validator: Callable[[CancellationCapability], bool] | None = None
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_order_bindings (
                    client_order_id TEXT PRIMARY KEY,
                    order_hash TEXT NOT NULL,
                    submission_id TEXT NOT NULL UNIQUE,
                    nautilus_client_order_id TEXT NOT NULL UNIQUE,
                    acceptance_id TEXT NOT NULL,
                    configuration_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    instrument_routes_hash TEXT NOT NULL,
                    market TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    provider_order_id TEXT,
                    dispatch_state TEXT NOT NULL,
                    accepted_at TEXT
                );
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_cancel_bindings (
                    cancellation_id TEXT PRIMARY KEY,
                    client_order_id TEXT NOT NULL,
                    provider_order_id TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    acceptance_id TEXT NOT NULL,
                    configuration_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    instrument_routes_hash TEXT NOT NULL,
                    dispatch_state TEXT NOT NULL,
                    observed_at TEXT
                );
                """
            )
        os.chmod(self._state_path, 0o600)

    @property
    def manifest(self) -> ProviderManifest:
        now = self._clock()
        require_aware(now, "provider manifest evaluation time")
        operational = (
            self._acceptance is not None
            and self._acceptance.allows_risk_reduction(now)
            and self._acceptance_scope_matches(self._acceptance)
        )
        acceptance = self._acceptance
        markets = ("US", "HK") if acceptance is None else acceptance.markets
        order_types = ("market", "limit") if acceptance is None else acceptance.order_types
        return ProviderManifest(
            schema_version="market-impact.provider-manifest.v1",
            provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
            transport=ProviderTransport.NATIVE,
            environments=frozenset({TradingEnvironment.PAPER}),
            declared_capabilities=frozenset({Capability.PAPER_EXECUTION}),
            verified_capabilities=(
                frozenset({Capability.PAPER_EXECUTION}) if operational else frozenset()
            ),
            markets=markets,
            order_types=order_types,
            supports_streaming=True,
            supports_reconciliation=True,
            enabled=operational,
            trust_tier=TrustTier.PAPER_VALIDATED if operational else TrustTier.UNVERIFIED,
        )

    @property
    def new_order_admission_open(self) -> bool:
        now = self._clock()
        require_aware(now, "provider new-order admission evaluation time")
        return (
            self._acceptance is not None
            and self._acceptance.is_current(now)
            and self._acceptance_scope_matches(self._acceptance)
        )

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        if self._submission_validator is None:
            self._submission_validator = validator

    def bind_cancellation_validator(
        self,
        validator: Callable[[CancellationCapability], bool],
    ) -> None:
        if self._cancellation_validator is None:
            self._cancellation_validator = validator

    def _runtime_scope_command_fields(self) -> tuple[int, datetime, datetime, int | None]:
        generation = self._runtime.session_scope_generation
        observed_at = self._runtime.session_scope_observed_at
        if generation is None or observed_at is None:
            raise RuntimeError("Nautilus mutation requires a reconciled runtime generation")
        valid_until = observed_at + timedelta(seconds=self._runtime.session_scope_ttl_seconds)
        now = self._clock()
        require_aware(now, "Nautilus mutation preparation time")
        if not observed_at <= now < valid_until or not self._runtime.session_scope_valid:
            raise RuntimeError("Nautilus mutation requires fresh reconciled runtime scope")
        return (
            generation,
            observed_at,
            valid_until,
            self._runtime.session_scope_last_disconnection_ns,
        )

    def _dispatch_submission(
        self,
        command: NautilusPaperSubmitCommand,
    ) -> NautilusPaperOrderObservation:
        reference = self._record_canonical_mutation("submit", command)
        return self._runtime.submit(reference)

    def _dispatch_cancellation(
        self,
        command: NautilusPaperCancelCommand,
    ) -> NautilusPaperOrderObservation:
        reference = self._record_canonical_mutation("cancel", command)
        return self._runtime.cancel(reference)

    def _record_canonical_mutation(
        self,
        mutation_kind: str,
        command: NautilusPaperSubmitCommand | NautilusPaperCancelCommand,
    ) -> NautilusPaperMutationReference:
        store = self._activation_store
        head_id = self._activation_head_id
        acceptance = self._acceptance
        if store is None or head_id is None or acceptance is None:
            raise RuntimeError("Nautilus mutation lacks canonical activation state")
        payload = _nautilus_mutation_payload(command)
        core = {
            "harness_authority_id": store.harness_authority_id,
            "activation_head_id": head_id,
            "acceptance_id": acceptance.acceptance_id,
            "mutation_kind": mutation_kind,
            "payload": payload,
        }
        mutation_id = "ibkr-nautilus-mutation-" + canonical_hash(core)
        with store.authority_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO ibkr_nautilus_mutation_outbox(
                    mutation_id, harness_authority_id, activation_head_id,
                    acceptance_id, mutation_kind, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    mutation_id,
                    store.harness_authority_id,
                    head_id,
                    acceptance.acceptance_id,
                    mutation_kind,
                    _canonical_json(payload),
                ),
            )
        return NautilusPaperMutationReference(
            mutation_id=mutation_id,
            harness_authority_id=store.harness_authority_id,
            mutation_kind=mutation_kind,
        )

    def submit(self, capability: SubmissionCapability) -> ExecutionReceipt:
        self._validate_submission(capability)
        existing, should_dispatch = self._prepare_submission(capability)
        if not should_dispatch:
            provider_order_id = cast(str | None, existing["provider_order_id"])
            accepted_at = cast(str | None, existing["accepted_at"])
            if (
                cast(str, existing["dispatch_state"]) == "accepted"
                and provider_order_id is not None
                and accepted_at is not None
            ):
                return ExecutionReceipt(
                    client_order_id=capability.order.client_order_id,
                    provider_order_id=provider_order_id,
                    status=ExecutionStatus.ACCEPTED,
                    observed_at=_datetime(accepted_at),
                )
            raise RuntimeError("earlier Nautilus submission outcome is ambiguous; reconcile only")
        (
            generation,
            scope_observed_at,
            scope_valid_until,
            last_disconnection_ns,
        ) = self._runtime_scope_command_fields()
        observation = self._dispatch_submission(
            NautilusPaperSubmitCommand(
                submission_id=capability.submission_id,
                nautilus_client_order_id=cast(str, existing["nautilus_client_order_id"]),
                instrument_id=self._instrument_routes[
                    capability.order.instrument_id
                ].nautilus_instrument_id,
                side=capability.order.side,
                quantity=capability.order.quantity,
                order_kind=capability.order.order_kind,
                limit_price=capability.order.limit_price,
                created_at=capability.order.created_at,
                expires_at=capability.order.expires_at,
                connection_generation=generation,
                scope_observed_at=scope_observed_at,
                scope_valid_until=scope_valid_until,
                last_disconnection_ns=last_disconnection_ns,
            )
        )
        if observation.nautilus_client_order_id != cast(str, existing["nautilus_client_order_id"]):
            raise RuntimeError("Nautilus submission observation identity mismatch")
        if (
            observation.status is not NautilusPaperRuntimeStatus.ACCEPTED
            or observation.provider_order_id is None
            or observation.filled_quantity != 0
            or observation.fill_ids
        ):
            raise RuntimeError("Nautilus submission did not produce an accepted-without-fill event")
        self._record_submission_acceptance(
            capability.order.client_order_id,
            provider_order_id=observation.provider_order_id,
            observed_at=observation.observed_at,
        )
        return ExecutionReceipt(
            client_order_id=capability.order.client_order_id,
            provider_order_id=observation.provider_order_id,
            status=ExecutionStatus.ACCEPTED,
            observed_at=observation.observed_at,
        )

    def cancel(self, capability: CancellationCapability) -> CancellationCommandReceipt:
        self._validate_cancellation(capability)
        binding = self._order_binding(capability.client_order_id)
        if (
            binding is None
            or not self._binding_scope_matches(binding)
            or cast(str | None, binding["provider_order_id"]) != capability.provider_order_id
        ):
            raise CancellationCapabilityRejected("cancellation target is not an exact known order")
        existing, should_dispatch = self._prepare_cancellation(capability)
        if not should_dispatch:
            state = cast(str, existing["dispatch_state"])
            observed_at = cast(str | None, existing["observed_at"])
            if state in {"dispatched", "canceled"} and observed_at is not None:
                return CancellationCommandReceipt(
                    client_order_id=capability.client_order_id,
                    provider_order_id=capability.provider_order_id,
                    cancellation_id=capability.cancellation_id,
                    status=(
                        CancellationCommandStatus.CANCELED
                        if state == "canceled"
                        else CancellationCommandStatus.DISPATCHED
                    ),
                    observed_at=_datetime(observed_at),
                )
            raise RuntimeError("earlier Nautilus cancellation outcome is ambiguous; reconcile only")
        (
            generation,
            scope_observed_at,
            scope_valid_until,
            last_disconnection_ns,
        ) = self._runtime_scope_command_fields()
        observation = self._dispatch_cancellation(
            NautilusPaperCancelCommand(
                cancellation_id=capability.cancellation_id,
                nautilus_client_order_id=cast(str, binding["nautilus_client_order_id"]),
                provider_order_id=capability.provider_order_id,
                connection_generation=generation,
                scope_observed_at=scope_observed_at,
                scope_valid_until=scope_valid_until,
                last_disconnection_ns=last_disconnection_ns,
            )
        )
        if (
            observation.nautilus_client_order_id != cast(str, binding["nautilus_client_order_id"])
            or observation.provider_order_id != capability.provider_order_id
        ):
            raise RuntimeError("Nautilus cancellation observation identity mismatch")
        if observation.status not in {
            NautilusPaperRuntimeStatus.PENDING_CANCEL,
            NautilusPaperRuntimeStatus.CANCELED,
        }:
            raise RuntimeError("Nautilus cancellation did not produce a definitive command event")
        status = (
            CancellationCommandStatus.CANCELED
            if observation.status is NautilusPaperRuntimeStatus.CANCELED
            else CancellationCommandStatus.DISPATCHED
        )
        self._record_cancellation(
            capability.cancellation_id,
            status=status,
            observed_at=observation.observed_at,
        )
        return CancellationCommandReceipt(
            client_order_id=capability.client_order_id,
            provider_order_id=capability.provider_order_id,
            cancellation_id=capability.cancellation_id,
            status=status,
            observed_at=observation.observed_at,
        )

    def replace(
        self,
        *,
        cancellation: CancellationCapability,
        replacement: SubmissionCapability,
    ) -> ExecutionReceipt:
        """Safely replace via durable cancel, exact reconciliation, then a new submission."""

        if cancellation.client_order_id == replacement.order.client_order_id:
            raise ValueError("replacement must use a new Harness client order identity")
        try:
            self.cancel(cancellation)
        except RuntimeError as error:
            if str(error) != "earlier Nautilus cancellation outcome is ambiguous; reconcile only":
                raise
        snapshot = self.reconcile()
        canceled = next(
            (
                receipt
                for receipt in snapshot.receipts
                if receipt.client_order_id == cancellation.client_order_id
            ),
            None,
        )
        if (
            not snapshot.complete
            or canceled is None
            or canceled.provider_order_id != cancellation.provider_order_id
            or canceled.status is not ExecutionStatus.CANCELED
        ):
            raise RuntimeError(
                "replacement remains blocked until exact cancellation reconciliation completes"
            )
        return self.submit(replacement)

    def reconcile(self) -> ReconciliationSnapshot:
        runtime = self._runtime.reconcile()
        gaps = list(runtime.gaps)
        if not runtime.connected:
            gaps.append("nautilus_runtime_disconnected")
        if not runtime.reconciled:
            gaps.append("nautilus_execution_not_reconciled")
        for complete, gap in (
            (runtime.cash_complete, "nautilus_cash_reconciliation_incomplete"),
            (runtime.positions_complete, "nautilus_position_reconciliation_incomplete"),
            (runtime.orders_complete, "nautilus_order_reconciliation_incomplete"),
            (runtime.executions_complete, "nautilus_execution_reconciliation_incomplete"),
            (
                runtime.external_order_discovery_complete,
                "nautilus_external_order_discovery_incomplete",
            ),
        ):
            if not complete:
                gaps.append(gap)
        bindings = self._all_order_bindings()
        by_nautilus_id = {cast(str, row["nautilus_client_order_id"]): row for row in bindings}
        observations: dict[str, NautilusPaperOrderObservation] = {}
        for observation in runtime.orders:
            if observation.nautilus_client_order_id in observations:
                gaps.append(
                    "duplicate_nautilus_order:"
                    + canonical_hash(observation.nautilus_client_order_id)[:12]
                )
            observations[observation.nautilus_client_order_id] = observation
        receipts: list[ExecutionReceipt] = []
        for nautilus_id, observation in sorted(observations.items()):
            binding = by_nautilus_id.get(nautilus_id)
            if binding is None:
                gaps.append("external_nautilus_order:" + canonical_hash(nautilus_id)[:12])
                continue
            client_order_id = cast(str, binding["client_order_id"])
            if not self._binding_scope_matches(binding):
                gaps.append(f"order_runtime_scope_mismatch:{client_order_id}")
                continue
            expected_provider_id = cast(str | None, binding["provider_order_id"])
            status = _provider_execution_status(observation.status)
            if expected_provider_id is not None and observation.provider_order_id is None:
                gaps.append(f"provider_order_identity_missing:{client_order_id}")
                continue
            if expected_provider_id is not None and (
                observation.provider_order_id != expected_provider_id
            ):
                gaps.append(f"provider_order_identity_mismatch:{client_order_id}")
                continue
            if observation.provider_order_id is not None:
                self._bind_provider_order_id(
                    client_order_id,
                    provider_order_id=observation.provider_order_id,
                    observed_at=observation.observed_at,
                )
                if status is ExecutionStatus.CANCELED:
                    self._reconcile_cancellation_terminal(
                        client_order_id,
                        provider_order_id=observation.provider_order_id,
                        observed_at=observation.observed_at,
                    )
            if status is ExecutionStatus.UNKNOWN:
                gaps.append(
                    f"unsupported_nautilus_order_status:{client_order_id}:{observation.status.value}"
                )
            receipts.append(
                ExecutionReceipt(
                    client_order_id=client_order_id,
                    provider_order_id=observation.provider_order_id,
                    status=status,
                    observed_at=observation.observed_at,
                    filled_quantity=observation.filled_quantity,
                    fill_ids=observation.fill_ids,
                )
            )
        if runtime.complete:
            for row in bindings:
                nautilus_id = cast(str, row["nautilus_client_order_id"])
                if (
                    nautilus_id not in observations
                    and cast(str, row["dispatch_state"]) == "accepted"
                ):
                    gaps.append(f"accepted_nautilus_order_missing:{row['client_order_id']}")
        return ReconciliationSnapshot.build(
            provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            observed_at=runtime.observed_at,
            complete=runtime.all_facets_complete and not gaps,
            receipts=tuple(sorted(receipts, key=lambda item: item.client_order_id)),
            gaps=tuple(sorted(set(gaps))),
        )

    def _validate_submission(self, capability: object) -> None:
        if not isinstance(capability, SubmissionCapability):
            raise TypeError("provider submission requires a Harness-issued capability")
        if not self.new_order_admission_open:
            raise SubmissionCapabilityRejected("external Paper Provider lacks current acceptance")
        if self._submission_validator is None or not self._submission_validator(capability):
            raise SubmissionCapabilityRejected(
                "provider submission is not bound to an active durable outbox lease"
            )
        if (
            capability.provider_id != IBKR_NAUTILUS_PAPER_PROVIDER_ID
            or capability.provider_version != IBKR_NAUTILUS_PAPER_PROVIDER_VERSION
        ):
            raise SubmissionCapabilityRejected("submission capability targets another Provider")
        if capability.order.environment is not TradingEnvironment.PAPER:
            raise SubmissionCapabilityRejected("IBKR Nautilus Provider accepts Paper orders only")
        route = self._instrument_routes.get(capability.order.instrument_id)
        if route is None:
            raise SubmissionCapabilityRejected(
                "order has no accepted Instrument Master translation"
            )
        acceptance = self._acceptance
        if acceptance is None:  # pragma: no cover - manifest.enabled already rejects
            raise SubmissionCapabilityRejected("external Paper Provider lacks acceptance")
        if route.market not in acceptance.markets:
            raise SubmissionCapabilityRejected("order market is outside accepted Provider scope")
        if capability.order.order_kind.value not in acceptance.order_types:
            raise SubmissionCapabilityRejected("order type is outside accepted Provider scope")

    def _validate_cancellation(self, capability: object) -> None:
        if not isinstance(capability, CancellationCapability):
            raise TypeError("provider cancellation requires a Harness-issued capability")
        now = self._clock()
        require_aware(now, "provider cancellation evaluation time")
        acceptance = self._acceptance
        if (
            acceptance is None
            or not acceptance.allows_risk_reduction(now)
            or not self._acceptance_scope_matches(acceptance)
        ):
            raise CancellationCapabilityRejected(
                "external Paper Provider lacks exact-scope risk-reduction acceptance"
            )
        if self._cancellation_validator is None or not self._cancellation_validator(capability):
            raise CancellationCapabilityRejected(
                "provider cancellation is not bound to an active durable outbox lease"
            )
        if (
            capability.provider_id != IBKR_NAUTILUS_PAPER_PROVIDER_ID
            or capability.provider_version != IBKR_NAUTILUS_PAPER_PROVIDER_VERSION
        ):
            raise CancellationCapabilityRejected("cancellation targets another Provider")

    def _prepare_submission(
        self,
        capability: SubmissionCapability,
    ) -> tuple[sqlite3.Row, bool]:
        nautilus_id = _nautilus_client_order_id(capability)
        acceptance = self._acceptance
        route = self._instrument_routes[capability.order.instrument_id]
        if acceptance is None:  # pragma: no cover - validated before this method
            raise RuntimeError("submission acceptance disappeared")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings WHERE client_order_id = ?",
                (capability.order.client_order_id,),
            ).fetchone()
            if row is not None:
                if (
                    cast(str, row["order_hash"]) != capability.order_hash
                    or cast(str, row["nautilus_client_order_id"]) != nautilus_id
                    or cast(str, row["acceptance_id"]) != acceptance.acceptance_id
                    or not self._binding_scope_matches(row)
                    or cast(str, row["market"]) != route.market
                    or cast(str, row["order_type"]) != capability.order.order_kind.value
                ):
                    raise ValueError("IBKR Nautilus order identity or runtime scope conflict")
                return row, False
            connection.execute(
                """
                INSERT INTO ibkr_nautilus_order_bindings (
                    client_order_id, order_hash, submission_id,
                    nautilus_client_order_id, acceptance_id,
                    configuration_hash, account_reference_hash,
                    instrument_routes_hash, market, order_type, provider_order_id,
                    dispatch_state, accepted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'prepared', NULL)
                """,
                (
                    capability.order.client_order_id,
                    capability.order_hash,
                    capability.submission_id,
                    nautilus_id,
                    acceptance.acceptance_id,
                    self._runtime.configuration_hash,
                    self._runtime.account_reference_hash,
                    self._instrument_routes_hash,
                    route.market,
                    capability.order.order_kind.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings WHERE client_order_id = ?",
                (capability.order.client_order_id,),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite insert/read invariant
                raise RuntimeError("IBKR Nautilus order binding was not stored")
            return row, True

    def _prepare_cancellation(
        self,
        capability: CancellationCapability,
    ) -> tuple[sqlite3.Row, bool]:
        acceptance = self._acceptance
        if acceptance is None:  # pragma: no cover - validated before this method
            raise RuntimeError("cancellation acceptance disappeared")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_cancel_bindings WHERE cancellation_id = ?",
                (capability.cancellation_id,),
            ).fetchone()
            if row is not None:
                if (
                    cast(str, row["client_order_id"]) != capability.client_order_id
                    or cast(str, row["provider_order_id"]) != capability.provider_order_id
                    or cast(str, row["request_hash"]) != capability.request_hash
                    or not self._binding_scope_matches(row)
                ):
                    raise ValueError(
                        "IBKR Nautilus cancellation identity or runtime scope conflict"
                    )
                return row, False
            connection.execute(
                """
                INSERT INTO ibkr_nautilus_cancel_bindings (
                    cancellation_id, client_order_id, provider_order_id,
                    request_hash, acceptance_id, configuration_hash,
                    account_reference_hash, instrument_routes_hash,
                    dispatch_state, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'prepared', NULL)
                """,
                (
                    capability.cancellation_id,
                    capability.client_order_id,
                    capability.provider_order_id,
                    capability.request_hash,
                    acceptance.acceptance_id,
                    self._runtime.configuration_hash,
                    self._runtime.account_reference_hash,
                    self._instrument_routes_hash,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_cancel_bindings WHERE cancellation_id = ?",
                (capability.cancellation_id,),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite insert/read invariant
                raise RuntimeError("IBKR Nautilus cancellation binding was not stored")
            return row, True

    def _record_submission_acceptance(
        self,
        client_order_id: str,
        *,
        provider_order_id: str,
        observed_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE ibkr_nautilus_order_bindings
                SET provider_order_id = ?, dispatch_state = 'accepted', accepted_at = ?
                WHERE client_order_id = ? AND dispatch_state = 'prepared'
                """,
                (provider_order_id, _timestamp(observed_at), client_order_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("IBKR Nautilus submission binding changed during dispatch")

    def _record_cancellation(
        self,
        cancellation_id: str,
        *,
        status: CancellationCommandStatus,
        observed_at: datetime,
    ) -> None:
        state = "canceled" if status is CancellationCommandStatus.CANCELED else "dispatched"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE ibkr_nautilus_cancel_bindings
                SET dispatch_state = ?, observed_at = ?
                WHERE cancellation_id = ? AND dispatch_state = 'prepared'
                """,
                (state, _timestamp(observed_at), cancellation_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("IBKR Nautilus cancellation binding changed during dispatch")

    def _reconcile_cancellation_terminal(
        self,
        client_order_id: str,
        *,
        provider_order_id: str,
        observed_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE ibkr_nautilus_cancel_bindings
                SET dispatch_state = 'canceled', observed_at = ?
                WHERE client_order_id = ?
                  AND provider_order_id = ?
                  AND dispatch_state IN ('prepared', 'dispatched', 'canceled')
                """,
                (_timestamp(observed_at), client_order_id, provider_order_id),
            )

    def _bind_provider_order_id(
        self,
        client_order_id: str,
        *,
        provider_order_id: str,
        observed_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT provider_order_id
                FROM ibkr_nautilus_order_bindings
                WHERE client_order_id = ?
                """,
                (client_order_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("IBKR Nautilus reconciliation binding disappeared")
            existing = cast(str | None, row["provider_order_id"])
            if existing is not None and existing != provider_order_id:
                raise RuntimeError("IBKR Nautilus provider order identity changed")
            connection.execute(
                """
                UPDATE ibkr_nautilus_order_bindings
                SET provider_order_id = ?, accepted_at = COALESCE(accepted_at, ?)
                WHERE client_order_id = ?
                """,
                (provider_order_id, _timestamp(observed_at), client_order_id),
            )

    def _order_binding(self, client_order_id: str) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()

    def _all_order_bindings(self) -> tuple[sqlite3.Row, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings ORDER BY client_order_id"
            ).fetchall()
        return tuple(rows)

    def _acceptance_scope_matches(
        self,
        acceptance: IbkrNautilusPaperProviderAcceptance,
    ) -> bool:
        now = self._clock()
        require_aware(now, "Provider activation evaluation time")
        return self._acceptance_scope_matches_at(acceptance, now=now)

    def _new_order_activation_valid_at(self, now: datetime) -> bool:
        acceptance = self._acceptance
        return (
            acceptance is not None
            and acceptance.is_current(now)
            and self._acceptance_scope_matches_at(acceptance, now=now)
        )

    def _acceptance_scope_matches_at(
        self,
        acceptance: IbkrNautilusPaperProviderAcceptance,
        *,
        now: datetime,
    ) -> bool:
        require_aware(now, "Provider activation evaluation time")
        store = self._activation_store
        head_id = self._activation_head_id
        if store is None or head_id is None:
            return False
        try:
            reopened, _, _ = _reopen_canonical_activation(
                store,
                acceptance_id=acceptance.acceptance_id,
                head_id=head_id,
                now=now,
            )
        except (KeyError, OSError, sqlite3.Error, TypeError, ValueError, PermissionError):
            return False
        if reopened != acceptance:
            return False
        authority_id = store.harness_authority_id
        try:
            with store.authority_transaction() as connection:
                head = connection.execute(
                    """
                    SELECT acceptance_id, activation_valid_until
                    FROM ibkr_nautilus_activation_head
                    WHERE singleton = 1 AND harness_authority_id = ? AND head_id = ?
                    """,
                    (authority_id, head_id),
                ).fetchone()
                evidence = connection.execute(
                    """
                    SELECT runtime_registration_id, route_registration_id, activation_head_id
                    FROM ibkr_nautilus_accepted_evidence
                    WHERE acceptance_id = ? AND harness_authority_id = ?
                    """,
                    (acceptance.acceptance_id, authority_id),
                ).fetchone()
                if head is None or evidence is None:
                    return False
                runtime_row = connection.execute(
                    """
                    SELECT payload_json FROM ibkr_nautilus_runtime_registrations
                    WHERE registration_id = ? AND harness_authority_id = ?
                    """,
                    (evidence["runtime_registration_id"], authority_id),
                ).fetchone()
                route_row = connection.execute(
                    """
                    SELECT payload_json FROM ibkr_nautilus_route_registrations
                    WHERE registration_id = ? AND harness_authority_id = ?
                    """,
                    (evidence["route_registration_id"], authority_id),
                ).fetchone()
        except sqlite3.Error:
            return False
        if runtime_row is None or route_row is None:
            return False
        runtime_payload = json.loads(cast(str, runtime_row["payload_json"]))
        route_payload = json.loads(cast(str, route_row["payload_json"]))
        expected_runtime = {
            "runtime_version": self._runtime.runtime_version,
            "nautilus_version": self._runtime.nautilus_version,
            "nautilus_ibapi_version": self._runtime.nautilus_ibapi_version,
            "configuration_hash": self._runtime.configuration_hash,
            "account_reference_hash": self._runtime.account_reference_hash,
            "acceptance_authority_id": self._runtime.acceptance_authority_id,
            "time_in_force": self._runtime.time_in_force,
        }
        expected_routes = {
            key: {
                "nautilus_instrument_id": route.nautilus_instrument_id,
                "market": route.market,
            }
            for key, route in sorted(self._instrument_routes.items())
        }
        runtime_registration_id = cast(str, evidence["runtime_registration_id"])
        return (
            cast(str, head["acceptance_id"]) == acceptance.acceptance_id
            and cast(str, evidence["activation_head_id"]) == head_id
            and now < _datetime(cast(str, head["activation_valid_until"]))
            and runtime_payload == expected_runtime
            and route_payload == expected_routes
            and _CANONICAL_RUNTIME_HANDLES.get((authority_id, runtime_registration_id))
            is self._runtime
            and self._acceptance_verifier.authority_id == self._runtime.acceptance_authority_id
            and self._acceptance_verifier.verify(acceptance)
            and acceptance.configuration_hash == self._runtime.configuration_hash
            and acceptance.account_reference_hash == self._runtime.account_reference_hash
            and acceptance.instrument_routes_hash == self._instrument_routes_hash
            and acceptance.runtime_version == self._runtime.runtime_version
            and acceptance.nautilus_version == self._runtime.nautilus_version
            and acceptance.nautilus_ibapi_version == self._runtime.nautilus_ibapi_version
            and acceptance.time_in_force == (self._runtime.time_in_force,)
            and self._runtime.session_scope_valid
            and self._runtime.activation_runtime_active
        )

    def _binding_scope_matches(self, row: sqlite3.Row) -> bool:
        return (
            cast(str, row["configuration_hash"]) == self._runtime.configuration_hash
            and cast(str, row["account_reference_hash"]) == self._runtime.account_reference_hash
            and cast(str, row["instrument_routes_hash"]) == self._instrument_routes_hash
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._state_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection


def _nautilus_client_order_id(capability: SubmissionCapability) -> str:
    return (
        "MIA-"
        + canonical_hash(
            {
                "provider_id": capability.provider_id,
                "provider_version": capability.provider_version,
                "client_order_id": capability.order.client_order_id,
                "order_hash": capability.order_hash,
            }
        )[:24]
    )


def _nautilus_mutation_payload(
    command: NautilusPaperSubmitCommand | NautilusPaperCancelCommand,
) -> dict[str, object]:
    common = {
        "connection_generation": command.connection_generation,
        "last_disconnection_ns": command.last_disconnection_ns,
        "scope_observed_at": _timestamp(command.scope_observed_at),
        "scope_valid_until": _timestamp(command.scope_valid_until),
    }
    if isinstance(command, NautilusPaperSubmitCommand):
        return {
            **common,
            "submission_id": command.submission_id,
            "nautilus_client_order_id": command.nautilus_client_order_id,
            "instrument_id": command.instrument_id,
            "side": command.side.value,
            "quantity": str(command.quantity),
            "order_kind": command.order_kind.value,
            "limit_price": None if command.limit_price is None else str(command.limit_price),
            "created_at": _timestamp(command.created_at),
            "expires_at": _timestamp(command.expires_at),
        }
    return {
        **common,
        "cancellation_id": command.cancellation_id,
        "nautilus_client_order_id": command.nautilus_client_order_id,
        "provider_order_id": command.provider_order_id,
    }


def hash_ibkr_nautilus_instrument_routes(
    routes: Mapping[str, IbkrNautilusInstrumentRoute],
) -> str:
    return canonical_hash(
        {
            instrument_id: {
                "nautilus_instrument_id": route.nautilus_instrument_id,
                "market": route.market,
            }
            for instrument_id, route in sorted(routes.items())
        }
    )


def _provider_execution_status(status: NautilusPaperRuntimeStatus) -> ExecutionStatus:
    return ExecutionStatus(status.value)


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))) or any(
        not value or value != value.strip() for value in values
    ):
        raise ValueError(f"{name} must be sorted, unique, non-empty strings")


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], raw))


def _scenario_evidence_tuple(value: object) -> tuple[IbkrNautilusPaperScenarioEvidence, ...]:
    if not isinstance(value, list):
        raise TypeError("acceptance scenario_evidence must be an array")
    result: list[IbkrNautilusPaperScenarioEvidence] = []
    for raw_item in cast(list[object], value):
        if not isinstance(raw_item, dict):
            raise TypeError("acceptance scenario evidence must be objects")
        item = cast(dict[str, object], raw_item)
        if set(item) != {"scenario", "evidence_hash", "passed"}:
            raise ValueError("acceptance scenario evidence fields are invalid")
        if not isinstance(item["scenario"], str) or not isinstance(item["evidence_hash"], str):
            raise TypeError("acceptance scenario evidence identities must be strings")
        if not isinstance(item["passed"], bool):
            raise TypeError("acceptance scenario evidence passed must be a boolean")
        result.append(
            IbkrNautilusPaperScenarioEvidence(
                scenario=item["scenario"],
                evidence_hash=item["evidence_hash"],
                passed=item["passed"],
            )
        )
    return tuple(result)


def _scenario_observation_from_dict(
    payload: object,
    *,
    _seal: object,
) -> IbkrNautilusPaperScenarioObservation:
    if not isinstance(payload, dict):
        raise TypeError("scenario observation must be an object")
    fields = cast(dict[str, object], payload)
    expected = {
        "observation_id",
        "scenario",
        "artifact_hash",
        "result_hash",
        "runner_id",
        "runner_seal",
        "configuration_hash",
        "account_reference_hash",
        "instrument_routes_hash",
        "markets",
        "order_types",
        "time_in_force",
        "nautilus_ibapi_version",
        "effective_client_id",
        "client_id_collision",
        "manual_order_auto_bind_observed",
        "exclusive_api_client_scope_observed",
        "passed",
        "observed_at",
    }
    if set(fields) != expected:
        raise ValueError("scenario observation fields are invalid")
    for name in (
        "observation_id",
        "scenario",
        "artifact_hash",
        "result_hash",
        "runner_id",
        "runner_seal",
        "configuration_hash",
        "account_reference_hash",
        "instrument_routes_hash",
        "nautilus_ibapi_version",
        "observed_at",
    ):
        if not isinstance(fields[name], str):
            raise TypeError(f"scenario observation {name} must be a string")
    if not isinstance(fields["effective_client_id"], int):
        raise TypeError("scenario observation effective_client_id must be an integer")
    for name in (
        "client_id_collision",
        "manual_order_auto_bind_observed",
        "exclusive_api_client_scope_observed",
        "passed",
    ):
        if not isinstance(fields[name], bool):
            raise TypeError(f"scenario observation {name} must be a boolean")
    return IbkrNautilusPaperScenarioObservation(
        observation_id=cast(str, fields["observation_id"]),
        scenario=cast(str, fields["scenario"]),
        artifact_hash=cast(str, fields["artifact_hash"]),
        result_hash=cast(str, fields["result_hash"]),
        runner_id=cast(str, fields["runner_id"]),
        runner_seal=cast(str, fields["runner_seal"]),
        configuration_hash=cast(str, fields["configuration_hash"]),
        account_reference_hash=cast(str, fields["account_reference_hash"]),
        instrument_routes_hash=cast(str, fields["instrument_routes_hash"]),
        markets=_string_tuple(fields["markets"], "scenario observation markets"),
        order_types=_string_tuple(fields["order_types"], "scenario observation order types"),
        time_in_force=_string_tuple(
            fields["time_in_force"],
            "scenario observation time_in_force",
        ),
        nautilus_ibapi_version=cast(str, fields["nautilus_ibapi_version"]),
        effective_client_id=fields["effective_client_id"],
        client_id_collision=cast(bool, fields["client_id_collision"]),
        manual_order_auto_bind_observed=cast(
            bool,
            fields["manual_order_auto_bind_observed"],
        ),
        exclusive_api_client_scope_observed=cast(
            bool,
            fields["exclusive_api_client_scope_observed"],
        ),
        passed=cast(bool, fields["passed"]),
        observed_at=_datetime(cast(str, fields["observed_at"])),
        _seal=_seal,
    )


def _scenario_observation_from_result(
    payload: object,
    *,
    artifact_hash: str,
    result_hash: str,
    runner_id: str,
    runner_seal: str,
) -> IbkrNautilusPaperScenarioObservation:
    if not isinstance(payload, dict):
        raise TypeError("scenario result must be an object")
    fields = cast(dict[str, object], payload)
    expected = {
        "schema_version",
        "scenario",
        "configuration_hash",
        "account_reference_hash",
        "instrument_routes_hash",
        "markets",
        "order_types",
        "time_in_force",
        "nautilus_ibapi_version",
        "effective_client_id",
        "client_id_collision",
        "manual_order_auto_bind_observed",
        "exclusive_api_client_scope_observed",
        "passed",
        "observed_at",
    }
    if set(fields) != expected:
        raise ValueError("scenario result fields are invalid")
    if fields["schema_version"] != IBKR_NAUTILUS_PAPER_SCENARIO_RESULT_SCHEMA:
        raise ValueError("unsupported scenario result schema")
    for name in (
        "scenario",
        "configuration_hash",
        "account_reference_hash",
        "instrument_routes_hash",
        "nautilus_ibapi_version",
        "observed_at",
    ):
        if not isinstance(fields[name], str):
            raise TypeError(f"scenario result {name} must be a string")
    effective_client_id = fields["effective_client_id"]
    if isinstance(effective_client_id, bool) or not isinstance(effective_client_id, int):
        raise TypeError("scenario result effective_client_id must be an integer")
    for name in (
        "client_id_collision",
        "manual_order_auto_bind_observed",
        "exclusive_api_client_scope_observed",
        "passed",
    ):
        if not isinstance(fields[name], bool):
            raise TypeError(f"scenario result {name} must be a boolean")
    return _issue_scenario_observation(
        scenario=cast(str, fields["scenario"]),
        artifact_hash=artifact_hash,
        result_hash=result_hash,
        runner_id=runner_id,
        runner_seal=runner_seal,
        configuration_hash=cast(str, fields["configuration_hash"]),
        account_reference_hash=cast(str, fields["account_reference_hash"]),
        instrument_routes_hash=cast(str, fields["instrument_routes_hash"]),
        markets=_string_tuple(fields["markets"], "scenario result markets"),
        order_types=_string_tuple(fields["order_types"], "scenario result order types"),
        time_in_force=_string_tuple(fields["time_in_force"], "scenario result time_in_force"),
        nautilus_ibapi_version=cast(str, fields["nautilus_ibapi_version"]),
        effective_client_id=effective_client_id,
        client_id_collision=cast(bool, fields["client_id_collision"]),
        manual_order_auto_bind_observed=cast(
            bool,
            fields["manual_order_auto_bind_observed"],
        ),
        exclusive_api_client_scope_observed=cast(
            bool,
            fields["exclusive_api_client_scope_observed"],
        ),
        passed=cast(bool, fields["passed"]),
        observed_at=_datetime(cast(str, fields["observed_at"])),
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _runner_evidence_seal(
    *,
    runner_id: str,
    artifact_bytes: bytes,
    result_bytes: bytes,
    key: bytes,
) -> str:
    material = (
        b"market-impact.ibkr-nautilus-paper-runner-evidence.v1\0"
        + runner_id.encode("utf-8")
        + b"\0"
        + artifact_bytes
        + b"\0"
        + result_bytes
    )
    return hmac.new(key, material, hashlib.sha256).hexdigest()


def _acceptance_authority_id(runner_id: str, key: bytes) -> str:
    digest = hmac.new(
        key,
        b"market-impact.ibkr-nautilus-paper-authority.v1\0" + runner_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return "ibkr-nautilus-paper-authority-" + digest


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "timestamp")
    return parsed
