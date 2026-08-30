import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_impact_agent.provider_reliability import (
    ProviderCircuitState,
    ProviderFailure,
    ProviderGenerationState,
    ProviderHealthStore,
    ProviderRetryDisposition,
)

NOW = datetime(2026, 8, 31, 4, tzinfo=UTC)


def test_auth_unavailable_opens_durable_circuit_and_pending_notice(tmp_path: Path) -> None:
    path = tmp_path / "provider-health.sqlite"
    store = ProviderHealthStore(path)
    failure = ProviderFailure(
        "sanitized authentication failure",
        error_class="http",
        diagnostic_code="auth_unavailable",
        http_status=500,
        request_id="mia-auth-1",
        generation_state=ProviderGenerationState.NOT_STARTED,
        retry_disposition=ProviderRetryDisposition.TERMINAL,
        attempts=1,
        elapsed_latency_ms=12.5,
    )

    store.record_failure(
        provider_id="cliproxyapi-openai-compatible",
        failure=failure,
        physical_attempt=1,
        observed_at=NOW,
    )

    reopened = ProviderHealthStore(path)
    admission = reopened.admission("cliproxyapi-openai-compatible", now=NOW)
    assert not admission.allowed
    assert admission.state is ProviderCircuitState.OPEN
    notices = reopened.pending_notices(provider_id="cliproxyapi-openai-compatible")
    assert len(notices) == 1
    assert notices[0].notice_kind == "provider_action_required"
    assert notices[0].payload == {
        "diagnostic_code": "auth_unavailable",
        "error_class": "http",
        "http_status": 500,
        "request_id": "mia-auth-1",
    }

    reopened.mark_notice_delivered(notice_id=notices[0].notice_id, delivered_at=NOW)
    assert reopened.pending_notices() == ()
    reopened.operator_reset(
        provider_id="cliproxyapi-openai-compatible",
        observed_at=NOW + timedelta(minutes=1),
    )
    assert reopened.admission(
        "cliproxyapi-openai-compatible", now=NOW + timedelta(minutes=1)
    ).allowed


def test_provider_store_uses_wal_full_and_never_persists_failure_message(
    tmp_path: Path,
) -> None:
    path = tmp_path / "provider-health.sqlite"
    secret = "SUPERSECRET-BEARER-CREDENTIAL"
    store = ProviderHealthStore(path)
    store.record_failure(
        provider_id="cliproxyapi-openai-compatible",
        failure=ProviderFailure(
            f"must not persist {secret}",
            error_class="tls",
            diagnostic_code="tls_bad_record_mac",
            request_id="mia-tls-1",
            generation_state=ProviderGenerationState.UNKNOWN,
            retry_disposition=ProviderRetryDisposition.FORBIDDEN,
            attempts=1,
            elapsed_latency_ms=7.0,
        ),
        physical_attempt=1,
        observed_at=NOW,
    )

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        incident = connection.execute(
            "SELECT diagnostic_code, generation_state FROM provider_incidents"
        ).fetchone()
    assert incident == ("tls_bad_record_mac", "unknown")
    assert secret.encode() not in path.read_bytes()

    cooldown = ProviderHealthStore(path).admission("cliproxyapi-openai-compatible", now=NOW)
    assert not cooldown.allowed
    assert cooldown.state is ProviderCircuitState.COOLDOWN
    assert cooldown.retry_after_seconds == 30.0


@pytest.mark.parametrize("diagnostic_code", ["auth_unavailable", "quota_exhausted"])
def test_ordinary_late_success_never_closes_open_circuit_but_probe_can_recover(
    tmp_path: Path, diagnostic_code: str
) -> None:
    store = ProviderHealthStore(tmp_path / "provider-health.sqlite")
    provider_id = "cliproxyapi-openai-compatible"
    store.record_failure(
        provider_id=provider_id,
        failure=ProviderFailure(
            "sanitized immediate failure",
            error_class="http",
            diagnostic_code=diagnostic_code,
            http_status=500,
            request_id=f"mia-{diagnostic_code}",
            generation_state=ProviderGenerationState.NOT_STARTED,
            retry_disposition=ProviderRetryDisposition.TERMINAL,
        ),
        physical_attempt=1,
        observed_at=NOW,
    )

    store.record_success(
        provider_id=provider_id,
        request_id="mia-late-success",
        observed_at=NOW + timedelta(seconds=1),
    )

    assert (
        store.admission(provider_id, now=NOW + timedelta(seconds=1)).state
        is ProviderCircuitState.OPEN
    )

    store.record_failure(
        provider_id=provider_id,
        failure=ProviderFailure(
            "sanitized secondary failure",
            error_class="transport",
            diagnostic_code="transport_error",
            request_id="mia-secondary-failure",
            generation_state=ProviderGenerationState.UNKNOWN,
            retry_disposition=ProviderRetryDisposition.FORBIDDEN,
        ),
        physical_attempt=1,
        observed_at=NOW + timedelta(seconds=2),
    )

    assert (
        store.admission(provider_id, now=NOW + timedelta(seconds=2)).state
        is ProviderCircuitState.OPEN
    )

    store.record_probe_success(
        provider_id=provider_id,
        request_id="mia-probe-success",
        observed_at=NOW + timedelta(seconds=3),
    )

    assert (
        store.admission(provider_id, now=NOW + timedelta(seconds=3)).state
        is ProviderCircuitState.HEALTHY
    )
    assert [notice.notice_kind for notice in store.pending_notices(provider_id=provider_id)] == [
        "provider_action_required",
        "provider_recovered",
    ]


def test_stale_failure_is_audited_without_regressing_newer_success(tmp_path: Path) -> None:
    path = tmp_path / "provider-health.sqlite"
    provider_id = "minimax-openai-compatible"
    store = ProviderHealthStore(path)
    store.record_success(
        provider_id=provider_id,
        request_id="mia-newer-success",
        observed_at=NOW,
    )
    store.record_failure(
        provider_id=provider_id,
        failure=ProviderFailure(
            "sanitized stale transport failure",
            error_class="transport",
            diagnostic_code="transport_error",
            request_id="mia-stale-failure",
            generation_state=ProviderGenerationState.UNKNOWN,
            retry_disposition=ProviderRetryDisposition.FORBIDDEN,
        ),
        physical_attempt=1,
        observed_at=NOW - timedelta(seconds=1),
    )

    assert store.admission(provider_id, now=NOW).state is ProviderCircuitState.HEALTHY
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM provider_incidents").fetchone() == (1,)


def test_stale_success_does_not_clear_newer_cooldown(tmp_path: Path) -> None:
    store = ProviderHealthStore(tmp_path / "provider-health.sqlite")
    provider_id = "cliproxyapi-openai-compatible"
    store.record_failure(
        provider_id=provider_id,
        failure=ProviderFailure(
            "sanitized ambiguous transport failure",
            error_class="transport",
            diagnostic_code="transport_error",
            request_id="mia-newer-failure",
            generation_state=ProviderGenerationState.UNKNOWN,
            retry_disposition=ProviderRetryDisposition.FORBIDDEN,
        ),
        physical_attempt=1,
        observed_at=NOW,
    )

    store.record_success(
        provider_id=provider_id,
        request_id="mia-stale-success",
        observed_at=NOW - timedelta(seconds=1),
    )

    assert store.admission(provider_id, now=NOW).state is ProviderCircuitState.COOLDOWN
