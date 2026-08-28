from __future__ import annotations

import base64
import gzip
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha1, sha256
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.internet_archive import (
    InternetArchiveLocator,
    VerifiedInternetArchiveRecord,
)
from market_impact_agent.publisher_evidence import extract_publisher_news_evidence
from market_impact_agent.regime_archive_recovery import (
    audit_publisher_archive_recovery,
    recover_publisher_archive_record,
    recover_publisher_archive_snapshot,
    write_publisher_archive_research_document,
)
from market_impact_agent.regime_evidence import RegimeEvidenceManifest, RegimeEvidenceRecord
from market_impact_agent.regime_study import (
    RegimeBaselineProtocol,
    RegimeCheckpointProtocol,
    RegimeSourceRequirement,
    RegimeStudyCase,
    RegimeStudyRegistration,
    RegimeStudySource,
)

PAYLOAD = b"""
<html><head>
  <meta name="publishdate" content="2024-09-23">
  <meta property="og:title" content="Chinese shares close higher Monday">
</head><body><span>2024-09-23 16:06:15</span><p>Market report body.</p></body></html>
"""
CUTOFF = datetime(2024, 9, 24, 1, 25, tzinfo=UTC)


def _registration() -> RegimeStudyRegistration:
    case = RegimeStudyCase(
        case_key="case-a",
        decision_schedule="weekly",
        analysis_needs=("narrative_diffusion",),
        candidate_method_skills=("skill-a",),
        query_terms=("market",),
        evaluation_horizons=("full_case",),
        source_requirements=(
            RegimeSourceRequirement(
                category="established_news",
                source_ids=("xinhua-established-news", "scmp-established-news"),
                minimum_records_per_checkpoint=1,
                minimum_distinct_sources=1,
                authenticated_availability_required=True,
            ),
        ),
    )
    protocol = RegimeCheckpointProtocol(
        timezone="Asia/Shanghai",
        decision_time_local="09:25:00",
        price_lookback_sessions=60,
        news_lookback_calendar_days=(
            ("monthly", 31),
            ("weekly", 14),
            ("event_then_weekly", 14),
        ),
        maximum_age_calendar_days=(),
    )
    baseline = RegimeBaselineProtocol(
        annualization_sessions=252,
        minimum_risk_sessions=20,
        risk_free_rate_annual=Decimal(0),
        cvar_confidence=Decimal("0.95"),
        transaction_cost_bps_one_way=Decimal(10),
        rebalance_frequency="monthly_first_session",
        momentum_lookback_sessions=20,
        momentum_top_k=3,
        strategies=("cash",),
    )
    return RegimeStudyRegistration(
        registration_id="regime-study-registration-" + "1" * 64,
        version="1.0.0",
        dataset_id="market-regime-dataset-" + "2" * 64,
        dataset_hash="3" * 64,
        method_catalog_id="method-skill-catalog-" + "4" * 64,
        method_catalog_hash="5" * 64,
        outcomes_opened=True,
        source_catalog=(
            RegimeStudySource(
                source_id="xinhua-established-news",
                category="established_news",
                provider_id="publisher-https-snapshot",
                source_tier="established_news",
                acquisition_mode="implemented_source_reported_version",
                point_in_time_authority=True,
                evidence_types=("timestamped_narrative_corpus",),
                license_note="private",
            ),
            RegimeStudySource(
                source_id="scmp-established-news",
                category="established_news",
                provider_id="publisher-https-snapshot",
                source_tier="established_news",
                acquisition_mode="implemented_source_reported_version",
                point_in_time_authority=True,
                evidence_types=("timestamped_narrative_corpus",),
                license_note="private",
            ),
        ),
        checkpoint_protocol=protocol,
        baseline_protocol=baseline,
        cases=(case,),
        core={"synthetic": True},
    )


def _record():
    return extract_publisher_news_evidence(
        url="https://www.xinhuanet.com/fortune/2024-09/23/c_example.htm",
        payload=PAYLOAD,
        retrieved_at=datetime(2026, 8, 28, tzinfo=UTC),
        case_keys=("case-a",),
        claim_id="claim-a",
        lineage_id="lineage-a",
    )


def _manifest() -> RegimeEvidenceManifest:
    registration = _registration()
    return RegimeEvidenceManifest.build(
        dataset_id=registration.dataset_id,
        dataset_hash=registration.dataset_hash,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        panel_id="regime-panel-" + "6" * 64,
        panel_hash="7" * 64,
        outcomes_opened=True,
        records=(_record(),),
    )


def _qualification(manifest: RegimeEvidenceManifest) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "market-impact.regime-evidence-qualification-report.v1",
        "dataset_id": manifest.dataset_id,
        "registration_id": manifest.registration_id,
        "panel_id": manifest.panel_id,
        "manifest_id": manifest.manifest_id,
        "outcomes_opened": True,
        "case_count": 1,
        "all_source_requirements_ready": False,
        "diagnostic_agent_run_eligible": False,
        "agent_effectiveness_claim_eligible": False,
        "cases": [
            {
                "case_key": "case-a",
                "checkpoint_count": 1,
                "all_checkpoints_ready": False,
                "checkpoints": [
                    {
                        "session_date": "2024-09-24",
                        "cutoff_at": "2024-09-24T01:25:00Z",
                        "ready": False,
                        "event_revelation": {
                            "required": False,
                            "ready": True,
                            "record_ids": [],
                            "blockers": [],
                        },
                        "requirements": [],
                    }
                ],
            }
        ],
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-evidence-qualification-report-{canonical_hash(core)}",
    }


def _locator() -> InternetArchiveLocator:
    return InternetArchiveLocator(
        target_url="http://www.xinhuanet.com/fortune/2024-09/23/c_example.htm",
        timestamp="20240923161200",
        digest="sha1:" + "A" * 32,
        http_status=200,
        media_type="text/html",
    )


def test_archive_audit_separates_recoverable_not_found_and_source_error() -> None:
    class Index:
        def __init__(self) -> None:
            self.mode = "found"

        def locate_latest(self, *, target_url: str, not_after: datetime):
            if self.mode == "error":
                raise RuntimeError("network detail must not leak")
            return _locator() if self.mode == "found" else None

    manifest = _manifest()
    index = Index()
    found = audit_publisher_archive_recovery(
        manifest,
        _registration(),
        _qualification(manifest),
        case_keys=("case-a",),
        index=index,
    )
    checkpoint = cast(list[dict[str, object]], found["checkpoints"])[0]
    candidate = cast(list[dict[str, object]], checkpoint["candidates"])[0]
    assert candidate["status"] == "capture_found_unverified"
    assert checkpoint["news_ready_if_found_captures_verify"] is True
    assert found["found_count"] == 1

    index.mode = "missing"
    missing = audit_publisher_archive_recovery(
        manifest,
        _registration(),
        _qualification(manifest),
        case_keys=("case-a",),
        index=index,
    )
    assert cast(list[dict[str, object]], missing["checkpoints"])[0]["candidates"] == [
        {
            **{key: value for key, value in candidate.items() if key not in {"status", "locator"}},
            "status": "not_found",
            "locator": None,
        }
    ]

    index.mode = "error"
    failed = audit_publisher_archive_recovery(
        manifest,
        _registration(),
        _qualification(manifest),
        case_keys=("case-a",),
        index=index,
    )
    failed_candidate = cast(
        list[dict[str, object]],
        cast(list[dict[str, object]], failed["checkpoints"])[0]["candidates"],
    )[0]
    assert failed_candidate["status"] == "source_error"
    assert failed_candidate["error_type"] == "RuntimeError"
    assert "network" not in str(failed)


def test_verified_archive_recovery_preserves_source_identity_and_uses_capture_authority() -> None:
    original = _record()
    locator = _locator()
    archive = VerifiedInternetArchiveRecord(
        provider_id="internet-archive-replay",
        archive_id="internet-archive",
        adapter_version="1.0.0",
        locator=locator,
        captured_at=locator.captured_at,
        retrieved_at=datetime(2026, 8, 28, tzinfo=UTC),
        target_url=locator.target_url,
        http_status=200,
        media_type="text/html",
        replay_url="https://web.archive.org/web/replay",
        payload_sha256=sha256(PAYLOAD).hexdigest(),
        payload_digest=locator.digest,
        payload=PAYLOAD,
    )

    recovered = recover_publisher_archive_record(
        original,
        locator,
        not_after=CUTOFF,
        fetch=lambda item: archive,
    )

    assert recovered.source_id == original.source_id
    assert recovered.provider_id == original.provider_id
    assert recovered.source_ref == original.source_ref
    assert recovered.authority_kind.value == "verified_archive"
    assert recovered.authority_at == locator.captured_at
    assert recovered.authority_id == locator.source_version_id
    assert recovered.content_hash == sha256(PAYLOAD).hexdigest()
    assert recovered.record_id != original.record_id


def test_verified_archive_recovery_decodes_gzip_only_for_publisher_extraction() -> None:
    original = _record()
    compressed = gzip.compress(PAYLOAD, mtime=0)
    digest = "sha1:" + base64.b32encode(sha1(compressed, usedforsecurity=False).digest()).decode(
        "ascii"
    ).rstrip("=")
    locator = InternetArchiveLocator(
        target_url="http://www.xinhuanet.com/fortune/2024-09/23/c_example.htm",
        timestamp="20240923161200",
        digest=digest,
        http_status=200,
        media_type="text/html",
    )
    archive = VerifiedInternetArchiveRecord(
        provider_id="internet-archive-replay",
        archive_id="internet-archive",
        adapter_version="1.0.0",
        locator=locator,
        captured_at=locator.captured_at,
        retrieved_at=datetime(2026, 8, 28, tzinfo=UTC),
        target_url=locator.target_url,
        http_status=200,
        media_type="text/html",
        replay_url="https://web.archive.org/web/replay",
        payload_sha256=sha256(compressed).hexdigest(),
        payload_digest=locator.digest,
        payload=compressed,
    )

    recovered = recover_publisher_archive_record(
        original,
        locator,
        not_after=CUTOFF,
        fetch=lambda item: archive,
    )

    assert recovered.content_hash == sha256(compressed).hexdigest()
    assert recovered.title == original.title


def test_verified_archive_recovery_writes_private_research_document(tmp_path: Path) -> None:
    original = _record()
    locator = _locator()
    archive = VerifiedInternetArchiveRecord(
        provider_id="internet-archive-replay",
        archive_id="internet-archive",
        adapter_version="1.0.0",
        locator=locator,
        captured_at=locator.captured_at,
        retrieved_at=datetime(2026, 8, 28, tzinfo=UTC),
        target_url=locator.target_url,
        http_status=200,
        media_type="text/html",
        replay_url="https://web.archive.org/web/replay",
        payload_sha256=sha256(PAYLOAD).hexdigest(),
        payload_digest=locator.digest,
        payload=PAYLOAD,
    )

    snapshot = recover_publisher_archive_snapshot(
        original,
        locator,
        not_after=CUTOFF,
        fetch=lambda item: archive,
    )
    path = write_publisher_archive_research_document(snapshot, root=tmp_path)

    assert snapshot.research_document["article_excerpt"] == "Market report body."
    assert snapshot.research_document["content_hash"] == snapshot.record.content_hash
    assert path.name == f"{snapshot.record.content_hash}.json"
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "target_url",
    (
        "http://www.xinhuanet.com:8080/fortune/2024-09/23/c_example.htm",
        "http://www.xinhuanet.com/fortune/2024-09/23/c_example.htm/",
        "http://www.xinhuanet.com/fortune/2024-09/23/c_example.htm;revision=1",
        "http://www.xinhuanet.com/fortune/2024-09/23/c_example.htm?revision=1",
    ),
)
def test_verified_archive_recovery_rejects_a_different_http_target(target_url: str) -> None:
    locator = InternetArchiveLocator(
        target_url=target_url,
        timestamp="20240923161200",
        digest="sha1:" + "A" * 32,
        http_status=200,
        media_type="text/html",
    )

    with pytest.raises(ValueError, match="locator does not match"):
        recover_publisher_archive_record(_record(), locator, not_after=CUTOFF)


def test_verified_archive_recovery_can_link_a_later_publisher_revision() -> None:
    url = "https://www.scmp.com/business/china-business/article/3278947/example"
    current_payload = b"""
    <html><head>
      <meta property="og:title" content="Chinese stocks edge up slightly">
      <meta property="article:published_time" content="2024-09-18T10:29:06+08:00">
      <meta property="article:modified_time" content="2024-09-18T15:18:50+08:00">
    </head><body><p>Current article body.</p></body></html>
    """
    earlier_payload = current_payload.replace(
        b"2024-09-18T15:18:50+08:00", b"2024-09-18T14:00:00+08:00"
    )
    later_payload = current_payload.replace(
        b"2024-09-18T15:18:50+08:00", b"2024-09-18T15:00:00+08:00"
    )
    original = extract_publisher_news_evidence(
        url=url,
        payload=current_payload,
        retrieved_at=datetime(2026, 8, 28, tzinfo=UTC),
        case_keys=("case-a",),
        claim_id="claim-a",
        lineage_id="lineage-a",
    )

    def recovered(
        payload: bytes,
        timestamp: str,
        supersedes: RegimeEvidenceRecord | None = None,
    ) -> RegimeEvidenceRecord:
        locator = InternetArchiveLocator(
            target_url=url.replace("https://", "http://"),
            timestamp=timestamp,
            digest="sha1:" + "A" * 32,
            http_status=200,
            media_type="text/html",
        )
        archive = VerifiedInternetArchiveRecord(
            provider_id="internet-archive-replay",
            archive_id="internet-archive",
            adapter_version="1.0.0",
            locator=locator,
            captured_at=locator.captured_at,
            retrieved_at=datetime(2026, 8, 28, tzinfo=UTC),
            target_url=locator.target_url,
            http_status=200,
            media_type="text/html",
            replay_url="https://web.archive.org/web/replay",
            payload_sha256=sha256(payload).hexdigest(),
            payload_digest=locator.digest,
            payload=payload,
        )
        return recover_publisher_archive_record(
            original,
            locator,
            not_after=CUTOFF,
            supersedes=supersedes,
            fetch=lambda item: archive,
        )

    earlier = recovered(earlier_payload, "20240918061000")
    with pytest.raises(ValueError, match="changes source or lineage identity"):
        recovered(later_payload, "20240918071000", supersedes=_record())
    with pytest.raises(ValueError, match="must advance availability"):
        recovered(earlier_payload, "20240918061100", supersedes=earlier)
    later = recovered(later_payload, "20240918071000", supersedes=earlier)

    assert later.supersedes_id == earlier.record_id
    assert later.lineage_id == earlier.lineage_id
    assert later.claim_id == earlier.claim_id
    assert later.available_at > earlier.available_at
