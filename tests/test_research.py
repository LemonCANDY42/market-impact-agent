from datetime import UTC, datetime
from hashlib import sha256

import pytest

from market_impact_agent.research import (
    AssessmentMode,
    AssessmentRoute,
    EventArchetype,
    EventAssessment,
    EventEnvelope,
    EventStage,
    EvidenceItem,
    EvidenceTier,
    ExpectationDelta,
    ExpectationDirection,
    ExpectedEffect,
    RevelationMode,
    TransmissionChannel,
    TransmissionDirectness,
    TransmissionPath,
    TransmissionStep,
    materialize_event_envelope,
    route_event_assessment,
)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 25, hour, minute, tzinfo=UTC)


DEFAULT_VISIBLE_AT = at(2, 3)
DEFAULT_RETRIEVED_AT = at(2, 4)


def evidence(
    evidence_id: str,
    *,
    visible_at: datetime = DEFAULT_VISIBLE_AT,
    retrieved_at: datetime = DEFAULT_RETRIEVED_AT,
    claim_id: str | None = None,
    tier: EvidenceTier = EvidenceTier.OFFICIAL,
    supersedes_id: str | None = None,
) -> EvidenceItem:
    claim = f"canonical claim for {evidence_id}"
    return EvidenceItem(
        evidence_id=evidence_id,
        claim_id=claim_id or evidence_id,
        source_ref=f"synthetic://{evidence_id}",
        source_tier=tier,
        occurred_at=at(2),
        published_at=at(2, 2),
        visible_at=visible_at,
        retrieved_at=retrieved_at,
        claim=claim,
        claim_hash=sha256(claim.encode()).hexdigest(),
        supersedes_id=supersedes_id,
    )


def test_evidence_requires_point_in_time_chronology() -> None:
    claim = "outage confirmed"
    with pytest.raises(ValueError, match="published_at must not be after visible_at"):
        EvidenceItem(
            evidence_id="evidence-1",
            claim_id="claim-1",
            source_ref="synthetic://evidence-1",
            source_tier=EvidenceTier.OFFICIAL,
            occurred_at=at(2),
            published_at=at(2, 5),
            visible_at=at(2, 4),
            retrieved_at=at(2, 6),
            claim=claim,
            claim_hash=sha256(claim.encode()).hexdigest(),
        )


def test_evidence_can_describe_a_future_scheduled_occurrence() -> None:
    claim = "meeting scheduled"
    item = EvidenceItem(
        evidence_id="meeting-announcement",
        claim_id="meeting-scheduled",
        source_ref="synthetic://official/meeting",
        source_tier=EvidenceTier.OFFICIAL,
        occurred_at=at(12),
        published_at=at(2),
        visible_at=at(2, 1),
        retrieved_at=at(2, 2),
        claim=claim,
        claim_hash=sha256(claim.encode()).hexdigest(),
    )

    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(item,),
    )

    assert envelope.evidence == (item,)


def test_evidence_rejects_a_claim_hash_mismatch() -> None:
    with pytest.raises(ValueError, match="claim_hash must match claim"):
        EvidenceItem(
            evidence_id="evidence-1",
            claim_id="claim-1",
            source_ref="synthetic://evidence-1",
            source_tier=EvidenceTier.OFFICIAL,
            occurred_at=at(2),
            published_at=at(2),
            visible_at=at(2),
            retrieved_at=at(2),
            claim="outage confirmed",
            claim_hash="a" * 64,
        )


def test_event_envelope_rejects_future_visible_evidence() -> None:
    item = evidence("future", visible_at=at(3, 1), retrieved_at=at(3, 2))

    with pytest.raises(ValueError, match="future-visible evidence"):
        EventEnvelope(
            envelope_id="envelope-1",
            event_id="event-1",
            as_of=at(3),
            evidence=(item,),
        )


def test_materialization_filters_future_evidence_and_retains_visible_revision_history() -> None:
    original = evidence("report-v1", claim_id="throughput")
    revision = evidence(
        "report-v2",
        claim_id="throughput",
        visible_at=at(2, 5),
        retrieved_at=at(2, 6),
        supersedes_id="report-v1",
    )
    future_revision = evidence(
        "report-v3",
        claim_id="throughput",
        visible_at=at(3, 5),
        retrieved_at=at(3, 6),
        supersedes_id="report-v2",
    )
    independent = evidence(
        "independent-report",
        visible_at=at(2, 4),
        retrieved_at=at(2, 5),
    )

    envelope = materialize_event_envelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(revision, future_revision, independent, original),
    )

    assert [item.evidence_id for item in envelope.evidence] == [
        "report-v1",
        "independent-report",
        "report-v2",
    ]
    assert [item.evidence_id for item in envelope.current_evidence] == [
        "independent-report",
        "report-v2",
    ]


def test_materialization_rejects_an_unknown_revision_target() -> None:
    with pytest.raises(ValueError, match="unknown evidence: missing-report"):
        materialize_event_envelope(
            envelope_id="envelope-1",
            event_id="event-1",
            as_of=at(3),
            evidence=(evidence("report-v2", supersedes_id="missing-report"),),
        )


def test_event_envelope_rejects_invalid_revision_lineage() -> None:
    original = evidence("report-v1", claim_id="throughput")
    too_early_revision = evidence(
        "report-v2",
        claim_id="throughput",
        supersedes_id="report-v1",
    )

    with pytest.raises(ValueError, match="must be visible after superseded evidence"):
        EventEnvelope(
            envelope_id="envelope-1",
            event_id="event-1",
            as_of=at(3),
            evidence=(original, too_early_revision),
        )


def test_materialization_rejects_cross_claim_and_forked_revisions() -> None:
    original = evidence("report-v1", claim_id="throughput")
    cross_claim = evidence(
        "report-v2",
        claim_id="inventory",
        visible_at=at(2, 5),
        retrieved_at=at(2, 6),
        supersedes_id="report-v1",
    )
    with pytest.raises(ValueError, match="must retain claim_id"):
        materialize_event_envelope(
            envelope_id="envelope-1",
            event_id="event-1",
            as_of=at(3),
            evidence=(original, cross_claim),
        )

    revision = evidence(
        "report-v2",
        claim_id="throughput",
        visible_at=at(2, 5),
        retrieved_at=at(2, 6),
        supersedes_id="report-v1",
    )
    competing_revision = evidence(
        "report-v3",
        claim_id="throughput",
        visible_at=at(2, 6),
        retrieved_at=at(2, 7),
        supersedes_id="report-v1",
    )
    with pytest.raises(ValueError, match="must have at most one direct revision"):
        materialize_event_envelope(
            envelope_id="envelope-1",
            event_id="event-1",
            as_of=at(3),
            evidence=(original, revision, competing_revision),
        )


def test_duplicate_reporting_does_not_inflate_independent_claim_count() -> None:
    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(
            evidence("wire-copy-1", claim_id="outage-confirmed"),
            evidence("wire-copy-2", claim_id="outage-confirmed"),
        ),
    )

    assert envelope.independent_claim_count == 1


@pytest.mark.parametrize(
    ("kwargs", "expected_mode"),
    [
        ({"mapping_known": True}, AssessmentMode.FAST),
        ({"mapping_known": False}, AssessmentMode.DEEP),
        ({"mapping_known": True, "facts_disputed": True}, AssessmentMode.DEEP),
        ({"mapping_known": True, "market_state_conflicting": True}, AssessmentMode.COMBINED),
        ({"mapping_known": True, "high_impact": True}, AssessmentMode.COMBINED),
    ],
)
def test_assessment_router_is_fail_closed(
    kwargs: dict[str, bool], expected_mode: AssessmentMode
) -> None:
    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(evidence("official-confirmation"),),
    )

    route = route_event_assessment(envelope, **kwargs)

    assert route.mode is expected_mode
    assert route.max_depth >= 1
    assert route.max_branches >= 1


def test_unverified_source_forces_deep_assessment() -> None:
    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(evidence("social-post", tier=EvidenceTier.UNVERIFIED),),
    )

    assert route_event_assessment(envelope, mapping_known=True).mode is AssessmentMode.DEEP


def test_superseded_weak_source_does_not_force_deep_assessment() -> None:
    weak_original = evidence(
        "social-post",
        visible_at=at(2, 3),
        retrieved_at=at(2, 4),
        claim_id="outage-status",
        tier=EvidenceTier.UNVERIFIED,
    )
    official_revision = evidence(
        "official-confirmation",
        visible_at=at(2, 5),
        retrieved_at=at(2, 6),
        claim_id="outage-status",
        tier=EvidenceTier.OFFICIAL,
        supersedes_id="social-post",
    )
    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(weak_original, official_revision),
    )

    assert envelope.evidence == (weak_original, official_revision)
    assert envelope.current_evidence == (official_revision,)
    assert route_event_assessment(envelope, mapping_known=True).mode is AssessmentMode.FAST


def test_fast_route_cannot_exceed_second_order_depth() -> None:
    with pytest.raises(ValueError, match="fast assessment routes cannot exceed second-order depth"):
        AssessmentRoute(
            mode=AssessmentMode.FAST,
            max_depth=4,
            max_branches=3,
            reasons=("test route",),
        )


def test_event_assessment_enforces_evidence_links_and_route_caps() -> None:
    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(evidence("official-confirmation"),),
    )
    route = route_event_assessment(envelope, mapping_known=True)
    step = TransmissionStep(
        step_id="step-1",
        from_node="physical_supply",
        to_node="asset:crude_benchmark",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.DIRECT,
        mechanism="lower near-term physical availability",
        affected_variable="prompt crude availability",
        expected_effect=ExpectedEffect.UP,
        horizon_sessions=1,
        confidence=0.8,
        evidence_refs=("official-confirmation",),
    )
    path = TransmissionPath(
        path_id="path-1",
        target_ref="asset:crude_benchmark",
        steps=(step,),
        counterevidence_refs=(),
        blockers=(),
        invalidation_conditions=("replacement supply fully offsets the outage",),
    )

    assessment = EventAssessment(
        assessment_id="assessment-1",
        envelope=envelope,
        archetype=EventArchetype.PHYSICAL_SUPPLY_LOGISTICS,
        revelation_mode=RevelationMode.UNSCHEDULED,
        stage=EventStage.CORROBORATED,
        route=route,
        expectation_delta=ExpectationDelta(
            baseline_source_ref="official-confirmation",
            expected="normal throughput",
            observed="material temporary outage",
            direction=ExpectationDirection.NEGATIVE,
            confidence=0.9,
        ),
        paths=(path,),
        blockers=(),
    )

    assert assessment.paths == (path,)

    bad_step = TransmissionStep(
        step_id="step-2",
        from_node="physical_supply",
        to_node="industry:airlines",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.DIRECT,
        mechanism="higher expected fuel input cost",
        affected_variable="jet fuel input cost",
        expected_effect=ExpectedEffect.DOWN,
        horizon_sessions=3,
        confidence=0.6,
        evidence_refs=("missing-evidence",),
    )
    bad_path = TransmissionPath(
        path_id="path-2",
        target_ref="industry:airlines",
        steps=(bad_step,),
        counterevidence_refs=(),
        blockers=(),
        invalidation_conditions=("fuel hedges fully offset spot prices",),
    )

    with pytest.raises(ValueError, match="unknown evidence reference"):
        EventAssessment(
            assessment_id="assessment-2",
            envelope=envelope,
            archetype=EventArchetype.PHYSICAL_SUPPLY_LOGISTICS,
            revelation_mode=RevelationMode.UNSCHEDULED,
            stage=EventStage.CORROBORATED,
            route=route,
            expectation_delta=ExpectationDelta(
                baseline_source_ref="official-confirmation",
                expected="normal throughput",
                observed="material temporary outage",
                direction=ExpectationDirection.NEGATIVE,
                confidence=0.9,
            ),
            paths=(bad_path,),
            blockers=(),
        )


def test_expectation_delta_can_be_explicitly_unknown() -> None:
    delta = ExpectationDelta(
        baseline_source_ref=None,
        expected=None,
        observed="unscheduled conflict escalation",
        direction=ExpectationDirection.UNKNOWN,
        confidence=0.0,
    )

    assert delta.direction is ExpectationDirection.UNKNOWN


@pytest.mark.parametrize("field", ["baseline_source_ref", "expected", "observed"])
def test_known_expectation_delta_requires_all_values(field: str) -> None:
    values: dict[str, str | None] = {
        "baseline_source_ref": "official-confirmation",
        "expected": "normal throughput",
        "observed": "material temporary outage",
    }
    values[field] = None

    with pytest.raises(ValueError, match="known expectation deltas require"):
        ExpectationDelta(
            baseline_source_ref=values["baseline_source_ref"],
            expected=values["expected"],
            observed=values["observed"],
            direction=ExpectationDirection.NEGATIVE,
            confidence=0.9,
        )


def test_transmission_path_requires_adjacent_steps_and_directness_by_position() -> None:
    direct_step = TransmissionStep(
        step_id="step-1",
        from_node="physical_supply",
        to_node="crude_benchmark",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.DIRECT,
        mechanism="lower near-term physical availability",
        affected_variable="prompt crude availability",
        expected_effect=ExpectedEffect.UP,
        horizon_sessions=1,
        confidence=0.8,
        evidence_refs=("official-confirmation",),
    )
    non_adjacent_second_step = TransmissionStep(
        step_id="step-2",
        from_node="unrelated_input",
        to_node="airline_margin",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.SECOND_ORDER,
        mechanism="higher expected fuel input cost",
        affected_variable="jet fuel input cost",
        expected_effect=ExpectedEffect.DOWN,
        horizon_sessions=3,
        confidence=0.6,
        evidence_refs=("official-confirmation",),
    )

    with pytest.raises(ValueError, match="transmission path steps must be adjacent"):
        TransmissionPath(
            path_id="non-adjacent-path",
            target_ref="industry:airlines",
            steps=(direct_step, non_adjacent_second_step),
            counterevidence_refs=(),
            blockers=(),
            invalidation_conditions=("fuel hedges fully offset spot prices",),
        )

    incorrect_second_step = TransmissionStep(
        step_id="step-2",
        from_node="crude_benchmark",
        to_node="airline_margin",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.FOURTH_ORDER,
        mechanism="higher expected fuel input cost",
        affected_variable="jet fuel input cost",
        expected_effect=ExpectedEffect.DOWN,
        horizon_sessions=3,
        confidence=0.6,
        evidence_refs=("official-confirmation",),
    )

    with pytest.raises(ValueError, match="must be second_order at position 2"):
        TransmissionPath(
            path_id="incorrect-directness-path",
            target_ref="industry:airlines",
            steps=(direct_step, incorrect_second_step),
            counterevidence_refs=(),
            blockers=(),
            invalidation_conditions=("fuel hedges fully offset spot prices",),
        )


def test_transmission_path_must_reach_its_declared_target() -> None:
    step = TransmissionStep(
        step_id="step-1",
        from_node="physical_supply",
        to_node="commodity:crude_benchmark",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.DIRECT,
        mechanism="lower near-term physical availability",
        affected_variable="prompt crude availability",
        expected_effect=ExpectedEffect.UP,
        horizon_sessions=1,
        confidence=0.8,
        evidence_refs=("official-confirmation",),
    )

    with pytest.raises(ValueError, match="must end at target_ref"):
        TransmissionPath(
            path_id="unrelated-target-path",
            target_ref="industry:airlines",
            steps=(step,),
            counterevidence_refs=(),
            blockers=(),
            invalidation_conditions=("replacement supply offsets the outage",),
        )


def test_event_assessment_rejects_supporting_evidence_as_counterevidence() -> None:
    item = evidence("official-confirmation")
    envelope = EventEnvelope(
        envelope_id="envelope-1",
        event_id="event-1",
        as_of=at(3),
        evidence=(item,),
    )
    step = TransmissionStep(
        step_id="step-1",
        from_node="physical_supply",
        to_node="commodity:crude_benchmark",
        channel=TransmissionChannel.CAPACITY_COST_INVENTORY,
        directness=TransmissionDirectness.DIRECT,
        mechanism="lower near-term physical availability",
        affected_variable="prompt crude availability",
        expected_effect=ExpectedEffect.UP,
        horizon_sessions=1,
        confidence=0.8,
        evidence_refs=(item.evidence_id,),
    )
    path = TransmissionPath(
        path_id="contradictory-path",
        target_ref="commodity:crude_benchmark",
        steps=(step,),
        counterevidence_refs=(item.evidence_id,),
        blockers=(),
        invalidation_conditions=("replacement supply offsets the outage",),
    )

    with pytest.raises(ValueError, match="both supporting and counterevidence"):
        EventAssessment(
            assessment_id="assessment-1",
            envelope=envelope,
            archetype=EventArchetype.PHYSICAL_SUPPLY_LOGISTICS,
            revelation_mode=RevelationMode.UNSCHEDULED,
            stage=EventStage.CORROBORATED,
            route=route_event_assessment(envelope, mapping_known=True),
            expectation_delta=ExpectationDelta(
                baseline_source_ref=item.evidence_id,
                expected="normal throughput",
                observed="material temporary outage",
                direction=ExpectationDirection.NEGATIVE,
                confidence=0.9,
            ),
            paths=(path,),
            blockers=(),
        )
