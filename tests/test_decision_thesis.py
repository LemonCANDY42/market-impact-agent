from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.decision_thesis import (
    BaseCaseDirection,
    HorizonBand,
    ResearchThesisV1,
    ReviewCadence,
    ReviewScheduleV1,
    parse_research_thesis,
    research_thesis_text_normalizations,
)
from market_impact_agent.research_thesis_runtime import theses_semantically_disagree

NOW = datetime(2026, 9, 4, 8, 0, tzinfo=UTC)


def _model_answer() -> dict[str, object]:
    return {
        "horizon_band": "tactical",
        "primary_horizon_sessions": 10,
        "base_case_direction": "up",
        "thesis": "The incremental earnings evidence supports a tactical rerating.",
        "priced_in_assessment": "The prior price reflected slower growth, not the new beat.",
        "transmission": ["earnings surprise -> estimate revision -> valuation rerating"],
        "counter_scenario": "A margin reset could overwhelm the revenue surprise.",
        "evidence_refs": ["earnings-1"],
        "counterevidence_refs": ["margin-1"],
        "invalidation_conditions": ["Management cuts the new revenue guide."],
        "review_after_sessions": 3,
        "typed_unknowns": ["intraday liquidity is not supplied"],
    }


def test_research_thesis_injects_identity_and_validates_dynamic_horizon() -> None:
    thesis = parse_research_thesis(
        _model_answer(),
        root_event_id="event-earnings",
        thesis_epoch="epoch-1",
        as_of=NOW,
        evidence_ids=frozenset({"earnings-1", "margin-1"}),
    )

    assert thesis.horizon_band is HorizonBand.TACTICAL
    assert thesis.base_case_direction is BaseCaseDirection.UP
    assert thesis.primary_horizon_sessions == 10
    assert validate_agent_contract(thesis.to_dict(), "research-thesis-v1.schema.json") == ()

    legacy_mismatch = _model_answer()
    legacy_mismatch["primary_horizon_sessions"] = 20
    legacy_mismatch["counterevidence_refs"] = ["earnings-1"]
    derived = parse_research_thesis(
        legacy_mismatch,
        root_event_id="event-earnings",
        thesis_epoch="epoch-2",
        as_of=NOW,
        evidence_ids=frozenset({"earnings-1", "margin-1"}),
    )
    assert derived.horizon_band is HorizonBand.SWING
    assert derived.evidence_refs == ("earnings-1",)
    assert derived.counterevidence_refs == ("earnings-1",)

    current = _model_answer()
    current.pop("horizon_band")
    current["primary_horizon_sessions"] = 3
    immediate = parse_research_thesis(
        current,
        root_event_id="event-earnings",
        thesis_epoch="epoch-3",
        as_of=NOW,
        evidence_ids=frozenset({"earnings-1", "margin-1"}),
    )
    assert immediate.horizon_band is HorizonBand.IMMEDIATE

    singleton_lists = _model_answer()
    singleton_lists["transmission"] = "surprise -> revisions -> valuation"
    singleton_lists["invalidation_conditions"] = "Guidance is withdrawn."
    normalized = parse_research_thesis(
        singleton_lists,
        root_event_id="event-earnings",
        thesis_epoch="epoch-4",
        as_of=NOW,
        evidence_ids=frozenset({"earnings-1", "margin-1"}),
    )
    assert normalized.transmission == ("surprise -> revisions -> valuation",)
    assert normalized.invalidation_conditions == ("Guidance is withdrawn.",)


def test_research_thesis_trims_only_narrative_formatting_and_records_it() -> None:
    answer = _model_answer()
    answer["thesis"] = "  The incremental evidence supports a rerating. "
    answer["transmission"] = ["earnings -> revisions -> rerating "]
    answer["typed_unknowns"] = [" short-term positioning is unknown "]

    thesis = parse_research_thesis(
        answer,
        root_event_id="event-earnings",
        thesis_epoch="epoch-whitespace",
        as_of=NOW,
        evidence_ids=frozenset({"earnings-1", "margin-1"}),
    )

    assert thesis.thesis == "The incremental evidence supports a rerating."
    assert thesis.transmission == ("earnings -> revisions -> rerating",)
    assert thesis.typed_unknowns == ("short-term positioning is unknown",)
    assert research_thesis_text_normalizations(answer) == (
        {"path": "thesis", "operation": "trim_surrounding_whitespace"},
        {"path": "transmission[0]", "operation": "trim_surrounding_whitespace"},
        {"path": "typed_unknowns[0]", "operation": "trim_surrounding_whitespace"},
    )

    bad_identity = dict(answer)
    bad_identity["evidence_refs"] = ["earnings-1 "]
    with pytest.raises(ValueError, match="trimmed text"):
        parse_research_thesis(
            bad_identity,
            root_event_id="event-earnings",
            thesis_epoch="epoch-whitespace",
            as_of=NOW,
            evidence_ids=frozenset({"earnings-1", "margin-1"}),
        )


def test_analysis_cannot_abstain_or_echo_harness_identity() -> None:
    abstain = _model_answer()
    abstain["decision"] = "abstain"
    with pytest.raises(ValueError, match="unauthorized"):
        parse_research_thesis(
            abstain,
            root_event_id="event-earnings",
            thesis_epoch="epoch-1",
            as_of=NOW,
            evidence_ids=frozenset({"earnings-1", "margin-1"}),
        )

    identity = _model_answer()
    identity["root_event_id"] = "invented"
    with pytest.raises(ValueError, match="unauthorized"):
        parse_research_thesis(
            identity,
            root_event_id="event-earnings",
            thesis_epoch="epoch-1",
            as_of=NOW,
            evidence_ids=frozenset({"earnings-1", "margin-1"}),
        )


@pytest.mark.parametrize(
    ("horizon", "expected"),
    [
        (1, (1,)),
        (3, (1, 2, 3)),
        (5, (1, 3, 5)),
        (10, (1, 3, 5, 10)),
        (20, (5, 10, 20)),
        (60, (5, 10, 20, 40, 60)),
    ],
)
def test_scheduled_review_offsets_are_deterministic(
    horizon: int, expected: tuple[int, ...]
) -> None:
    sessions = tuple(date(2026, 9, 5) + timedelta(days=index) for index in range(80))
    schedule = ReviewScheduleV1.build(
        root_event_id="event-1",
        thesis_epoch="epoch-1",
        cadence=ReviewCadence.SCHEDULED,
        primary_horizon_sessions=horizon,
        future_trading_sessions=sessions,
        created_at=NOW,
    )

    assert tuple(item.session_offset for item in schedule.review_points) == expected
    assert (
        validate_agent_contract(schedule.to_dict(), "decision-review-schedule-v1.schema.json") == ()
    )


def test_event_driven_schedule_has_only_forced_terminal_and_three_wakes() -> None:
    sessions = tuple(date(2026, 9, 5) + timedelta(days=index) for index in range(10))
    schedule = ReviewScheduleV1.build(
        root_event_id="event-1",
        thesis_epoch="epoch-1",
        cadence=ReviewCadence.MATERIAL_EVENT_DRIVEN,
        primary_horizon_sessions=5,
        future_trading_sessions=sessions,
        created_at=NOW,
    )

    assert tuple(item.session_offset for item in schedule.review_points) == (5,)
    assert schedule.maximum_intermediate_wakes == 3


def test_old_five_session_thesis_remains_a_plain_replayable_contract() -> None:
    thesis = ResearchThesisV1(
        root_event_id="new-event",
        thesis_epoch="new-epoch",
        as_of=NOW,
        horizon_band=HorizonBand.TACTICAL,
        primary_horizon_sessions=5,
        base_case_direction=BaseCaseDirection.RANGEBOUND,
        thesis="New dynamic contracts do not reinterpret old Judgment artifacts.",
        priced_in_assessment="The old outcome is not supplied.",
        transmission=("new evidence -> balanced market response",),
        counter_scenario="A material event may break the range.",
        evidence_refs=("evidence-1",),
        counterevidence_refs=(),
        invalidation_conditions=("A material price or fact threshold is crossed.",),
        review_after_sessions=1,
    )
    assert thesis.primary_horizon_sessions == 5


def test_conditional_judge_triggers_on_direction_or_horizon_not_wording() -> None:
    thesis = parse_research_thesis(
        _model_answer(),
        root_event_id="event-earnings",
        thesis_epoch="analyst-a",
        as_of=NOW,
        evidence_ids=frozenset({"earnings-1", "margin-1"}),
    )
    wording_only = replace(thesis, thesis_epoch="analyst-b", thesis="Equivalent wording.")
    opposing = replace(
        wording_only,
        base_case_direction=BaseCaseDirection.DOWN,
        thesis="The margin risk dominates.",
    )
    shorter = replace(wording_only, primary_horizon_sessions=5, review_after_sessions=1)

    assert not theses_semantically_disagree(thesis, wording_only)
    assert theses_semantically_disagree(thesis, opposing)
    assert theses_semantically_disagree(thesis, shorter)
