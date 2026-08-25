import json
from pathlib import Path
from typing import Any, cast

from market_impact_agent.events import event_transmission_chronology_errors


def load_event(
    fixture: str = "examples/events/synthetic-energy-supply-shock.json",
) -> dict[str, Any]:
    payload = cast(
        object,
        json.loads(Path(fixture).read_text(encoding="utf-8")),
    )
    if not isinstance(payload, dict):
        raise TypeError("event fixture must be an object")
    return cast(dict[str, Any], payload)


def test_event_chronology_accepts_evidence_visible_by_as_of() -> None:
    assert event_transmission_chronology_errors(load_event()) == ()


def test_real_event_fixture_satisfies_point_in_time_rules() -> None:
    assert (
        event_transmission_chronology_errors(
            load_event("examples/events/real-abqaiq-geopolitical-supply-shock.json")
        )
        == ()
    )


def test_event_chronology_accepts_a_future_scheduled_occurrence() -> None:
    payload = load_event()
    payload["envelope"]["evidence"][0]["occurred_at"] = "2026-08-25T02:00:00Z"

    assert event_transmission_chronology_errors(payload) == ()


def test_event_chronology_rejects_future_visible_evidence() -> None:
    payload = load_event()
    payload["envelope"]["evidence"][0]["visible_at"] = "2026-08-24T02:06:00Z"
    payload["envelope"]["evidence"][0]["retrieved_at"] = "2026-08-24T02:07:00Z"

    assert event_transmission_chronology_errors(payload) == (
        "envelope.evidence[0].visible_at must not be after envelope.as_of",
    )


def test_event_validation_rejects_unknown_path_evidence() -> None:
    payload = load_event()
    payload["transmission_paths"][0]["steps"][0]["evidence_refs"] = ["future-source"]

    assert event_transmission_chronology_errors(payload) == (
        "transmission_paths[0] has unknown evidence references: future-source",
    )


def test_event_validation_requires_adjacent_transmission_steps() -> None:
    payload = load_event()
    payload["transmission_paths"][1]["steps"][1]["from"] = "unrelated_input"

    assert event_transmission_chronology_errors(payload) == (
        "transmission_paths[1].steps[1].from must match the previous step.to",
    )


def test_event_validation_requires_directness_to_match_step_position() -> None:
    payload = load_event()
    payload["transmission_paths"][1]["steps"][1]["directness"] = "fourth_order"

    assert event_transmission_chronology_errors(payload) == (
        "transmission_paths[1].steps[1].directness must be second_order for its position",
    )


def test_event_validation_requires_path_to_reach_target() -> None:
    payload = load_event()
    payload["transmission_paths"][0]["target_ref"] = "industry:unrelated"

    assert event_transmission_chronology_errors(payload) == (
        "transmission_paths[0].steps must end at target_ref",
    )


def test_event_validation_separates_supporting_and_counterevidence() -> None:
    payload = load_event()
    payload["transmission_paths"][0]["counterevidence_refs"].append("official-outage-confirmation")

    assert event_transmission_chronology_errors(payload) == (
        "transmission_paths[0] uses evidence as both supporting and counterevidence: "
        "official-outage-confirmation",
    )


def test_event_validation_rejects_unknown_revision_target() -> None:
    payload = load_event()
    payload["envelope"]["evidence"][1]["supersedes_id"] = "missing-report"

    assert event_transmission_chronology_errors(payload) == (
        "revision official-outage-confirmation supersedes unknown evidence: missing-report",
    )


def test_event_validation_requires_revision_to_keep_claim_id() -> None:
    payload = load_event()
    original = payload["envelope"]["evidence"][0]
    revision = payload["envelope"]["evidence"][1]
    revision["supersedes_id"] = original["evidence_id"]

    assert event_transmission_chronology_errors(payload) == (
        "revision official-outage-confirmation must retain claim_id from "
        "normal-throughput-baseline",
    )


def test_event_validation_requires_later_revision_visibility() -> None:
    payload = load_event()
    original = payload["envelope"]["evidence"][0]
    revision = payload["envelope"]["evidence"][1]
    original["claim_id"] = revision["claim_id"]
    original["supersedes_id"] = revision["evidence_id"]

    assert event_transmission_chronology_errors(payload) == (
        "revision normal-throughput-baseline must be visible after superseded evidence "
        "official-outage-confirmation",
    )


def test_event_validation_rejects_competing_direct_revisions() -> None:
    payload = load_event()
    original = payload["envelope"]["evidence"][0]
    first_revision = payload["envelope"]["evidence"][1]
    competing_revision = payload["envelope"]["evidence"][2]
    first_revision["claim_id"] = original["claim_id"]
    first_revision["supersedes_id"] = original["evidence_id"]
    competing_revision["claim_id"] = original["claim_id"]
    competing_revision["supersedes_id"] = original["evidence_id"]

    assert event_transmission_chronology_errors(payload) == (
        "evidence normal-throughput-baseline must have at most one direct revision",
    )


def test_event_validation_requires_values_for_known_expectation_delta() -> None:
    payload = load_event()
    payload["expectation_delta"]["expected"] = None

    assert event_transmission_chronology_errors(payload) == (
        "known expectation_delta requires non-null baseline_source_ref, expected, and observed",
    )


def test_event_validation_allows_unknown_expectation_delta() -> None:
    payload = load_event()
    payload["expectation_delta"] = {
        "baseline_source_ref": None,
        "expected": None,
        "observed": None,
        "direction": "unknown",
        "confidence": 0,
    }

    assert event_transmission_chronology_errors(payload) == ()
