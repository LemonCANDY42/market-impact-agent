from market_impact_agent.events import event_transmission_chronology_errors


def test_event_chronology_accepts_evidence_visible_by_as_of() -> None:
    assert (
        event_transmission_chronology_errors(
            {
                "event_time": "2026-08-24T02:00:00Z",
                "first_publication_time": "2026-08-24T02:03:00Z",
                "as_of": "2026-08-24T02:05:00Z",
            }
        )
        == ()
    )


def test_event_chronology_rejects_future_event_and_evidence() -> None:
    assert event_transmission_chronology_errors(
        {
            "event_time": "2026-08-24T02:06:00Z",
            "first_publication_time": "2026-08-24T02:07:00Z",
            "as_of": "2026-08-24T02:05:00Z",
        }
    ) == (
        "event_time must not be after as_of",
        "first_publication_time must not be after as_of",
    )
