# pyright: reportPrivateUsage=false
from __future__ import annotations

import hmac
import io
import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast
from urllib.parse import urlencode
from urllib.request import Request

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import _PrivilegedEventSink
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.sse_fund_suspension import (
    capture_sse_fund_suspensions,
    freeze_sse_fund_suspensions,
    reopen_sse_fund_suspensions,
)

NOW = datetime(2026, 9, 5, tzinfo=UTC)


def _receipt(
    store: LocalDataSnapshotStore,
    *,
    start: str = "19901219",
    end: str = "20241231",
    code: str = "510500",
    rows: list[dict[str, object]] | None = None,
    defect: str | None = None,
) -> str:
    rows = [] if rows is None else rows
    parameters = {
        "isPagination": "true",
        "sqlId": "SSE_PL_JYTS_TFPXX_JJ",
        "secCode": code,
        "stopReason": "",
        "order": "startStopDate|desc,secCode|asc",
        "startDate": start,
        "endDate": end,
        "pageHelp.pageSize": "25",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "1",
        "jsonCallBack": "jsonpCallback1",
    }
    if defect == "extra_filter":
        parameters["businessType"] = "ignored-records"
    request = {
        "url": "https://query.sse.com.cn/sseQuery/commonSoaQuery.do?" + urlencode(parameters),
        "method": "GET",
        "headers": {"Referer": "https://www.sse.com.cn/disclosure/dealinstruc/suspension/fund/"},
    }
    payload: dict[str, object] = {
        "sqlId": "SSE_PL_JYTS_TFPXX_JJ",
        "actionErrors": [],
        "actionMessages": [],
        "fieldErrors": {},
        "result": rows,
        "pageHelp": {"data": rows, "total": len(rows), "pageNo": 1, "pageCount": 1 if rows else 0},
    }
    if defect == "source_failure":
        payload["success"] = "false"
    if defect == "missing_pages":
        payload["pageHelp"] = {"data": rows, "total": 26, "pageNo": 1, "pageCount": 2}
    raw = ("(" + json.dumps(payload) + ")").encode()
    event_id = "sse-fund-suspension-v1." + canonical_hash(request) + ".received"
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id="source", config_hash=canonical_hash("source"), created_at=NOW)
    key = (store.root / ".harness-event-hmac.key").read_bytes()
    events = _PrivilegedEventSink(
        journal=journal,
        authority_id=store.harness_authority_id,
        signer=lambda value: hmac.new(key, value, sha256).hexdigest(),
    )
    events.append(
        run_id="source",
        event_id=event_id,
        event_type="research.sse-fund-suspension.received",
        observed_at=NOW,
        payload={
            "request": request,
            "raw_hash": store.put_raw(raw),
            "retrieved_at": NOW.isoformat(),
            "http_status": 200,
            "response_url": request["url"],
            "size_limit_exceeded": False,
        },
    )
    return event_id


def test_complete_partitions_reopen_and_halt_intervals_remain_explicit_gaps(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    first = _receipt(store, end="20140925")
    second = _receipt(
        store,
        start="20140926",
        rows=[
            {
                "secCode": "510500",
                "startStopDate": "20150413",
                "endStopDate": "20150414",
                "dateSource": "0",
                "startType": "TR",
                "stopTime": " ",
            }
        ],
    )
    artifact = freeze_sse_fund_suspensions(store=store, receipt_event_ids=(second, first))
    assert artifact == freeze_sse_fund_suspensions(store=store, receipt_event_ids=(first, second))
    reopened = reopen_sse_fund_suspensions(
        store=store, artifact_hashes=(artifact,), symbol="510500.SH", session=date(2024, 9, 24)
    )
    assert reopened.halted is False and not reopened.gaps
    assert artifact in reopened.source_record_hashes
    for day in (date(2015, 4, 13), date(2015, 4, 14)):
        blocked = reopen_sse_fund_suspensions(
            store=store, artifact_hashes=(artifact,), symbol="510500.SH", session=day
        )
        assert blocked.halted is None
        assert blocked.gaps == ("fund_halt_record_requires_session_mapping",)
    for symbol, day in (("510300.SH", date(2024, 9, 24)), ("510500.SH", date(2025, 1, 2))):
        assert (
            reopen_sse_fund_suspensions(
                store=store, artifact_hashes=(artifact,), symbol=symbol, session=day
            ).halted
            is None
        )


@pytest.mark.parametrize("defect", ["source_failure", "missing_pages", "extra_filter"])
def test_failure_or_filtered_or_incomplete_source_cannot_prove_absence(
    tmp_path: Path, defect: str
) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    event_id = _receipt(store, defect=defect)
    with pytest.raises(ValueError):
        freeze_sse_fund_suspensions(store=store, receipt_event_ids=(event_id,))


def test_left_truncated_history_cannot_hide_an_earlier_ongoing_halt(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    event_id = _receipt(store, start="20140926")
    with pytest.raises(ValueError, match="consecutive history"):
        freeze_sse_fund_suspensions(store=store, receipt_event_ids=(event_id,))


def test_coverage_reopens_exact_raw_graph_and_cannot_promote_current_receipt(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    event_id = _receipt(store)
    artifact = freeze_sse_fund_suspensions(store=store, receipt_event_ids=(event_id,))
    value = cast(dict[str, object], store.artifacts.read_json(artifact))
    value["strict_pit_accepted"] = True
    forged = store.artifacts.put_json(value).content_hash
    with pytest.raises(PermissionError, match="historical PIT"):
        reopen_sse_fund_suspensions(
            store=store, artifact_hashes=(forged,), symbol="510500.SH", session=date(2024, 9, 24)
        )
    value["strict_pit_accepted"] = False
    value["receipt_hashes"] = ["0" * 64]
    forged = store.artifacts.put_json(value).content_hash
    with pytest.raises(PermissionError, match="source receipts"):
        reopen_sse_fund_suspensions(
            store=store, artifact_hashes=(forged,), symbol="510500.SH", session=date(2024, 9, 24)
        )


def test_complete_official_negative_coverage_reaches_preopen_and_executor(tmp_path: Path) -> None:
    from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
    from tests.test_historical_ashare_inputs import _source

    original = _source(tmp_path, etf_halt=False)
    day = date(2025, 1, 3)
    assert "halt_status_unverified" in original.session("510300.SH", day).gaps
    event_id = _receipt(original.store, code="510300", end="20250103")
    artifact = freeze_sse_fund_suspensions(store=original.store, receipt_event_ids=(event_id,))
    market = HistoricalAShareInputs(
        store=original.store,
        snapshot_ids=original.snapshot_ids,
        rule_artifact_hashes=original.rule_artifact_hashes,
        policy=original.policy,
        fund_halt_artifact_hashes=(artifact,),
    )
    evidence = market.reopen_security("510300.SH", datetime(2025, 1, 3, 1, 25, tzinfo=UTC))
    assert evidence is not None and evidence.halted is False and not evidence.gaps
    session = market.session("510300.SH", day)
    assert session.execution_ready and artifact in session.source_record_hashes
    assert market.with_snapshots(()).session("510300.SH", day) == session


def test_historical_binding_admits_fund_graph_once_and_rebinding_rechecks_raw(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from market_impact_agent import sse_fund_suspension as source
    from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
    from tests.test_historical_ashare_inputs import _source

    original = _source(tmp_path, etf_halt=False)
    event_id = _receipt(original.store, code="510300", end="20250103")
    artifact = freeze_sse_fund_suspensions(store=original.store, receipt_event_ids=(event_id,))
    reads: list[str] = []
    read_query = source._read_query

    def counted(store: LocalDataSnapshotStore, journal: RunJournal, identity: str):
        reads.append(identity)
        return read_query(store, journal, identity)

    monkeypatch.setattr(source, "_read_query", counted)
    market = HistoricalAShareInputs(
        store=original.store,
        snapshot_ids=original.snapshot_ids,
        rule_artifact_hashes=original.rule_artifact_hashes,
        policy=original.policy,
        fund_halt_artifact_hashes=(artifact,),
    )
    day = date(2025, 1, 3)
    expected = market.session("510300.SH", day)
    for _ in range(3):
        assert market.session("510300.SH", day) == expected
        assert market.reopen_security("510300.SH", datetime(2025, 1, 3, 1, 25, tzinfo=UTC))
    assert reads == [event_id]
    rebound = market.with_snapshots(())
    assert rebound.session("510300.SH", day) == expected
    assert reads == [event_id, event_id]
    graph = cast(dict[str, object], original.store.artifacts.read_json(artifact))
    raw_hash = cast(list[str], graph["receipt_hashes"])[0]
    raw_path = original.store.artifacts.get(raw_hash, media_type="application/octet-stream").path
    raw_path.write_bytes(b"tampered")
    assert market.session("510300.SH", day) == expected
    with pytest.raises(ValueError, match="artifact content does not match its identity"):
        market.with_snapshots(()).session("510300.SH", day)
    with pytest.raises(ValueError, match="artifact content does not match its identity"):
        reopen_sse_fund_suspensions(
            store=original.store, artifact_hashes=(artifact,), symbol="510300.SH", session=day
        )


def test_unsigned_source_receipt_cannot_enter_execution_authority(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id="forged", config_hash=canonical_hash("forged"), created_at=NOW)
    with pytest.raises(PermissionError, match="root-authenticated"):
        journal.append(
            run_id="forged",
            event_id="fabricated-receipt",
            event_type="research.sse-fund-suspension.received",
            observed_at=NOW,
            payload={},
        )


def test_baseline_cannot_replay_under_replacement_fund_halt_coverage(tmp_path: Path) -> None:
    from market_impact_agent.continuous_baselines import evaluate_continuous_baseline_window
    from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
    from tests.test_continuous_baselines import _registered_window, _seed
    from tests.test_historical_ashare_inputs import _source

    original = _source(tmp_path)
    event_id = _receipt(original.store, code="510300", end="20250103")
    coverage = freeze_sse_fund_suspensions(store=original.store, receipt_event_ids=(event_id,))
    replacement = HistoricalAShareInputs(
        store=original.store,
        snapshot_ids=original.snapshot_ids,
        rule_artifact_hashes=original.rule_artifact_hashes,
        policy=original.policy,
        fund_halt_artifact_hashes=(coverage,),
    )
    reports = [
        evaluate_continuous_baseline_window(
            registration_id="fixture-registration",
            baseline_id="same_initial_account_hold",
            registered_window=_registered_window(sessions=(date(2025, 1, 3),)),
            historical_inputs=source,
            account_seed=_seed(),
            state_root=tmp_path / "state",
        )
        for source in (original, replacement)
    ]
    assert all(report["status"] == "complete" for report in reports)
    assert reports[0]["source_binding_hash"] != reports[1]["source_binding_hash"]
    assert len(list((tmp_path / "state").rglob("account.jsonl"))) == 2


@pytest.mark.parametrize("crash", [False, True])
def test_source_capture_signs_receipt_and_never_blindly_reissues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash: bool
) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id="source", config_hash=canonical_hash("source"), created_at=NOW)
    budget = ModelBudget(journal, "source", 1, 1)
    opened: list[str] = []
    payload: dict[str, object] = {
        "sqlId": "SSE_PL_JYTS_TFPXX_JJ",
        "actionErrors": [],
        "actionMessages": [],
        "fieldErrors": {},
        "result": [],
        "pageHelp": {"data": [], "total": 0, "pageNo": 1, "pageCount": 0},
    }

    class Response(io.BytesIO):
        status = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = {"Content-Type": "application/json"}
            super().__init__(("(" + json.dumps(payload) + ")").encode())

    def fetch(request: Request, *, timeout: int) -> Response:
        assert timeout == 30 and request.get_header("Referer")
        opened.append(request.full_url)
        if crash:
            raise OSError("transport result is unknown")
        return Response(request.full_url)

    monkeypatch.setattr("market_impact_agent.sse_fund_suspension.urllib.request.urlopen", fetch)

    def capture() -> str:
        return capture_sse_fund_suspensions(
            store=store,
            parent_budget=budget,
            symbol="510500.SH",
            start=date(1990, 12, 19),
            end=date(2024, 12, 31),
        )

    if crash:
        with pytest.raises(OSError):
            capture()
        with pytest.raises(PermissionError, match="reconciliation"):
            capture()
    else:
        event_id = capture()
        assert capture() == event_id
        assert freeze_sse_fund_suspensions(store=store, receipt_event_ids=(event_id,))
    assert len(opened) == 1
