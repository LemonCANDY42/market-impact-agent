"""Reopen complete official fund-suspension queries as modeled historical evidence.

The current receipt never becomes historical PIT authority. Absence is usable
only after every source page and the continuous query range have been verified.
An overlapping record stays a gap until its trading/session semantics are mapped.
"""

from __future__ import annotations

import hmac
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import cast
from urllib.parse import parse_qs, urlencode, urlsplit

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    _PrivilegedEventSink,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.runtime_store import RunJournal

_SCHEMA = "market-impact.sse-fund-suspension-coverage.v1"
_QUERY = "SSE_PL_JYTS_TFPXX_JJ"
_PAGE = "https://www.sse.com.cn/disclosure/dealinstruc/suspension/fund/"
_HISTORY_START = date(1990, 12, 19)


def capture_sse_fund_suspensions(
    *,
    store: LocalDataSnapshotStore,
    parent_budget: ModelBudget,
    symbol: str,
    start: date,
    end: date,
) -> str:
    """One exact source task outside the model Run, using shared durable claims/CAS.

    An unsigned diagnostic capture cannot be promoted. This source version creates
    authenticated receipts directly from its HTTP response. Known receipts replay;
    unknown post-dispatch state requires reconciliation instead of another request.
    """
    if (
        parent_budget.journal.path != store.index_path
        or len(symbol) != 9
        or not symbol.endswith(".SH")
        or not symbol[:6].isdigit()
        or not _HISTORY_START <= start <= end <= datetime.now(UTC).date()
    ):
        raise ValueError("fund suspension acquisition requires exact shared-root historical scope")
    params = {
        "isPagination": "true",
        "sqlId": _QUERY,
        "secCode": symbol[:6],
        "stopReason": "",
        "order": "startStopDate|desc,secCode|asc",
        "startDate": start.strftime("%Y%m%d"),
        "endDate": end.strftime("%Y%m%d"),
        "pageHelp.pageSize": "25",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "1",
        "jsonCallBack": "jsonpCallback1",
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": _PAGE, "Accept": "*/*"}
    request: dict[str, object] = {
        "url": "https://query.sse.com.cn/sseQuery/commonSoaQuery.do?" + urlencode(params),
        "headers": headers,
        "method": "GET",
    }
    prefix = "sse-fund-suspension-v1." + canonical_hash(request)
    journal = RunJournal.authoritative(store)
    claim = journal.try_claim_run(prefix)
    if claim is None:
        raise LookupError("fund suspension source query is already owned")
    key = (store.root / ".harness-event-hmac.key").read_bytes()
    events = _PrivilegedEventSink(
        journal=journal,
        authority_id=store.harness_authority_id,
        signer=lambda value: hmac.new(key, value, sha256).hexdigest(),
    )
    with claim:
        received = journal.event(prefix + ".received")
        if received is not None:
            _read_query(store, journal, received.event_id)
            return received.event_id
        if journal.event(prefix + ".started") is not None:
            raise PermissionError("unknown fund suspension receipt requires reconciliation")
        parent_budget.check_cancel()
        events.append(
            run_id=parent_budget.owner_run_id,
            event_id=prefix + ".started",
            event_type="research.sse-fund-suspension.started",
            observed_at=datetime.now(UTC),
            payload=request,
        )
        try:
            response = urllib.request.urlopen(
                urllib.request.Request(str(request["url"]), headers=headers), timeout=30
            )
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read(8_000_001)
            raw_hash = store.put_raw(raw)
            receipt: dict[str, object] = {
                "request": request,
                "raw_hash": raw_hash,
                "retrieved_at": datetime.now(UTC).isoformat(),
                "http_status": response.status,
                "content_type": response.headers.get("Content-Type"),
                "response_url": str(response.url),
                "size_limit_exceeded": len(raw) > 8_000_000,
            }
        events.append(
            run_id=parent_budget.owner_run_id,
            event_id=prefix + ".received",
            event_type="research.sse-fund-suspension.received",
            observed_at=datetime.now(UTC),
            payload=receipt,
        )
        _read_query(store, journal, prefix + ".received")
        return prefix + ".received"


@dataclass(frozen=True)
class FundSuspensionEvidence:
    halted: bool | None
    gaps: tuple[str, ...]
    source_record_hashes: tuple[str, ...]


def freeze_sse_fund_suspensions(
    *,
    store: LocalDataSnapshotStore,
    receipt_event_ids: tuple[str, ...],
) -> str:
    """Bind existing durable exact receipts; never fetch or infer empty coverage."""
    if not receipt_event_ids or len(set(receipt_event_ids)) != len(receipt_event_ids):
        raise ValueError("fund suspension coverage requires unique source receipts")
    journal = RunJournal.authoritative(store)
    queries = [_read_query(store, journal, event_id) for event_id in receipt_event_ids]
    symbols = {item[0] for item in queries}
    if len(symbols) != 1:
        raise ValueError("fund suspension coverage must bind one exact symbol")
    _coverage_end(queries)
    artifact = {
        "schema_version": _SCHEMA,
        "symbol": next(iter(symbols)),
        "receipt_event_ids": sorted(receipt_event_ids),
        "receipt_hashes": sorted(item[4] for item in queries),
        "pit_lane": "modeled_historical",
        "strict_pit_accepted": False,
    }
    return store.artifacts.put_json(artifact).content_hash


def reopen_sse_fund_suspensions(
    *,
    store: LocalDataSnapshotStore,
    artifact_hashes: tuple[str, ...],
    symbol: str,
    session: date,
) -> FundSuspensionEvidence:
    return read_sse_fund_suspensions(
        store=store, artifact_hashes=artifact_hashes, symbol=symbol
    ).project(symbol=symbol, session=session)


@dataclass(frozen=True)
class _VerifiedCoverage:
    symbol: str
    end: date
    intervals: tuple[tuple[date, date | None], ...]
    hashes: tuple[str, ...]


@dataclass(frozen=True)
class VerifiedFundSuspensions:
    """Immutable, authenticated source graph; session projections perform no I/O."""

    coverages: tuple[_VerifiedCoverage, ...]

    def project(self, *, symbol: str, session: date) -> FundSuspensionEvidence:
        matches = tuple(
            coverage
            for coverage in self.coverages
            if coverage.symbol == symbol and _HISTORY_START <= session <= coverage.end
        )
        if not matches:
            return FundSuspensionEvidence(None, ("fund_halt_coverage_missing",), ())
        hashes = tuple(sorted({value for coverage in matches for value in coverage.hashes}))
        for coverage in matches:
            for start, end in coverage.intervals:
                if start <= session and (end is None or session <= end):
                    # Neither a reason nor a blank time proves a full-session halt.
                    return FundSuspensionEvidence(
                        None, ("fund_halt_record_requires_session_mapping",), hashes
                    )
        return FundSuspensionEvidence(False, (), hashes)


def read_sse_fund_suspensions(
    *,
    store: LocalDataSnapshotStore,
    artifact_hashes: tuple[str, ...],
    symbol: str | None = None,
) -> VerifiedFundSuspensions:
    """Verify a fresh CAS/receipt graph before admitting immutable projections."""
    journal = RunJournal.authoritative(store)
    coverages: list[_VerifiedCoverage] = []
    for artifact_hash in artifact_hashes:
        artifact = cast(dict[str, object], store.artifacts.read_json(artifact_hash))
        if artifact.get("schema_version") != _SCHEMA:
            raise ValueError("unsupported fund suspension coverage schema")
        coverage_symbol = str(artifact.get("symbol"))
        if symbol is not None and coverage_symbol != symbol:
            continue
        if artifact.get("strict_pit_accepted") is not False or artifact.get("pit_lane") != (
            "modeled_historical"
        ):
            raise PermissionError("current fund suspension receipts cannot gain historical PIT")
        event_ids = cast(list[str], artifact["receipt_event_ids"])
        if event_ids != sorted(set(event_ids)) or not event_ids:
            raise ValueError("fund suspension source receipt identities are not canonical")
        queries = [_read_query(store, journal, event_id) for event_id in event_ids]
        if {item[0] for item in queries} != {coverage_symbol} or sorted(
            item[4] for item in queries
        ) != artifact["receipt_hashes"]:
            raise PermissionError("fund suspension coverage differs from its source receipts")
        end = _coverage_end(queries)
        intervals = tuple(
            (
                _date(row["startStopDate"]),
                None if not str(row.get("endStopDate", "")).strip() else _date(row["endStopDate"]),
            )
            for _, _, _, rows, _ in queries
            for row in rows
        )
        hashes = tuple(sorted({artifact_hash, *(item[4] for item in queries)}))
        coverages.append(_VerifiedCoverage(coverage_symbol, end, intervals, hashes))
    return VerifiedFundSuspensions(tuple(coverages))


def _coverage_end(
    queries: list[tuple[str, date, date, list[dict[str, object]], str]],
) -> date:
    expected = _HISTORY_START
    for _, start, end, _, _ in sorted(queries, key=lambda item: item[1]):
        if start != expected or end < start:
            raise ValueError(
                "fund suspension queries must cover consecutive history from inception"
            )
        expected = end + timedelta(days=1)
    return expected - timedelta(days=1)


def _read_query(
    store: LocalDataSnapshotStore, journal: RunJournal, event_id: str
) -> tuple[str, date, date, list[dict[str, object]], str]:
    event = journal.event(event_id)
    if event is None or event.event_type != "research.sse-fund-suspension.received":
        raise PermissionError("fund suspension receipt is not in the authority Journal")
    receipt = event.payload
    request = cast(dict[str, object], receipt["request"])
    if event_id != "sse-fund-suspension-v1." + canonical_hash(request) + ".received":
        raise PermissionError("fund suspension receipt identity differs from its exact request")
    headers = cast(dict[str, str], request["headers"])
    url = urlsplit(str(request["url"]))
    if (
        request.get("method") != "GET"
        or url.scheme != "https"
        or url.netloc != "query.sse.com.cn"
        or url.path != "/sseQuery/commonSoaQuery.do"
        or url.fragment
        or headers.get("Referer") != _PAGE
        or receipt.get("http_status") != 200
        or receipt.get("response_url") != request["url"]
        or receipt.get("size_limit_exceeded") is not False
    ):
        raise PermissionError("fund suspension receipt has an unaccepted official route")
    params = parse_qs(url.query, keep_blank_values=True)
    if any(len(values) != 1 for values in params.values()):
        raise ValueError("fund suspension query contains ambiguous parameters")
    args = {key: values[0] for key, values in params.items()}
    if (
        set(args)
        != {
            "isPagination",
            "sqlId",
            "secCode",
            "stopReason",
            "order",
            "startDate",
            "endDate",
            "pageHelp.pageSize",
            "pageHelp.pageNo",
            "pageHelp.beginPage",
            "pageHelp.cacheSize",
            "pageHelp.endPage",
            "jsonCallBack",
        }
        or args.get("sqlId") != _QUERY
        or args.get("stopReason") != ""
        or args.get("isPagination") != "true"
        or args.get("pageHelp.pageNo") != "1"
        or args.get("pageHelp.pageSize") != "25"
        or any(
            args.get(key) != "1"
            for key in ("pageHelp.beginPage", "pageHelp.cacheSize", "pageHelp.endPage")
        )
    ):
        raise ValueError("fund suspension source must retain the complete unfiltered query")
    code = args.get("secCode", "")
    if len(code) != 6 or not code.isdigit():
        raise ValueError("fund suspension source requires one concrete Shanghai fund")
    start, end = _date(args["startDate"]), _date(args["endDate"])
    received_at = datetime.fromisoformat(str(receipt["retrieved_at"]))
    if received_at.tzinfo is None or end > received_at.date() or start > end:
        raise ValueError("fund suspension query exceeds its actual receipt")
    raw_hash = str(receipt["raw_hash"])
    raw = store.artifacts.get(raw_hash, media_type="application/octet-stream").path.read_bytes()
    text = raw.decode("utf-8").strip()
    # Source returns a JSONP envelope even when the callback is registered.
    callback = args.get("jsonCallBack", "")
    prefixes = ("(", callback + "(") if callback else ("(",)
    if not any(text.startswith(prefix) for prefix in prefixes) or not text.rstrip(";").endswith(
        ")"
    ):
        raise ValueError("unexpected fund suspension response envelope")
    payload = cast(dict[str, object], json.loads(text[text.index("{") : text.rindex("}") + 1]))
    if (
        payload.get("sqlId") != _QUERY
        or payload.get("success") in (False, "false")
        or payload.get("errorType")
        or payload.get("actionErrors") != []
        or payload.get("actionMessages") != []
        or payload.get("fieldErrors") != {}
    ):
        raise ValueError("failed official fund query cannot establish absence")
    page = cast(dict[str, object], payload["pageHelp"])
    if not isinstance(payload["result"], list):
        raise ValueError("fund suspension source result is not a row list")
    rows = cast(list[dict[str, object]], payload["result"])
    if (
        page.get("data") != rows
        or type(page.get("total")) is not int
        or page["total"] != len(rows)
        or len(rows) > 25
        or page.get("pageNo") != 1
        or page.get("pageCount") != (1 if rows else 0)
    ):
        raise ValueError("fund suspension source pages are incomplete or inconsistent")
    for row in rows:
        row_start = _date(row["startStopDate"])
        row_end = None if not str(row.get("endStopDate", "")).strip() else _date(row["endStopDate"])
        if (
            row.get("secCode") != code
            or not start <= row_start <= end
            or (row_end is not None and row_end < row_start)
        ):
            raise ValueError("fund suspension record differs from its symbol/date range")
    return code + ".SH", start, end, rows, raw_hash


def _date(value: object) -> date:
    return datetime.strptime(str(value), "%Y%m%d").date()
