"""Candidate identity from signed native calls and verified source receipts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataPITLane
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchContinuation
from market_impact_agent.pi_execution import native_turn
from market_impact_agent.research_thesis_runtime import ResearchThesisAuthority


@dataclass(frozen=True, slots=True)
class NativeCandidateProof:
    symbol: str
    api: str
    snapshot_id: str
    provenance: dict[str, object]


def prove_native_candidates(
    authority: ResearchThesisAuthority,
    acquisition: OnDemandResearch,
    results: tuple[ResearchContinuation, ...] = (),
    *,
    excluded_targets: tuple[str, ...] = (),
    policy: str = "native-profile-candidates-v3",
) -> tuple[NativeCandidateProof, ...]:
    """Reopen signed evidence; a model-mentioned symbol alone never proves identity.

    Both a cache hit and a fulfilled acquisition must have the matching native
    profile call and durable tool result. Excluded holdings do not consume the cap.
    """
    authority.replay(acquisition.run_id)
    record = authority.journal.get_run(acquisition.run_id)
    binding = _object(authority.store.artifacts.read_json(record.config_hash))
    owner = _object(binding["budget_owner"])
    if (
        owner.get("run_id") != acquisition.budget.owner_run_id
        or owner.get("binding") != acquisition.budget.binding
        or owner.get("journal_path") != str(acquisition.budget.journal.path)
        or binding.get("account_scope") != authority.account_scope
        or binding.get("arm_id") != authority.arm_id
        or binding.get("experiment_id") != authority.experiment_id
    ):
        raise PermissionError("candidate proof crosses signed research ownership")
    profile = _object(binding["profile"])
    confirmed: dict[str, NativeCandidateProof] = {}
    identity_hashes: dict[str, str] = {}
    for event in authority.journal.events(acquisition.run_id):
        if event.event_type != "pi.role.response.completed":
            continue
        native_hash = str(event.payload["artifact_hash"])
        turn = native_turn(
            _object(authority.store.artifacts.read_json(native_hash)), str(profile["model"])
        )
        for call in turn.tool_calls:
            if call.name not in {"lookup_company_profile", "lookup_fund_profile"}:
                continue
            parameters = {
                key: value
                for key, value in call.arguments.items()
                if key not in {"offset", "limit"}
            }
            symbol = parameters.get("ts_code")
            if (
                not isinstance(symbol, str)
                or len(symbol) != 9
                or not symbol[:6].isdigit()
                or symbol[6:] not in {".SH", ".SZ"}
                or symbol in excluded_targets
            ):
                continue
            tool_event = next(
                (
                    item
                    for item in authority.journal.events(acquisition.run_id)
                    if item.event_type == "pi.role.tool.completed"
                    and item.event_id.endswith(f".tool.{call.call_id}")
                ),
                None,
            )
            tool_hash = None
            tool_result: dict[str, object] = {}
            if tool_event is not None:
                result_binding = _object(tool_event.payload["binding"])
                if (
                    result_binding.get("name") != call.name
                    or result_binding.get("arguments") != call.arguments
                ):
                    raise PermissionError("candidate native result differs from its call")
                saved = _object(
                    authority.store.artifacts.read_json(str(tool_event.payload["artifact_hash"]))
                )
                tool_hash = str(saved["result_artifact_hash"])
                tool_result = _object(authority.store.artifacts.read_json(tool_hash))
            template = next(
                (item for item in acquisition.templates.values() if item.tool_name == call.name),
                None,
            )
            if template is None:
                raise PermissionError("candidate native profile source is unavailable")
            snapshot_id = (
                tool_result.get("snapshot_id") if tool_result.get("status") == "available" else None
            )
            request_id = None
            cutoff = acquisition.cutoff
            if snapshot_id is not None:
                if snapshot_id not in {item.snapshot_id for item in acquisition.snapshots}:
                    raise PermissionError("candidate cache result is outside frozen inputs")
            else:
                for result in results:
                    requested = acquisition.budget.journal.event(
                        f"{acquisition.budget.owner_run_id}.{result.request_id}.requested"
                    )
                    if requested is None or requested.payload.get("origin") != "agent_tool":
                        continue
                    if (
                        requested.payload.get("binding") != acquisition.binding
                        or requested.payload.get("template_id") != template.template_id
                        or requested.payload.get("parameters") != parameters
                    ):
                        continue
                    if result.snapshot_id is None or result.successor_cutoff is None:
                        continue
                    # The yielding adapter seals the Run before a pi tool result;
                    # the signed request plus parent completion is its durable result.
                    terminal = authority.replay(acquisition.run_id)
                    if terminal.get("reason") != "ResearchAcquisitionRequired":
                        continue
                    acquisition.successor_input((result,))
                    snapshot_id, cutoff, request_id = (
                        result.snapshot_id,
                        result.successor_cutoff,
                        result.request_id,
                    )
                    break
            if not isinstance(snapshot_id, str):
                continue
            snapshot = acquisition.store.get(snapshot_id)
            if (
                not snapshot.coverage_complete
                or snapshot.query.sources != (template.source,)
                or snapshot.query.parameters != parameters
                or snapshot.query.source_policy_id != template.template_id
                or snapshot.query.pit_lane
                != (
                    DataPITLane.PROSPECTIVE
                    if acquisition.historical_inputs
                    else acquisition.pit_lane
                )
                or (acquisition.historical_inputs is None and snapshot.query.as_of > cutoff)
            ):
                continue
            rows: dict[str, tuple[dict[str, object], str]] = {}
            projection_proof: dict[str, object] = {}
            if acquisition.historical_inputs is not None:
                market = acquisition.historical_inputs.with_snapshots((snapshot_id,))
                projection = market.research_projection(snapshot, template.api_name, cutoff)
                visible_hashes = (
                    {
                        str(_object(item)["raw_content_hash"])
                        for item in cast(list[object], tool_result.get("observations", []))
                    }
                    if request_id is None
                    else {item.raw_content_hash for item in snapshot.observations}
                )
                for item in cast(list[dict[str, object]], projection["rows"]):
                    row = _object(item["record"])
                    digest = str(item["source_record_hash"])
                    if row.get("ts_code") == symbol and digest in visible_hashes:
                        rows[canonical_hash(row)] = (row, digest)
                projection_proof = {
                    "projection_version": projection["projection_version"],
                    "modeled_cutoff": cutoff.isoformat(),
                    "historical_policy_id": market.policy.policy_id,
                    "strict_pit_accepted": False,
                }
            else:
                config = template.provider.public_source_config(template.source.upstream_source)
                for observation in snapshot.observations:
                    raw = _object(
                        acquisition.store.artifacts.read_json(observation.raw_content_hash)
                    )
                    if raw.get("fields") != config["fields"]:
                        raise PermissionError(
                            "candidate metadata raw fields differ from source contract"
                        )
                    row = dict(
                        zip(
                            cast(list[str], raw["fields"]),
                            cast(list[object], raw["values"]),
                            strict=True,
                        )
                    )
                    if row.get("ts_code") == symbol:
                        rows[canonical_hash(row)] = (row, observation.raw_content_hash)
            if len(rows) > 1:
                raise PermissionError("candidate metadata has conflicting identities")
            if not rows:
                continue
            identity_hash, (row, raw_hash) = next(iter(rows.items()))
            if row.get("exchange") != ("SSE" if symbol.endswith(".SH") else "SZSE") or (
                acquisition.historical_inputs is None
                and (
                    not isinstance(row.get("name") or row.get("csname"), str)
                    or not str(row.get("name") or row.get("csname")).strip()
                )
            ):
                continue
            if symbol in identity_hashes and identity_hashes[symbol] != identity_hash:
                raise PermissionError("candidate metadata has conflicting identities")
            identity_hashes[symbol] = identity_hash
            confirmed.setdefault(
                symbol,
                NativeCandidateProof(
                    symbol,
                    template.api_name,
                    snapshot_id,
                    {
                        "policy": policy,
                        "candidate": symbol,
                        "profile_api": template.api_name,
                        "predecessor_run_id": acquisition.run_id,
                        "predecessor_terminal_hash": record.terminal_artifact_id,
                        "predecessor_binding_hash": record.config_hash,
                        "native_response_event_id": event.event_id,
                        "native_response_hash": native_hash,
                        "native_call_id": call.call_id,
                        "native_tool_name": call.name,
                        "native_arguments": dict(call.arguments),
                        "native_result_event_id": tool_event.event_id
                        if tool_event
                        else f"{acquisition.budget.owner_run_id}.{request_id}.completed",
                        "native_result_hash": tool_hash,
                        "request_id": request_id,
                        "metadata_snapshot_id": snapshot_id,
                        "metadata_raw_record_hash": raw_hash,
                        "metadata_identity_hash": identity_hash,
                        **(
                            {**projection_proof, "modeled_identity": row}
                            if projection_proof
                            else {}
                        ),
                    },
                ),
            )
    if len(confirmed) > 5:
        raise ValueError(
            "candidate_limit_exceeded: narrow discovery to at most five distinct securities"
        )
    return tuple(confirmed.values())


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("candidate proof requires a frozen object")
    return cast(dict[str, object], value)
