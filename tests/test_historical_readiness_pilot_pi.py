"""Pilot worker ownership through real Python/Node/pi; only network I/O is synthetic."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.historical_readiness_pilot import (
    HistoricalReadinessInputs,
    run_historical_readiness_pilot,
)
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.usage_ledger import UsageLedger

from .test_historical_readiness_pilot import (
    NOW,
    _prepare,  # pyright: ignore[reportPrivateUsage]
    _prepare_v2,  # pyright: ignore[reportPrivateUsage]
)
from .test_historical_readiness_pilot import inputs as inputs


@pytest.mark.parametrize("adjudicated", [False, True])
def test_real_pi_arm_admission_is_concurrent_and_parent_usage_reconciles(
    tmp_path: Path,
    inputs: HistoricalReadinessInputs,
    monkeypatch: pytest.MonkeyPatch,
    adjudicated: bool,
) -> None:
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    # Synthetic key only, never read a real credential or an installed route permit.
    monkeypatch.setenv(profile.credential_env, "synthetic-offline-pilot")
    permit = PiRuntimePermit(
        canonical_hash(runtime_identity()), (profile.route_identity,), "synthetic-pilot-proof"
    )

    def offline_permit(_root: Path) -> PiRuntimePermit:
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", offline_permit)
    profile_path = tmp_path / "pi-profile.json"
    profile_path.write_text(json.dumps(profile.to_dict()))
    inputs = replace(inputs, provider_profile_path=profile_path)
    prepared = (
        _prepare_v2(inputs, tmp_path, judge_profile=profile_path)
        if adjudicated
        else _prepare(inputs, tmp_path)
    )
    pattern_id = json.loads(inputs.pattern_pack_paths[0].read_text())["pack_id"]

    async def scenario() -> None:
        parent = RunJournal(tmp_path / "parent.sqlite3")
        parent.start_run(run_id="parent", config_hash=canonical_hash("fixed"), created_at=NOW)
        budget = ModelBudget(parent, "parent", 8, 2_000_000)
        control = PiRuntimeProvider(profile, budget=budget, permit=permit)
        treatment = PiRuntimeProvider(profile, budget=budget, permit=permit)
        judge = PiRuntimeProvider(profile, budget=budget, permit=permit)
        arrived, release = asyncio.Event(), asyncio.Event()
        physical: list[tuple[int, dict[str, Any]]] = []
        contexts: list[tuple[int, str]] = []
        errors: list[Exception] = []
        send = PiRuntimeProvider._send  # pyright: ignore[reportPrivateUsage]

        async def observe_send(
            self: PiRuntimeProvider, process: asyncio.subprocess.Process, frame: dict[str, object]
        ) -> None:
            if frame.get("type") == "run":
                payload = cast(dict[str, object], frame["payload"])
                contexts.append((process.pid, cast(str, payload["conversationId"])))
            await send(process, frame)

        monkeypatch.setattr(PiRuntimeProvider, "_send", observe_send)

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                header = (await reader.readuntil(b"\r\n\r\n")).decode()
                headers = dict(
                    line.lower().split(": ", 1) for line in header.split("\r\n")[1:] if line
                )
                body = json.loads(await reader.readexactly(int(headers["content-length"])))
                physical.append((int(headers["x-fixture-worker"]), body))
                number = len(physical)
                if number == 2:
                    arrived.set()
                if number <= 2:
                    await release.wait()
                has_results = any(
                    item.get("type") == "function_call_output" for item in body["input"]
                )
                text = json.dumps(
                    {
                        "event_id": "synthetic-event",
                        "decision": "abstain",
                        "summary": "Synthetic evidence was read; transmission remains unknown.",
                        "transmission_steps": [],
                        "candidates": [],
                        "blockers": ["Unknown transmission"],
                        "unresolved_questions": [],
                        "stopped_reason": "Synthetic test complete",
                    }
                )
                items: list[dict[str, object]] = (
                    [
                        {
                            "type": "message",
                            "id": "msg-1",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": text, "annotations": []}],
                        }
                    ]
                    if has_results
                    else [
                        {
                            "type": "function_call",
                            "id": f"fc-{index}",
                            "call_id": f"call-{index}",
                            "name": name,
                            "arguments": json.dumps(arguments),
                        }
                        for index, (name, arguments) in enumerate(
                            [
                                ("read_evidence", {"evidence_id": "news"}),
                                ("read_evidence", {"evidence_id": "context"}),
                                ("read_pattern_pack", {"pack_id": pattern_id}),
                            ]
                        )
                    ]
                )
                frames: list[dict[str, object]] = [
                    {"type": "response.created", "response": {"id": f"response-{number}"}}
                ]
                for index, item in enumerate(items):
                    frames.append(
                        {"type": "response.output_item.added", "output_index": index, "item": item}
                    )
                    if has_results:
                        frames.append(
                            {
                                "type": "response.output_text.delta",
                                "output_index": index,
                                "delta": text,
                            }
                        )
                    frames.append(
                        {"type": "response.output_item.done", "output_index": index, "item": item}
                    )
                frames.append(
                    {
                        "type": "response.completed",
                        "response": {
                            "id": f"response-{number}",
                            "model": body["model"],
                            "status": "completed",
                            "output": items,
                            "usage": {
                                "input_tokens": 100,
                                "output_tokens": 20,
                                "total_tokens": 120,
                            },
                        },
                    }
                )
                data = "".join(
                    f"event: {frame['type']}\ndata: {json.dumps(frame)}\n\n" for frame in frames
                ).encode()
                writer.write(
                    (
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                        f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n"
                    ).encode()
                    + data
                )
                await writer.drain()
            except Exception as exc:
                errors.append(exc)
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        preload = tmp_path / "pilot-network.mjs"
        preload.write_text(
            "const original = globalThis.fetch;\n"
            "globalThis.fetch = async (input, init) => {\n"
            " const request = new Request(input, init);\n"
            " const headers = new Headers(request.headers);\n"
            " headers.set('x-fixture-worker', String(process.pid));\n"
            f" return original('http://127.0.0.1:{port}', {{method: 'POST', headers,\n"
            " body: await request.text(), signal: request.signal});\n"
            "};\n"
        )
        spawn = asyncio.create_subprocess_exec

        async def offline_spawn(program: str, *args: str, **kwargs: Any):
            return await spawn(program, "--import", str(preload), *args, **kwargs)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", offline_spawn)
        task = asyncio.create_task(
            run_historical_readiness_pilot(
                prepared,
                control_provider=control,
                treatment_provider=treatment,
                judge_provider=judge if adjudicated else None,
            )
        )
        try:
            try:
                await asyncio.wait_for(arrived.wait(), 15)
                assert len(physical) == 2 and len({pid for pid, _ in physical}) == 2
                assert len(contexts) == 2 and len({context for _, context in contexts}) == 2
                assert budget.summary()["physical_requests"] == 2
                assert budget.summary()["unsettled_requests"] == 2
                assert budget.summary()["known_cost_microusd"] == 0
                assert control.budget is treatment.budget is budget
                first_pids = {pid for pid, _ in physical}
            finally:
                release.set()
            report = await asyncio.wait_for(task, 30)
            assert not errors
            assert report["diagnostic_valid"] and report["protocol_complete"]
            assert report["accounting_complete"] and report["provider_request_count"] == 8
            assert len(physical) == 8 and {pid for pid, _ in physical} == first_pids
            assert len(contexts) == 4 and len({context for _, context in contexts}) == 4
            assert all(sum(worker == pid for worker, _ in contexts) == 2 for pid in first_pids)
            for arm, worker in (("control", control), ("treatment", treatment)):
                process = worker._process  # pyright: ignore[reportPrivateUsage]
                assert process is not None
                expected_contexts = {
                    worker.context_identity(f"{prepared.experiment_id}.{arm}.pair-{pair}", [], [])[
                        "conversationId"
                    ]
                    for pair in (1, 2)
                }
                assert {
                    context for pid, context in contexts if pid == process.pid
                } == expected_contexts
                # Reusing the process must not carry the preceding pair's conversation.
                assert [
                    any(item.get("type") == "function_call_output" for item in body["input"])
                    for pid, body in physical
                    if pid == process.pid
                ] == [False, True, False, True]
            records = UsageLedger(prepared.directory / "usage.sqlite3").records()
            assert len(records) == 4
            assert all(record.record.terminal_artifact_hash for record in records)
            assert sum(record.record.metrics.input_tokens for record in records) == 800
            assert sum(record.record.metrics.output_tokens for record in records) == 160
            assert budget.summary() == {
                "physical_requests": 8,
                "known_cost_microusd": report["ledger_actual_microusd"],
                "reserved_microusd": 0,
                "unsettled_requests": 0,
            }
            # Injection ownership stays with the caller; workers survive sequential pairs.
            assert control._process is not None and treatment._process is not None  # pyright: ignore[reportPrivateUsage]
            with pytest.raises(ValueError, match="already dispatched"):
                await run_historical_readiness_pilot(
                    prepared,
                    control_provider=control,
                    treatment_provider=treatment,
                    judge_provider=judge if adjudicated else None,
                )
            assert len(physical) == 8
        finally:
            release.set()
            if not task.done():
                await asyncio.wait_for(task, 30)
            await asyncio.gather(control.close(), treatment.close(), judge.close())
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())
