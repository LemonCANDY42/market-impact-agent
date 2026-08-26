import asyncio
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from mcp.server.mcpserver import MCPServer

from market_impact_agent.agent_runtime import ToolSideEffect
from market_impact_agent.mcp_runtime import (
    McpServerConfig,
    McpToolBridge,
    McpToolPolicy,
    McpTransport,
    mcp_server_config_from_dict,
)


def policy(tool_name: str, *, timeout_bytes: int = 4096) -> McpToolPolicy:
    return McpToolPolicy(
        tool_name=tool_name,
        side_effect=ToolSideEffect.READ_ONLY,
        required_capabilities=frozenset({"evidence.read"}),
        max_result_bytes=timeout_bytes,
    )


def in_process_config(*tool_names: str) -> McpServerConfig:
    return McpServerConfig(
        server_id="fixture-mcp",
        expected_name="fixture",
        expected_version="1.0.0",
        transport=McpTransport.IN_PROCESS,
        enabled=True,
        timeout_seconds=1,
        tools=tuple(policy(name) for name in tool_names),
    )


def test_in_process_mcp_negotiates_identity_discovers_and_calls_tools() -> None:
    server = MCPServer("fixture", version="1.0.0")

    @server.tool(name="read_evidence", structured_output=True)
    def _read_evidence(evidence_id: str) -> dict[str, str]:
        return {"evidence_id": evidence_id, "status": "available"}

    _ = _read_evidence

    bridge = McpToolBridge(
        in_process_config("read_evidence"),
        in_process_server=server,
    )

    snapshot = asyncio.run(bridge.discover())
    descriptors = asyncio.run(bridge.tool_descriptors(snapshot))
    result = asyncio.run(
        bridge.call(
            "read_evidence",
            {"evidence_id": "ev-1"},
            verified_snapshot=snapshot,
        )
    )

    assert snapshot.server_name == "fixture"
    assert snapshot.server_version == "1.0.0"
    assert snapshot.protocol_version
    assert snapshot.discovered_tools == ("read_evidence",)
    assert snapshot.tool_schema_hashes[0][0] == "read_evidence"
    assert len(snapshot.tool_schema_hashes[0][1]) == 64
    assert snapshot.binding_hash != snapshot.manifest_hash
    assert cast(Mapping[str, object], result["structuredContent"]) == {
        "evidence_id": "ev-1",
        "status": "available",
    }
    assert descriptors[0].name == "fixture-mcp.read_evidence"
    assert descriptors[0].side_effect is ToolSideEffect.READ_ONLY
    assert descriptors[0].mcp_binding_hash == snapshot.binding_hash


def test_mcp_identity_and_declared_tool_mismatch_fail_closed() -> None:
    wrong_identity = MCPServer("other", version="1.0.0")

    @wrong_identity.tool(name="read_evidence")
    def _read_evidence() -> str:
        return "ok"

    _ = _read_evidence

    bridge = McpToolBridge(
        in_process_config("read_evidence"),
        in_process_server=wrong_identity,
    )
    with pytest.raises(ValueError, match="identity"):
        asyncio.run(bridge.discover())

    server = MCPServer("fixture", version="1.0.0")

    @server.tool(name="available")
    def _available() -> str:
        return "ok"

    _ = _available

    missing = McpToolBridge(
        in_process_config("not-available"),
        in_process_server=server,
    )
    with pytest.raises(ValueError, match="missing declared tools"):
        asyncio.run(missing.discover())
    available_snapshot = asyncio.run(
        McpToolBridge(in_process_config("available"), in_process_server=server).discover()
    )
    with pytest.raises(PermissionError, match="not declared"):
        asyncio.run(
            missing.call(
                "available",
                {},
                verified_snapshot=available_snapshot,
            )
        )


def stdio_config(
    repo_root: Path,
    *tool_names: str,
    timeout_seconds: float = 1,
) -> McpServerConfig:
    return McpServerConfig(
        server_id="fixture-stdio",
        expected_name="market-impact-fixture",
        expected_version="1.0.0",
        transport=McpTransport.STDIO,
        enabled=True,
        timeout_seconds=timeout_seconds,
        tools=tuple(policy(name) for name in tool_names),
        command=sys.executable,
        args=("tests/fixtures/mcp_fixture_server.py",),
        cwd=repo_root,
    )


def test_stdio_mcp_process_success_timeout_crash_and_restart(repo_root: Path) -> None:
    echo_bridge = McpToolBridge(stdio_config(repo_root, "echo"))
    echo_snapshot = asyncio.run(echo_bridge.discover())
    first = asyncio.run(
        echo_bridge.call("echo", {"value": "first"}, verified_snapshot=echo_snapshot)
    )
    assert cast(Mapping[str, object], first["structuredContent"]) == {"value": "first"}

    timeout_bridge = McpToolBridge(stdio_config(repo_root, "hang", timeout_seconds=0.05))
    timeout_snapshot = asyncio.run(timeout_bridge.discover())
    with pytest.raises(TimeoutError, match="timed out"):
        asyncio.run(timeout_bridge.call("hang", {"seconds": 5}, verified_snapshot=timeout_snapshot))

    crash_bridge = McpToolBridge(stdio_config(repo_root, "crash", timeout_seconds=1))
    crash_snapshot = asyncio.run(crash_bridge.discover())
    with pytest.raises(RuntimeError, match="MCP transport failed"):
        asyncio.run(crash_bridge.call("crash", {}, verified_snapshot=crash_snapshot))

    restarted = asyncio.run(
        echo_bridge.call("echo", {"value": "after-crash"}, verified_snapshot=echo_snapshot)
    )
    assert cast(Mapping[str, object], restarted["structuredContent"]) == {"value": "after-crash"}

    async def cancel_hanging_call() -> None:
        cancellable = McpToolBridge(stdio_config(repo_root, "hang", timeout_seconds=5))
        snapshot = await cancellable.discover()
        task = asyncio.create_task(
            cancellable.call("hang", {"seconds": 5}, verified_snapshot=snapshot)
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_hanging_call())
    after_cancel = asyncio.run(
        echo_bridge.call("echo", {"value": "after-cancel"}, verified_snapshot=echo_snapshot)
    )
    assert cast(Mapping[str, object], after_cancel["structuredContent"]) == {
        "value": "after-cancel"
    }


def test_changed_schema_before_call_fails_without_invoking_handler() -> None:
    server = MCPServer("fixture", version="1.0.0")
    handler_calls: list[object] = []

    @server.tool(name="read_evidence", structured_output=True)
    def _read_evidence(evidence_id: str) -> dict[str, str]:
        handler_calls.append(evidence_id)
        return {"evidence_id": evidence_id}

    bridge = McpToolBridge(
        in_process_config("read_evidence"),
        in_process_server=server,
    )
    snapshot = asyncio.run(bridge.discover())
    descriptors = asyncio.run(bridge.tool_descriptors(snapshot))

    changed_server = MCPServer("fixture", version="1.0.0")

    @changed_server.tool(name="read_evidence", structured_output=True)
    def _changed_read_evidence(evidence_id: int) -> dict[str, int]:
        handler_calls.append(evidence_id)
        return {"evidence_id": evidence_id}

    server._tool_manager._tools["read_evidence"] = (  # pyright: ignore[reportPrivateUsage]
        changed_server._tool_manager._tools[  # pyright: ignore[reportPrivateUsage]
            "read_evidence"
        ]
    )
    _ = (_read_evidence, _changed_read_evidence)

    async def invoke_bound_handler() -> object:
        return await descriptors[0].handler({"evidence_id": "ev-1"})

    with pytest.raises(ValueError, match="snapshot changed"):
        asyncio.run(invoke_bound_handler())
    assert handler_calls == []


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_disabled_and_unsafe_http_mcp_configuration_is_rejected() -> None:
    disabled = McpServerConfig(
        server_id="disabled",
        expected_name="disabled",
        expected_version="1.0.0",
        transport=McpTransport.STREAMABLE_HTTP,
        enabled=False,
        timeout_seconds=1,
        tools=(policy("read"),),
        url="https://example.test/mcp",
    )
    with pytest.raises(PermissionError, match="disabled"):
        asyncio.run(McpToolBridge(disabled).discover())

    with pytest.raises(ValueError, match="HTTPS"):
        McpServerConfig(
            server_id="unsafe",
            expected_name="unsafe",
            expected_version="1.0.0",
            transport=McpTransport.STREAMABLE_HTTP,
            enabled=True,
            timeout_seconds=1,
            tools=(policy("read"),),
            url="http://example.test/mcp",
        )


def test_mcp_configuration_roundtrips_as_a_closed_versioned_value(repo_root: Path) -> None:
    config = stdio_config(repo_root, "echo")

    parsed = mcp_server_config_from_dict(config.to_dict())

    assert parsed == config
    assert parsed.manifest_hash == config.manifest_hash

    invalid = config.to_dict()
    invalid["undeclared"] = True
    with pytest.raises(ValueError, match="keys mismatch"):
        mcp_server_config_from_dict(invalid)
