from __future__ import annotations

import asyncio
import math
import os
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, NoReturn, cast
from urllib.parse import urlparse

from mcp import Client, StdioServerParameters
from mcp.server.mcpserver import MCPServer

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ToolDescriptor, ToolHandler, ToolSideEffect


class McpTransport(StrEnum):
    IN_PROCESS = "in_process"
    STDIO = "stdio"
    STREAMABLE_HTTP = "streamable_http"


@dataclass(frozen=True, slots=True)
class McpToolPolicy:
    tool_name: str
    side_effect: ToolSideEffect
    required_capabilities: frozenset[str]
    max_result_bytes: int

    def __post_init__(self) -> None:
        _identifier(self.tool_name, "MCP tool_name")
        if self.max_result_bytes < 1:
            raise ValueError("MCP tool max_result_bytes must be positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "tool_name": self.tool_name,
            "side_effect": self.side_effect.value,
            "required_capabilities": sorted(self.required_capabilities),
            "max_result_bytes": self.max_result_bytes,
        }


@dataclass(frozen=True, slots=True)
class McpServerConfig:
    server_id: str
    expected_name: str
    expected_version: str
    transport: McpTransport
    enabled: bool
    timeout_seconds: float
    tools: tuple[McpToolPolicy, ...]
    command: str | None = None
    args: tuple[str, ...] = ()
    cwd: Path | None = None
    environment: tuple[tuple[str, str], ...] = ()
    url: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.server_id, "MCP server_id")
        _trimmed(self.expected_name, "MCP expected_name")
        _trimmed(self.expected_version, "MCP expected_version")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("MCP timeout_seconds must be finite and positive")
        if not self.tools:
            raise ValueError("MCP servers require at least one explicit tool policy")
        if len({item.tool_name for item in self.tools}) != len(self.tools):
            raise ValueError("MCP tool policies must have unique names")
        child_names = [child for child, _parent in self.environment]
        if len(child_names) != len(set(child_names)):
            raise ValueError("MCP environment child names must be unique")
        if any(not child or not parent for child, parent in self.environment):
            raise ValueError("MCP environment names must not be empty")
        if self.transport is McpTransport.STDIO:
            if not self.command or self.url is not None:
                raise ValueError("stdio MCP servers require command and forbid url")
        elif self.transport is McpTransport.STREAMABLE_HTTP:
            if self.command is not None or not self.url:
                raise ValueError("HTTP MCP servers require url and forbid command")
            parsed = urlparse(self.url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError("HTTP MCP server url must be an absolute HTTPS URL")
            if self.environment:
                raise ValueError("HTTP MCP configuration cannot inject subprocess environment")
        elif any((self.command, self.url, self.args, self.cwd, self.environment)):
            raise ValueError("in-process MCP configuration cannot define transport settings")

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "server_id": self.server_id,
            "expected_name": self.expected_name,
            "expected_version": self.expected_version,
            "transport": self.transport.value,
            "enabled": self.enabled,
            "timeout_seconds": float(self.timeout_seconds),
            "tools": [item.to_dict() for item in self.tools],
            "command": self.command,
            "args": list(self.args),
            "cwd": None if self.cwd is None else str(self.cwd),
            "environment": [
                {"child_name": child, "parent_name": parent} for child, parent in self.environment
            ],
            "url": self.url,
        }


@dataclass(frozen=True, slots=True)
class McpServerSnapshot:
    server_id: str
    server_name: str
    server_version: str
    protocol_version: str
    discovered_tools: tuple[str, ...]
    tool_schema_hashes: tuple[tuple[str, str], ...]
    manifest_hash: str

    def __post_init__(self) -> None:
        _identifier(self.server_id, "MCP snapshot server_id")
        for name, value in (
            ("server_name", self.server_name),
            ("server_version", self.server_version),
            ("protocol_version", self.protocol_version),
        ):
            _trimmed(value, f"MCP snapshot {name}")
        _sha256(self.manifest_hash, "MCP snapshot manifest_hash")
        names = tuple(name for name, _schema_hash in self.tool_schema_hashes)
        if names != self.discovered_tools:
            raise ValueError("MCP snapshot tool hashes must exactly match discovered tools")
        for name, schema_hash in self.tool_schema_hashes:
            _identifier(name, "MCP snapshot tool name")
            _sha256(schema_hash, "MCP snapshot tool schema_hash")

    @property
    def binding_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "server_id": self.server_id,
            "server_name": self.server_name,
            "server_version": self.server_version,
            "protocol_version": self.protocol_version,
            "discovered_tools": list(self.discovered_tools),
            "tool_schema_hashes": [
                {"tool_name": name, "schema_hash": schema_hash}
                for name, schema_hash in self.tool_schema_hashes
            ],
            "manifest_hash": self.manifest_hash,
        }


class McpToolBridge:
    def __init__(
        self,
        config: McpServerConfig,
        *,
        in_process_server: MCPServer[Any] | None = None,
    ) -> None:
        if config.transport is McpTransport.IN_PROCESS and in_process_server is None:
            raise ValueError("in-process MCP bridge requires a server instance")
        if config.transport is not McpTransport.IN_PROCESS and in_process_server is not None:
            raise ValueError("server instances are valid only for in-process MCP bridges")
        self.config = config
        self._in_process_server = in_process_server

    async def discover(self) -> McpServerSnapshot:
        try:
            if not self.config.enabled:
                raise PermissionError(f"MCP server is disabled: {self.config.server_id}")
            async with self._client() as client:
                snapshot, _discovered = await self._snapshot(client)
                return snapshot
        except BaseExceptionGroup as exc:
            _raise_normalized_mcp_error(exc, self.config.server_id)

    async def tool_descriptors(
        self, verified_snapshot: McpServerSnapshot
    ) -> tuple[ToolDescriptor, ...]:
        try:
            async with self._client() as client:
                current_snapshot, discovered = await self._snapshot(client)
                self._assert_verified_snapshot(verified_snapshot, current_snapshot)
        except BaseExceptionGroup as exc:
            _raise_normalized_mcp_error(exc, self.config.server_id)
        descriptors: list[ToolDescriptor] = []
        for policy in self.config.tools:
            tool = discovered[policy.tool_name]
            input_schema = cast(dict[str, object], tool.input_schema)
            description = tool.description or f"MCP tool {policy.tool_name}"
            descriptors.append(
                ToolDescriptor(
                    name=f"{self.config.server_id}.{policy.tool_name}",
                    version=(
                        f"{self.config.expected_version}:{verified_snapshot.binding_hash[:12]}"
                    ),
                    description=description,
                    input_schema=input_schema,
                    required_capabilities=policy.required_capabilities,
                    side_effect=policy.side_effect,
                    timeout_seconds=self.config.timeout_seconds,
                    max_result_bytes=policy.max_result_bytes,
                    handler=self._handler(policy.tool_name, verified_snapshot),
                    mcp_server_id=self.config.server_id,
                    mcp_binding_hash=verified_snapshot.binding_hash,
                )
            )
        return tuple(descriptors)

    async def call(
        self,
        tool_name: str,
        arguments: dict[str, object],
        *,
        verified_snapshot: McpServerSnapshot,
    ) -> dict[str, object]:
        policy = next((item for item in self.config.tools if item.tool_name == tool_name), None)
        if policy is None:
            raise PermissionError(f"MCP tool is not declared by policy: {tool_name}")
        try:
            async with self._client() as client:
                current_snapshot, _discovered = await self._snapshot(client)
                self._assert_verified_snapshot(verified_snapshot, current_snapshot)
                try:
                    result = await asyncio.wait_for(
                        client.call_tool(
                            tool_name,
                            cast(dict[str, Any], arguments),
                            read_timeout_seconds=self.config.timeout_seconds,
                        ),
                        timeout=self.config.timeout_seconds,
                    )
                except TimeoutError as exc:
                    raise TimeoutError(f"MCP tool timed out: {tool_name}") from exc
                payload = cast(
                    dict[str, object],
                    result.model_dump(mode="json", by_alias=True, exclude_none=True),
                )
        except BaseExceptionGroup as exc:
            _raise_normalized_mcp_error(exc, self.config.server_id)
        if result.is_error:
            raise RuntimeError(f"MCP tool returned an error: {tool_name}")
        return payload

    def _handler(self, tool_name: str, verified_snapshot: McpServerSnapshot) -> ToolHandler:
        async def call(arguments: dict[str, object]) -> object:
            return await self.call(
                tool_name,
                arguments,
                verified_snapshot=verified_snapshot,
            )

        return call

    async def _snapshot(self, client: Client) -> tuple[McpServerSnapshot, dict[str, Any]]:
        self._assert_identity(client)
        response = await asyncio.wait_for(
            client.list_tools(cache_mode="refresh"),
            timeout=self.config.timeout_seconds,
        )
        discovered = {tool.name: tool for tool in response.tools}
        names = tuple(sorted(discovered))
        declared = {item.tool_name for item in self.config.tools}
        missing = sorted(declared - set(names))
        if missing:
            raise ValueError(f"MCP server is missing declared tools: {', '.join(missing)}")
        info = client.server_info
        if info is None:
            raise ValueError("MCP server did not report an identity")
        snapshot = McpServerSnapshot(
            server_id=self.config.server_id,
            server_name=info.name,
            server_version=info.version,
            protocol_version=client.protocol_version,
            discovered_tools=names,
            tool_schema_hashes=tuple(
                sorted((tool.name, _discovered_tool_hash(tool)) for tool in response.tools)
            ),
            manifest_hash=self.config.manifest_hash,
        )
        return snapshot, discovered

    @staticmethod
    def _assert_verified_snapshot(verified: McpServerSnapshot, current: McpServerSnapshot) -> None:
        if current != verified:
            raise ValueError("MCP server snapshot changed before tool execution")

    def _client(self) -> Client:
        if self.config.transport is McpTransport.IN_PROCESS:
            server = self._in_process_server
            if server is None:
                raise AssertionError("validated in-process server is missing")
            return Client(server, read_timeout_seconds=self.config.timeout_seconds)
        if self.config.transport is McpTransport.STDIO:
            command = self.config.command
            if command is None:
                raise AssertionError("validated stdio command is missing")
            child_environment: dict[str, str] = {}
            for child_name, parent_name in self.config.environment:
                value = os.environ.get(parent_name)
                if value is None:
                    raise RuntimeError(
                        f"required MCP environment variable is unavailable: {parent_name}"
                    )
                child_environment[child_name] = value
            parameters = StdioServerParameters(
                command=command,
                args=list(self.config.args),
                env=child_environment,
                cwd=None if self.config.cwd is None else str(self.config.cwd),
            )
            return Client(parameters, read_timeout_seconds=self.config.timeout_seconds)
        url = self.config.url
        if url is None:
            raise AssertionError("validated MCP url is missing")
        return Client(url, read_timeout_seconds=self.config.timeout_seconds)

    def _assert_identity(self, client: Client) -> None:
        info = client.server_info
        if info is None:
            raise ValueError("MCP server did not report an identity")
        if info.name != self.config.expected_name or info.version != self.config.expected_version:
            raise ValueError("MCP server identity does not match its pinned configuration")


def mcp_server_config_from_dict(value: object) -> McpServerConfig:
    payload = _object(value, "MCP server configuration")
    expected_keys = {
        "args",
        "command",
        "cwd",
        "enabled",
        "environment",
        "expected_name",
        "expected_version",
        "server_id",
        "timeout_seconds",
        "tools",
        "transport",
        "url",
    }
    if set(payload) != expected_keys:
        missing = sorted(expected_keys - set(payload))
        extra = sorted(set(payload) - expected_keys)
        raise ValueError(
            f"MCP server configuration keys mismatch: missing={missing}, extra={extra}"
        )
    environment = tuple(
        (
            _string(item, "child_name"),
            _string(item, "parent_name"),
        )
        for item in _object_list(payload.get("environment"), "environment")
    )
    tools = tuple(
        McpToolPolicy(
            tool_name=_string(item, "tool_name"),
            side_effect=ToolSideEffect(_string(item, "side_effect")),
            required_capabilities=frozenset(
                _string_list(item.get("required_capabilities"), "required_capabilities")
            ),
            max_result_bytes=_integer(item, "max_result_bytes"),
        )
        for item in _object_list(payload.get("tools"), "tools")
    )
    return McpServerConfig(
        server_id=_string(payload, "server_id"),
        expected_name=_string(payload, "expected_name"),
        expected_version=_string(payload, "expected_version"),
        transport=McpTransport(_string(payload, "transport")),
        enabled=_boolean(payload, "enabled"),
        timeout_seconds=_number(payload, "timeout_seconds"),
        tools=tools,
        command=_optional_string(payload, "command"),
        args=tuple(_string_list(payload.get("args"), "args")),
        cwd=_optional_path(payload, "cwd"),
        environment=environment,
        url=_optional_string(payload, "url"),
    )


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", value) is None:
        raise ValueError(f"{name} contains unsupported characters")


def _discovered_tool_hash(tool: object) -> str:
    dump = getattr(tool, "model_dump", None)
    if not callable(dump):
        raise TypeError("MCP discovered tool does not expose a serializable schema")
    payload = dump(mode="json", by_alias=True, exclude_none=True)
    return canonical_hash(payload)


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], value)


def _object_list(value: object, name: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, f"{name} item") for item in cast(list[object], value))


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    _trimmed(value, name)
    return value


def _optional_string(payload: dict[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string or null")
    _trimmed(value, name)
    return value


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) or not item for item in raw):
        raise TypeError(f"{name} must contain non-empty strings")
    values = cast(list[str], raw)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} values must be unique")
    return values


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _number(payload: dict[str, object], name: str) -> float:
    value = payload.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number")
    return float(value)


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _optional_path(payload: dict[str, object], name: str) -> Path | None:
    value = _optional_string(payload, name)
    return None if value is None else Path(value)


def _raise_normalized_mcp_error(
    error: BaseExceptionGroup[BaseException], server_id: str
) -> NoReturn:
    leaves = _group_leaves(error)
    for error_type in (TimeoutError, PermissionError, ValueError):
        matching = next((item for item in leaves if isinstance(item, error_type)), None)
        if matching is not None:
            raise matching from error
    cancellation = next(
        (item for item in leaves if isinstance(item, asyncio.CancelledError)),
        None,
    )
    non_cancellation = next(
        (item for item in leaves if isinstance(item, Exception)),
        None,
    )
    if non_cancellation is None and cancellation is not None:
        raise cancellation from error
    raise RuntimeError(f"MCP transport failed: {server_id}") from error


def _group_leaves(error: BaseExceptionGroup[BaseException]) -> tuple[BaseException, ...]:
    leaves: list[BaseException] = []
    for item in error.exceptions:
        if isinstance(item, BaseExceptionGroup):
            leaves.extend(_group_leaves(cast(BaseExceptionGroup[BaseException], item)))
        else:
            leaves.append(item)
    return tuple(leaves)
