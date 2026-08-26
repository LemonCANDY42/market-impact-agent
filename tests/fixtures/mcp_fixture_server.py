from __future__ import annotations

import asyncio
import os

from mcp.server.mcpserver import MCPServer

server = MCPServer("market-impact-fixture", version="1.0.0")


@server.tool(structured_output=True)
def echo(value: str) -> dict[str, str]:
    return {"value": value}


@server.tool(structured_output=True)
async def hang(seconds: float) -> dict[str, float]:
    await asyncio.sleep(seconds)
    return {"seconds": seconds}


@server.tool()
def crash() -> None:
    os._exit(17)


if __name__ == "__main__":
    server.run("stdio")
