# Source Register

This register records authoritative inputs used to define the bootstrap. It is not a
historical-news allowlist.

| Area | Source | Use |
| --- | --- | --- |
| GitHub Actions | [GitHub billing documentation](https://docs.github.com/en/billing/managing-billing-for-your-products/managing-billing-for-github-actions/about-billing-for-github-actions) | Public-repository hosted-runner cost boundary; local commands remain authoritative. |
| Python packaging | [Python packaging specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/) | `pyproject.toml` and Python-version declaration. |
| MCP | [Model Context Protocol specification](https://modelcontextprotocol.io/specification/) | Optional tool transport; not treated as execution truth. |
| VeighNa | [vn.py repository](https://github.com/vnpy/vnpy) | Version/platform and gateway dependency review. |
| NautilusTrader | [Execution](https://nautilustrader.io/docs/latest/concepts/execution/), [reconciliation](https://nautilustrader.io/docs/latest/concepts/reconciliation/), and [adapter guide](https://nautilustrader.io/docs/latest/developer_guide/adapters/) | Default/reference engine semantics and first Provider conformance target. |
| Hummingbot | [Gateway documentation](https://hummingbot.org/gateway/) | Reference for external connector services and reconciliation patterns. |
| LEAN | [LEAN documentation](https://www.quantconnect.com/docs/v2/lean-engine/getting-started) | Reference backtest/live engine boundary. |
| IBKR | [TWS API documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/) | Planned US/HK paper connectivity requirements. |
| Tushare | [Tushare documentation](https://tushare.pro/document/2) | Planned read-only A-share historical data provider. |

Event datasets and literature will be added with license, timestamp semantics, revision
behavior, and survivorship limitations before ingestion code is accepted.
