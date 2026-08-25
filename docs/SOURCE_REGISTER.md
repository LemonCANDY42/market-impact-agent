# Source Register

This register records authoritative inputs used to define the bootstrap. It is not a
historical-news allowlist.

| Area | Source | Use |
| --- | --- | --- |
| GitHub Actions | [GitHub billing documentation](https://docs.github.com/en/billing/managing-billing-for-your-products/managing-billing-for-github-actions/about-billing-for-github-actions) | Public-repository hosted-runner cost boundary; local commands remain authoritative. |
| Python packaging | [Python packaging specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/) | `pyproject.toml` and Python-version declaration. |
| MCP | [Model Context Protocol specification](https://modelcontextprotocol.io/specification/) | Optional tool transport; not treated as execution truth. |
| VeighNa | [vn.py repository](https://github.com/vnpy/vnpy) | Version/platform and gateway dependency review. |
| NautilusTrader foundation | [Architecture](https://nautilustrader.io/docs/latest/concepts/architecture/), [backtesting](https://nautilustrader.io/docs/latest/concepts/backtesting/), [execution](https://nautilustrader.io/docs/latest/concepts/execution/), and [reconciliation](https://nautilustrader.io/docs/latest/concepts/reconciliation/) | Default engine and backtest semantics; shared core does not imply identical fill, venue, or recovery behavior. |
| NautilusTrader integration | [IB integration](https://nautilustrader.io/docs/latest/integrations/interactive_brokers/), [Python metadata](https://github.com/nautechsystems/nautilus_trader/blob/master/python/pyproject.toml), and [workspace license](https://github.com/nautechsystems/nautilus_trader/blob/master/Cargo.toml) | Later IBKR candidate, Python compatibility, and LGPL-3.0-only boundary; no Provider conformance is inherited. |
| Hummingbot | [Gateway documentation](https://hummingbot.org/gateway/) | Reference for external connector services and reconciliation patterns. |
| LEAN | [LEAN documentation](https://www.quantconnect.com/docs/v2/lean-engine/getting-started) | Reference backtest/live engine boundary. |
| IBKR | [TWS API documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/) | Planned US/HK paper connectivity requirements. |
| Tushare | [Tushare documentation](https://tushare.pro/document/2) | Planned read-only A-share historical data provider. |
| Corporate and macro events | [SEC Form 8-K](https://www.sec.gov/files/form8-k.pdf) and [Bernanke–Kuttner](https://www.nber.org/papers/w10402) | Auditable issuer triggers and separation of event origin, expectation delta, and transmission. |
| Geopolitics and policy | [UCDP GED codebook](https://ucdp.uu.se/downloads/ged/ged251.pdf) and [OFAC sanctions programs](https://ofac.treasury.gov/sanctions-programs-and-country-information) | Separate conflict/security facts from formal sanctions, trade controls, and policy actions. |
| Physical, climate, and supply-chain shocks | [UNDRR Hazard Information Profiles](https://www.undrr.org/publication/documents-and-publications/hazard-information-profiles-hips-2025-version), [EIA oil transit chokepoints](https://www.eia.gov/international/analysis/special-topics/World_Oil_Transit_Chokepoints), and [NY Fed GSCPI](https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr1017.pdf) | External hazard dictionary and observable physical/logistics transmission variables. |
| Technology and narratives | [OECD Oslo Manual](https://www.oecd.org/en/publications/oslo-manual-2018_9789264304604-en.html) and [Shiller, Narrative Economics](https://www.aeaweb.org/articles?id=10.1257/aer.107.4.967) | Require observable implementation/adoption and treat cumulative narratives as difficult-to-identify composed assessments. |
| Market mechanics | [Brunnermeier–Pedersen](https://pages.stern.nyu.edu/~lpederse/papers/Mkt_Fun_Liquidity.pdf) | Funding and market liquidity can form a distinct amplifying transmission channel. |
| Real-event fixture | [IEA September 2019 Oil Market Report](https://www.iea.org/reports/oil-market-report-september-2019), [Aramco incident notice](https://www.aramco.com/en/news-media/news/2019/incidents-at-abqaiq-and-khurais), and [Aramco recovery update](https://www.aramco.com/en/news-media/news/2019/saudi-aramco-swiftly-restores-production-capacity) | Point-in-time baseline, attack magnitude, recovery, and shipment evidence for the Abqaiq geopolitical supply-shock fixture. Date-only publication metadata is conservatively visible at end of day UTC. |

Event datasets and literature will be added with license, timestamp semantics, revision
behavior, and survivorship limitations before ingestion code is accepted.
