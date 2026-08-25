# Project Guidance

## Outcome

Build the smallest auditable harness that converts point-in-time event evidence
into policy-gated intents consumable by replaceable trading engines.

## Boundaries

- One orchestration owner: the harness. Providers execute capabilities; they do
  not own research, approval policy, or canonical state.
- An Agent may propose a signal or order intent. It may not bypass hard policy,
  edit a Trading Mandate, access raw broker credentials, or treat transport
  success as an accepted or filled order.
- Live execution stays fail-closed. Never add a working live path without a
  versioned mandate, idempotent order identity, reconciliation, a kill switch,
  and explicit acceptance evidence.
- Do not commit secrets, account identifiers, paid news, or licensed market data.
- Do not add a UI, service, database server, multi-agent debate framework, or
  second execution engine before its roadmap gate is satisfied.

## Domain and documentation

- Use the canonical terms in `CONTEXT.md`.
- Update the nearest owning document when a contract, boundary, claim gate, or
  user-visible behavior changes.
- Record an ADR only for a hard-to-reverse, surprising trade-off.

## Verification

Run locally before considering a change complete:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

GitHub Actions is an optional mirror, not acceptance authority.
