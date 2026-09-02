# Project Guidance

## Outcome

Build the smallest auditable path to Agent-directed automated paper and live
trading. The current harness converts point-in-time event evidence into
policy-gated intents consumable by replaceable trading engines while later
execution phases remain fail-closed until their acceptance gates pass.

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
- Enforce only invariants that protect an owned boundary such as authority, PIT,
  budget, idempotency, risk, or replay. Do not require Agents or Providers to
  echo IDs, ordering, defaults, or optional fields the Harness can derive or
  inject. Prefer typed absence or degradation over failure when safety and
  evidence remain intact, and avoid wrapper contracts, duplicate state, or
  formal symmetry without a concrete acceptance need.
- Update the nearest owning document when a contract, boundary, claim gate, or
  user-visible behavior changes.
- Record an ADR only for a hard-to-reverse, surprising trade-off.

## Verification

Follow the requirement-driven test selection and pruning rules in
`CONTRIBUTING.md#test-quality-and-efficiency`; test count is not an acceptance target.

Run locally before considering a change complete:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

GitHub Actions is an optional mirror, not acceptance authority.
