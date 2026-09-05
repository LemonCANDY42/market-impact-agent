# Contributing

Contributions are welcome while the project remains within its accepted scope.

## Setup

```bash
uv sync --python 3.13
uv run market-impact status
```

The pi runtime additionally requires Node >=22.19 and its local, locked dependencies:

```bash
cd runtime/pi
npm ci --ignore-scripts
npm run check
npm test
```

Return to the repository root for the Python checks below. Node tests exercise real pinned pi
modules; Python pi integration tests replace network I/O only. Paid canaries are not part of pytest.

## Required local checks

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

GitHub Actions may repeat these checks but is never the only acceptance path.

## Test quality and efficiency

- Tie each test to a module requirement, owned boundary, or observed regression and
  the incorrect behavior it must distinguish. Judge the suite by useful fault
  detection, stability, and runtime, not test count or coverage percentage alone.
- Use the smallest faithful test scope: pure checks for pure rules, parameterized
  equivalence classes and boundary cases for variations, and representative
  integration tests through the real composition root and authoritative stores.
  Mock external I/O where needed, not the behavior under acceptance; do not copy
  production logic into a test-only implementation.
- Pair valid behavior with relevant failures. An always-empty tool, always-blocked
  gate, or ignored filter must not pass merely because the expected rejection was
  observed. For changed critical behavior, use a targeted fault injection or
  mutation when useful to verify that the assertions distinguish the regression.
- Give shared behavior one owning test matrix. Keep cross-layer cases only where
  they protect distinct wiring, lifecycle, or authority failures. Merge or delete
  overlapping tests with no independent protection; do not retain weak tests just
  because they already exist. Preserve necessary legacy replay and safety cases,
  but avoid repeating the full lifecycle for every configuration combination.
- Prefer deterministic clocks, explicit concurrency barriers, and isolated state
  to real sleeps or incidental ordering. Profile slow tests before changing them;
  do not remove a necessary recovery or risk check merely to shorten the suite.
- Real continuous-study integration paths require ignored licensed panel manifests
  and the prior-usage audit under `.market-impact/`. They skip only when those
  files are absent on a portable clone; synthetic tests continue to run. Local
  acceptance with the private artifacts present must run those real paths and is
  distinct from a portable pytest result.
- During iteration run affected tests; at workslice completion run the required
  local checks above. Keep paid-model and broker acceptance separately authorized
  and budgeted. Offline passes do not establish real tool usability, investment
  effectiveness, or broker readiness.

For a bounded runtime/financial guard audit, use
`uv run python tests/reliability_ablation.py OUTPUT_DIRECTORY`. This optional,
zero-model-cost mutation run reuses existing tests in disposable source copies;
it is not another full suite or a production configuration. Inspect the exact
assertion failures, not only the mutation detection count. See the
[reliability scope and limitations](docs/MODEL_PROVIDER_RELIABILITY.md#bounded-reliability-ablation).

## Design rules

- Use the language in [CONTEXT.md](CONTEXT.md).
- Keep evidence time, decision time, executable time, and observed execution
  time separate.
- Providers may advertise only independently verified capabilities to agents.
- An Agent cannot override hard policy or mutate a Trading Mandate.
- Research fixtures must be synthetic or legally redistributable.
- A report or mocked test is not evidence of broker, paper, or live readiness.

Open an issue before adding a new execution engine, live provider, hosted
service, UI, database server, or event family. Describe the acceptance evidence
and the simpler alternative considered.
