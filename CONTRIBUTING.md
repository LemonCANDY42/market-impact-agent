# Contributing

Contributions are welcome while the project remains within its accepted scope.

## Setup

```bash
uv sync --python 3.13
uv run market-impact status
```

## Required local checks

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

GitHub Actions may repeat these checks but is never the only acceptance path.

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
