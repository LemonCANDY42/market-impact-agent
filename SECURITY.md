# Security Policy

## Supported versions

The project is pre-alpha. Only the latest commit on `main` receives security
fixes until the first tagged release.

## Reporting a vulnerability

Do not open a public issue for vulnerabilities involving credentials, order
submission, account state, mandate bypass, duplicate execution, or provider
impersonation. Use the repository's
[private vulnerability reporting form](https://github.com/LemonCANDY42/market-impact-agent/security/advisories/new).

Include the affected revision, environment, minimal reproduction, impact, and
whether any real account or market was touched. Never include live credentials,
tokens, account identifiers, or paid data in the report.

## Secret handling

- No secret belongs in this repository, fixtures, logs, screenshots, issues, or
  CI configuration.
- Providers must obtain credentials from an external secret store or process
  environment at runtime.
- Examples use placeholders and synthetic accounts only.
- Any suspected exposure requires revocation before investigation continues.

## Live-trading posture

Live trading is disabled and unimplemented in the bootstrap. Future live
providers must fail closed, separate paper from live accounts, require a
versioned Trading Mandate, persist idempotent intent IDs before submission, and
reconcile external truth before accepting new orders.
