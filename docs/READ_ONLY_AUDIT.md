# Read-only audit and replay boundaries

This document owns the distinction between source integrity, a consistent audit
snapshot, and physical filesystem immutability. It applies to offline Harness
audits; it does not replace the writable runtime or grant model/trading authority.

## Existing library capabilities

Use Python's standard `sqlite3` interface to the existing SQLite engine. This
change was tested with Python 3.13.15 / SQLite 3.53.4; no dependency, database
format, service or storage migration is required.

SQLite's [WAL documentation](https://www.sqlite.org/wal.html) describes shared
coordination files used by readers. A read-only database connection can participate
in WAL bookkeeping. A checkpoint can change the main database bytes without
changing its logical records. Therefore neither the whole live source directory
nor the main `.sqlite3` file alone is an appropriate unchanged-state assertion.
Content hashes still protect immutable CAS objects and explicitly frozen inputs;
byte comparisons remain appropriate for an exclusively owned, completed journal.

SQLite's [read transaction](https://www.sqlite.org/isolation.html) pins a database
view at its first read. The offline authority reader uses one short `BEGIN` read
transaction for signed events, Usage records and terminal Run records, then closes
the connection on both success and failure. Concurrent WAL writers can proceed.
The read-only compatibility transaction on `ReadOnlyDataSnapshotStore` follows the
same lifetime; opening a connection alone does not pin a multi-query snapshot.

For legacy engine replay, use the standard [Online Backup API](https://www.sqlite.org/backup.html)
via [`Connection.backup`](https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Connection.backup).
It includes committed WAL state in a consistent private database copy. Copying only
a live main database file is not equivalent. The audit owns its temporary copy;
legacy schema initialization, claims and replay can operate there without modifying
the source. Existing content and execution validators still apply to the copy.

These are application ownership fixes using maintained library capabilities. A new
ORM, database driver or database server would not remove the need to choose a
consistent snapshot and distinguish readers from writers. Such a migration has no
evidence-backed need in this scope.

## Current compositions and acceptance

| Boundary | Behavior | Acceptance evidence |
|---|---|---|
| `open_read_only_runtime_authority` | Reads and validates events, Usage and Run records in one SQLite transaction; materializes `events` and `usage_records` | A second connection commits a valid signed terminal Run during verification; the audit remains coherent, and the next audit sees the new Run |
| `ReadOnlyDataSnapshotStore.authority_transaction` | Pins its first read; closes the reader at exit; SQLite rejects domain writes | A concurrent writer commits between two reads; both reads see the old count, and a later transaction sees the new count |
| Continuous retrospective | Uses the exact materialized Usage tuple audited alongside its events | Existing retrospective behavior checks plus consistent-audit test |
| Existing-ledger reconciliation | `read_existing_usage_ledger` uses `mode=ro`, closes its connection and reuses the canonical chain verifier; it never initializes schema or chmods source paths | Valid reconciliation preserves source permissions; a non-ledger database is rejected without creating ledger tables |
| Paired completed-execution audit | Hashes and consumes one verified Usage tuple; copies each existing Run database with Backup API and its CAS payloads into the temporary audit workspace | End-to-end audit refuses any writable store constructor under the source experiment root; a committed WAL-only value survives the private-copy path |
| Legacy engine replay inside that audit | Keeps existing terminal/execution validation and `dispatch_allowed=False`; refuses a nonterminal Run before replay | Existing completed and tampered paired-audit checks remain applicable; no new execution capability |

The returned authority's store/journal/ledger handles remain **live read-only
handles**, not a persistent frozen database clone. Consumers combining the audited
records use `events` and `usage_records`; re-reading a live ledger later does not
extend the earlier audit. CAS reopening continues to validate content identity.
The audit does not hold a long-lived reader transaction across model execution or
report rendering, and introduces no new canonical state owner.

## Similar patterns inspected

- The isolated investment-assessment runner already checks its frozen source
  bindings and retains the failed shared-directory fingerprint check. Its original
  reports and evidence are historical and are not rewritten by this fix.
- The old ignored retrospective script still contains writer initializers. It is a
  historical artifact; the maintained `continuous-study retrospective` entry uses
  the corrected offline authority composition.
- `TushareDailyRangeCache.acquire(saved_only=True)` promises no new provider fetch
  and preservation of uncertain/active acquisition ownership. Its acquisition-store
  constructor is writable; `saved_only` is not a forensic/filesystem-read-only mode.
  The offline range-projection reader remains the route for evidence-only reopening.
- Qualification, acquisition and canary runs that deliberately own writable state
  are not converted into audit readers merely because they also read records.

A true archival requirement should use a separately sealed copy and its explicit
manifest. Do not set [`immutable=1`](https://www.sqlite.org/uri.html) on a database
that another process can still change: that flag disables normal change detection
and locking assumptions. No live-root checkpoint, sidecar removal, driver
replacement or authority migration is part of this change.
