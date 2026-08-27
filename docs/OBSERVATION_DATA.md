# Observation Data Boundary

This boundary keeps source aggregation useful without allowing a convenient API response
to become point-in-time truth. Observation Providers acquire external data; the Harness
owns immutable capture, time semantics, source identity, evidence promotion, and policy.

## First accepted slice

Three disabled, unverified read-only adapters share one contract:

| Provider | Role | Current time fields | Important gaps |
| --- | --- | --- | --- |
| `polymarket-public` | Direct prediction-market snapshot | Market `updatedAt`, open/close times, actual Harness receipt | No implemented historical/vintage or revision feed; some resolution sources are absent. |
| `kalshi-public` | Direct regulated prediction-market snapshot | Retrieval-observed quote time, metadata `updated_time`, open/close times, actual Harness receipt | `updated_time` is not a quote publication time; no implemented historical/vintage or revision feed in this slice. |
| `world-monitor-predictions` | Aggregated Polymarket/Kalshi discovery | Aggregator `fetchedAt`, close time, actual Harness receipt | No upstream market update time, Polymarket child-market identifier, rules, order book, or history. API key required. |

On 2026-08-26 the project CLI captured and validated current public Polymarket and Kalshi
snapshots through the real endpoints. That proves the bounded public read, normalization,
private content-addressed write, and validation path for those requests. It does not verify
general completeness, future schema stability, historical point-in-time correctness,
licensing for redistribution, probability calibration, alpha, or execution readiness. All
three Observation Providers remain disabled and have no verified capability.

World Monitor was contract-tested against its public OpenAPI degradation shape. A real
server-to-server request without a key returned an access denial, as documented; no working
World Monitor path is claimed.

## Time contract

The time fields answer different questions and must not be substituted for one another:

| Field | Meaning | Backtest use |
| --- | --- | --- |
| `occurred_at` | When the event, release value, quote state, or measurement occurred. | Orders events and values; it does not prove availability. |
| `published_at` | When the source first published this exact version. | Starting point for historical availability. Unknown stays null. |
| `source_updated_at` | When the upstream source says this record or version changed. | Revision lineage and current-state provenance. Unknown stays null. |
| `aggregator_fetched_at` | When an aggregator captured its upstream snapshot. | Describes aggregator freshness, never reconstructs a missing upstream update time. |
| `available_at` | When the strategy could use the information. | The Event Envelope visibility gate. |
| `retrieved_at` | When this Harness invocation wrote its local copy. | Audit, cache identity, and replay diagnostics only. |

For a real-time capture, `available_at` is the measured receipt time. For historical replay,
it must come from a source-reported availability time or a versioned latency model applied
to `published_at`. A later historical backfill's `retrieved_at` is not substituted for
historical availability. If the source supplies only an event date but no publication,
vintage, or revision availability, the observation is retained for exploratory research but
cannot be promoted into strict point-in-time Evidence.

Kalshi's `updated_time` is retained as `source_updated_at` because it is useful source
metadata, but it is not treated as the occurrence or publication time of the bid, ask, or
last price. The quote state is `retrieval_observed` at Harness receipt. Its source
publication time remains null, so this current adapter fails closed for Evidence promotion
instead of assigning the metadata update time to the quote.

World Monitor's `fetchedAt` is likewise an aggregator snapshot time, not an upstream market
publication time. It remains available for discovery freshness and audit, while
`published_at` stays null and the current World Monitor record is not Evidence-ready.

Every modeled availability must carry its immutable `latency_model` reference with a source
class, model identifier, fixed model version, and calibration reference. A timestamp with
`modeled_latency` but no such reference is rejected at construction and again at Evidence
promotion. The referenced model must document its market/session calendar and distribution
or fixed rule. Prospective live receipt measurements should replace guesses. Newswire,
exchange feed, public website, scheduled macro release, and aggregator cache are separate
latency classes.

## Aggregator and direct-source boundary

```text
World Monitor / later OpenBB or news aggregators
    -> discovery observation (aggregator identity retained)
    -> resolve original upstream source and record identity
    -> direct source adapter where rules, revisions, or history matter
    -> immutable decoded JSON response + normalized observation
    -> availability/latency gate
    -> Evidence Item -> Event Envelope
```

When the upstream market identity is defensible, an aggregator copy and its original record
may share one stable `claim_id`; they are not independent corroboration. World Monitor's
Polymarket `id` is only the event slug, not the direct child-market id. Event slug plus title
is insufficient because one live Polymarket event can contain multiple child markets with
the same displayed question but different market identities. World Monitor Polymarket
records therefore receive an explicitly discovery-scoped claim identity and cannot be
joined to a direct child market automatically. A mismatch between the supplied id and
canonical upstream URL still fails closed. Direct and aggregate copies retain different
`source_ref` values so latency, transformation, and discrepancies remain inspectable.

The direct/aggregated distinction is intentionally not hidden by normalization. Conversion
cannot recreate fields that an aggregator discarded. World Monitor is therefore a discovery
source for prediction markets, while Polymarket and Kalshi remain the canonical candidates
for market definitions, rules, prices, and later historical work.

## Local commands

Public direct captures need no credential:

```bash
uv run market-impact prediction capture --provider polymarket --limit 20
uv run market-impact prediction capture --provider kalshi --limit 20
```

World Monitor reads its key only from the process environment:

```bash
uv run market-impact prediction capture --provider world-monitor --limit 20
```

All bundles are private local JSON by default under the ignored
`.market-impact/observations/` directory. Validate one without network access:

```bash
uv run market-impact prediction validate BUNDLE.json
```

The bundle contains the complete decoded JSON response, normalized observations, query,
Provider manifest, all time fields, raw-record hashes, and a content-derived identity. It
does not claim to preserve HTTP framing or the exact response bytes. Provider text is data,
never an instruction to execute.

## Digital Oracle assessment

[`komako-workshop/digital-oracle`](https://github.com/komako-workshop/digital-oracle)
is a useful MIT-licensed Provider catalogue and implementation reference. Its small injected
HTTP clients, partial-failure isolation, and snapshot fixtures are good patterns. It is not
imported as a mandatory dependency because its product logic and returned records do not
provide the unified availability, revision, upstream-identity, and evidence-promotion
semantics required here. In particular, its test snapshots record `captured_at`, while its
replay client returns only the stored response; that is sufficient for fixture playback but
not this Harness's point-in-time contract.

No Digital Oracle code was copied in this slice, and its MIT license is compatible with the
repository's Apache-2.0 license. Calling World Monitor's external API does not grant rights
to licensed news or market data; source retention and redistribution rights remain separate
from the first-party code license.

## Prospective accrual observations

The first prospective physical-energy study adds a narrow Candidate Event Observation
contract above the general Source Observation boundary. It preserves direct upstream and
Provider identity, source tier, occurrence/publication/update times, measured actual receipt,
raw-content hash, a non-verbatim claim summary, event nature, affected commodity, estimated
loss, duration, official denominator provenance when a regional fraction is used, and the
exact Source Coverage Registration and Coverage Receipt under which it was observed.

Only `actual_receipt` availability is accepted for prospective accrual. Established news,
specialist, community, and aggregator observations may be retained as discoveries but do not
qualify the event. A later official or directly involved primary observation may supersede
the discovery through a linear revision chain. The deterministic Accrual Decision then
records admission or every applicable non-admission reason under the frozen registration.

The private SQLite ledger is registration-bound, append-only through the application,
hash-chained, idempotent by observation identity, mode `0600`, and fully replayed on reopen.
Missing onset, commodity, magnitude, unit, or duration values remain explicit `null` fields
and produce a retained `missing_critical_data` non-admission instead of disappearing before
evaluation. A lineaged later source revision may fill a missing identity fact but cannot
change a previously established occurrence time or commodity.
It stores normalized observation content and raw-content hashes; the exact external body is
verified against that hash and copied into a sibling private content-addressed artifact
store. Neither body nor ledger enters the repository.

The frozen Source Coverage Registration adds three explicit providers: GDELT multilingual
news metadata for mandatory global discovery, optional EIA Today in Energy RSS metadata,
and mandatory ENTSOG Urgent Market Messages for direct European gas confirmation. Every
polling cycle writes a Coverage Receipt in registered source order. A failed mandatory
attempt is not converted into an empty result: it makes the receipt incomplete, retains any
candidate found by another source, and adds `source_coverage_incomplete` to deterministic
non-admission. Discovery and optional official metadata are never occurrence-eligible.

The ENTSOG adapter retains the exact JSON batch privately, preserves publication and update
times, uses actual Harness receipt as `available_at`, keeps the latest message revision per
thread, and converts only reported `kWh/d` or `kWh/h` unavailable capacity under the frozen
5.8 million Btu per BOE convention. Unsupported units remain missing rather than receiving
an assumed gas heating value. Current deterministic tests cover complete, partial-failure,
planned, duplicate-revision, impossible-time, accrual, and cutoff-freeze paths. No real event
acceptance is yet claimed.

The first live cycle on 2026-08-26 reached EIA and ENTSOG but not GDELT. Its immutable
receipt marked the mandatory discovery attempt failed, so the CLI returned non-success and
the valid empty ledger remained at zero decisions. This is evidence that degradation is
preserved and blocks accrual; it is not evidence that the full registered source set is
currently healthy.

For an admitted event, the freeze scheduler uses the recorded cutoff—not scheduler run time—
as the Evidence Pack `as_of`. It includes only same-event observations visible by that
cutoff, plus the pre-outcome Exposure Registry and exact Pattern Packs. Its content-derived
manifest binds the study, source coverage, registry, accrual decision, evidence documents,
raw hashes, and Pattern Packs and is revalidated on every idempotent reopen.

## Normalized historical news batches

The first Provider-neutral news boundary is now implemented in
`news-observation-batch.schema.json`. A content-identified `NewsQuery` freezes historical or
masked-replay mode, an exact UTC half-open `[start_at, end_at)` publication window, terms,
per-source limit, and the ordered Provider/version/upstream-source registrations. A batch is
invalid if attempts are missing, added, reordered, or silently routed through an unregistered
fallback.

Every attempt reports one typed state: `data`, `no_data`, `not_configured`, `rate_limited`, or
`error`. Only actual `data`/`no_data` responses carry a raw-response hash and record counts;
rate-limit and error states carry explicit error classification and cannot masquerade as empty
news. Deterministic filtering happens before the per-source limit. Historical and future-shifted
masked replays both reject missing `published_at`, out-of-window publication, missing
`available_at`, and `source_updated_at` or availability at or after the cutoff without consulting
the host clock. Publication selects the half-open query window. For an exact accepted content
version, `available_at` cannot precede either `published_at` or the optional `source_updated_at`,
and all three independently gate the version at the cutoff. This ordering does not equate
availability with Harness receipt or `retrieved_at`.

Accepted observations preserve Provider/source versions, upstream record and lineage identities,
raw-content hash, `published_at`, optional `source_updated_at`, and `available_at`. Deduplication
uses version lineage across Providers, never title text, so syndicated copies are not independent
confirmation while different articles with the same headline remain distinct. Rejection counts
are reconciled against every raw result, and the schema plus canonical parser fail closed on
identity or count tampering.

This slice normalizes already fetched records; it does not yet live-enable Yahoo, GDELT, Bloomberg,
Reddit, StockTwits, or another news vendor. Licensed text remains outside committed artifacts.
The separate content-identified `news-evidence-assessment` Skill can describe sample size,
independent claims/sources, fact-versus-opinion mix, disagreement, timing gaps, and a qualitative
coverage-assessment confidence. It is read-only, cannot mint Evidence, cannot set an automatic
signal or weight, and explicitly does not replace `CandidateImpact.confidence`.

## Next acceptance gate

Before this prospective study can complete its Phase 2 calibration:

1. add pre-registered direct confirmation for oil and non-European infrastructure;
2. implement source-specific historical releases or vintages, including revision identity;
3. record prospective real-time delivery-latency distributions for each source class;
4. freeze a latency model before opening evaluation outcomes;
5. compare aggregator discovery with direct source coverage over at least 20 settled events;
6. verify market resolution and rules, not just the final price;
7. admit only evidence whose `published_at` and `available_at` are defensible at the replay
   cutoff.

This work repairs the research evidence plane. It does not reopen the failed Phase 2 cohort
or grant Phase 3, paper, account, or live-execution capability.
