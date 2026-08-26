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
loss, duration, and official denominator provenance when a regional fraction is used.

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
store. Neither body nor ledger enters the repository. Current tests prove the contract,
artifact, and ledger mechanics with synthetic source content; no direct physical-disruption
source adapter or real event acceptance is yet claimed.

## Next acceptance gate

Before these observations can support another Phase 2 calibration hypothesis:

1. implement source-specific historical releases or vintages, including revision identity;
2. record prospective real-time delivery-latency distributions for each source class;
3. freeze a latency model before opening evaluation outcomes;
4. compare aggregator discovery with direct source coverage over at least 20 settled events;
5. verify market resolution and rules, not just the final price;
6. admit only evidence whose `published_at` and `available_at` are defensible at the replay
   cutoff.

This work repairs the research evidence plane. It does not reopen the failed Phase 2 cohort
or grant Phase 3, paper, account, or live-execution capability.
