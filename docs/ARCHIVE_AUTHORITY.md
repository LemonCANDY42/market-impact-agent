# Historical archive authority

## Accepted capability

The accepted capability is deliberately narrow: locate an exact URL in one fixed Common Crawl
collection before an explicit timezone-aware cutoff, then verify the resulting immutable WARC
capture from the official HTTPS data origin. The index adapter is pinned to
`https://index.commoncrawl.org`, requests exact matching and HTTP 200 records, bounds the response,
and rejects a returned host/path/query mismatch. A locator binds the collection, target URL, capture
timestamp, object path, byte offset and length, upstream payload digest, HTTP status, and a derived
content identity. The adapter then requires:

- an exact `206` byte-range response with a matching `Content-Range` and byte count;
- exactly one complete gzip member and one CRLF-framed WARC response record; its decompressed
  member is bounded at 20 MiB before materialization;
- matching WARC target, capture timestamp, and HTTP status;
- a matching SHA-1 Base32 payload digest and, when present, block digest; and
- no `WARC-Truncated` marker for archive-capture acceptance.

Redirects, alternate origins, path traversal, widened ranges, malformed records, digest mismatch,
and duplicate authority-bearing WARC headers fail closed. Retrieved payload bytes remain in memory
for caller-side extraction but are not included in the verification report or committed fixtures.
The adapter has no trading capability.

Reproduce the accepted complete-record path with:

```bash
uv run market-impact archive common-crawl-verify \
  --locator examples/research/common-crawl-complete-capture-v1.json
```

This locator is the complete Common Crawl example published with its CDXJ documentation. The live
acceptance returned the bound target and timestamp, matched both WARC digests, reported no
truncation, and set `archive_capture_accepted` to `true`. A separate real 2018 record with
`WARC-Truncated: length` passed transport and digest validation but correctly remained capture-
ineligible.

## Internet Archive replay authority

The second archive Provider uses the official Wayback CDX endpoint and raw `id_` replay. Its closed
locator binds the exact original URL, capture timestamp, HTTP 200 status, HTML media type, CDX
SHA-1 Base32 digest, and derived source-version identity. Lookup remains exact-URL and cutoff-bound;
the replay stays on the official HTTPS origin, follows no redirects, is capped at 20 MiB, and is
accepted only when the replay body's computed digest exactly matches CDX. Metadata reports retain
the payload SHA-256 but never the body.

This is not represented as Common Crawl WARC verification. The public CDX/replay path does not
expose the WARC block and block digest used by the Common Crawl adapter, so its authority claim is
specifically “CDX-indexed raw replay with matching payload digest.” Source publication/update time
still comes from a separate source-specific extractor.

The first live locator identifies the CSRC 24 September 2024 merger-reform page at
`2024-09-24T14:27:38Z`. Its raw 18,279-byte replay matches CDX digest
`sha1:QSP2SVW3ZP7ZKAFXZJPGQRENY57UHU4P` and payload SHA-256
`2cf631274679bb9902d459429c0ef19f37a826ebc40999ce0d8c689e8e3c5865`.

```bash
uv run market-impact archive internet-archive-locate \
  --url https://www.csrc.gov.cn/csrc/c100028/c7508366/content.shtml \
  --not-after 2024-10-08T01:25:00Z

uv run market-impact archive internet-archive-verify \
  --locator examples/research/csrc-2024-merger-reform-internet-archive-v1.json
```

## Accepted source-specific admissions

The first source-specific path covers CSRC HTML pages only. The committed locator
`examples/research/csrc-2024-policy-common-crawl-v1.json` identifies the CSRC 24 September 2024
financial-support briefing. A live 2026-08-27 replay located and verified the exact record captured
at `2024-10-07T17:13:01Z`; its payload SHA-256 is
`be3637171bede33919cf131487542016c50204a7270e8bd0652508dfdede5a62`.

The CSRC extractor accepts only the official CSRC host, a complete HTTP 200 HTML capture, one
source date, and a non-empty page title. Because this page exposes only a calendar date, the
source-reported `published_at` and `available_at` are conservatively placed at 23:59:59
Asia/Shanghai on that date. The archive capture remains a separate `authority_at`; a checkpoint
cannot count authenticated availability until both source availability and archive authority
precede its cutoff. The public page body is never written to the evidence manifest or repository.

```bash
uv run market-impact archive common-crawl-locate \
  --collection CC-MAIN-2024-42 \
  --url https://www.csrc.gov.cn/csrc/c106311/c7508374/content.shtml \
  --not-after 2024-10-08T01:25:00Z

uv run market-impact regime evidence-capture-csrc \
  --locator examples/research/csrc-2024-policy-common-crawl-v1.json \
  --case-key cn-2024-policy-melt-up \
  --claim-id csrc-2024-09-24-financial-support-briefing \
  --lineage-id csrc-c7508374
```

The second path covers State Council HTML pages on the official `gov.cn` hosts. The committed
locator `examples/research/gov-cn-2024-stimulus-common-crawl-v1.json` binds the 25 September 2024
English stimulus summary to the exact Common Crawl record captured at `2024-10-09T22:20:54Z`.
A live replay verified its WARC and payload digests; the payload SHA-256 is
`7fcd736e7847dd024065a45e00c030e5e95618a3f82a32946a270f51ec680598`.

The archived page retains one exact `Updated: September 25, 2024 08:58` value. The extractor
requires that value to be unique, interprets it in Asia/Shanghai, and binds it as
`published_at`, `source_updated_at`, and source-reported `available_at`. A current-page publication
meta tag is not used because it was absent from the archived representation. The separate archive
capture remains `authority_at`, so this record is not point-in-time authority for the 30 September
or 8 October checkpoints; it becomes eligible at the 14 October checkpoint.

```bash
uv run market-impact regime evidence-capture-state-council \
  --locator examples/research/gov-cn-2024-stimulus-common-crawl-v1.json \
  --case-key cn-2024-policy-melt-up \
  --case-key cn-2024-post-rally-whipsaw \
  --claim-id state-council-2024-09-25-stimulus-summary \
  --lineage-id state-council-content-WS66f3602ec6d0868f4e8eb3c0
```

The third source-specific path covers National Bureau of Statistics HTML releases. It requires the
official `stats.gov.cn` host, a unique exact `PubDate` in `YYYY/MM/DD HH:MM`, the same timestamp in
visible content, an HTML title, and an accepted archive replay. Two real August 2024 vintages now
pass: the 9 September CPI release captured the same day, and the 14 September national-economy
release captured on 17 September. Both are authoritative before the 24 September checkpoint and
remain separate content-identified records under one NBS source lineage.

```bash
uv run market-impact regime evidence-capture-nbs \
  --locator examples/research/nbs-2024-08-economy-internet-archive-v1.json \
  --case-key cn-2024-policy-melt-up \
  --case-key cn-2024-post-rally-whipsaw \
  --claim-id nbs-2024-08-national-economy \
  --lineage-id nbs-t20240914-1956487
```

## What this does not prove

Common Crawl's `WARC-Date` is when the archive captured a representation. It is a conservative
upper bound on when that content existed; it is not necessarily when the publisher first released
the news. The current local `retrieved_at` is retained only as an operational audit timestamp and
never substitutes for historical availability.

Therefore a generic verified archive record is not yet Historical Evidence. A source-specific
adapter must also extract and validate the publisher's `published_at` (and `source_updated_at` when
available), bind the exact extracted content, and state whether availability is an actual receipt,
a source-reported time, or publication plus a frozen latency model. Modeled latency requires a
content-identified calibration; no record may silently fall back to crawl time or current local
retrieval time. The CSRC, State Council, and NBS paths establish this contract for three official
source classes only. They do not establish Bloomberg, Reuters, PBC or other macro vintages,
positioning, filing, exchange, or market-price authority.
