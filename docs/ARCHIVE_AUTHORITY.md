# Historical archive authority

## Accepted capability

The first accepted capability is deliberately narrow: verify one immutable Common Crawl WARC
capture from its official HTTPS data origin. A locator binds the collection, target URL, capture
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

## What this does not prove

Common Crawl's `WARC-Date` is when the archive captured a representation. It is a conservative
upper bound on when that content existed; it is not necessarily when the publisher first released
the news. The current local `retrieved_at` is retained only as an operational audit timestamp and
never substitutes for historical availability.

Therefore a verified archive record is not yet a Source Version Receipt or Historical Evidence.
Before a method-quality case can be admitted, a source-specific adapter must also extract and
validate the publisher's `published_at` (and `source_updated_at` when available), bind the exact
extracted content, and apply a latency calibration frozen before the evaluated case. The modeled
`available_at = published_at + frozen latency` must not be later than the verified archive capture.
Missing or ambiguous publisher time makes the source ineligible rather than falling back to crawl
or current retrieval time.
