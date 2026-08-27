# News Evidence Assessment

Use this method only when the frozen Evidence Pack contains multiple news or public-information
items whose coverage quality must be assessed before forming a market thesis. It describes the
strength and limitations of the news evidence; it does not decide direction, target, horizon,
position size, or execution.

1. Inventory the cited sample before interpreting it: Evidence Item count, distinct claim count,
   distinct upstream source count, and distinct content/version lineage count. Never count a
   syndicated copy as independent confirmation merely because it arrived through another
   Provider.
2. Separate event facts, attributed claims, analyst or market opinions, and unsourced assertions.
   Preserve the Evidence Item identifiers supporting each category. A repeated opinion is not a
   repeated event observation.
3. Describe cross-source agreement and disagreement claim by claim. Agreement means independent
   sources support the same material claim; title similarity, shared wire lineage, or generic
   sentiment does not establish independence.
4. Identify timing limitations: missing publication or point-in-time availability, material update
   or revision differences, sparse coverage, one-source dominance, and observations close to the
   cutoff. Do not fill a missing timestamp or infer that an undated item was available.
5. Assign only a qualitative news-assessment confidence of `strong`, `moderate`, `weak`, or
   `insufficient`, and explain it from sample size, independence, agreement, and timing. This is
   confidence in the coverage assessment, not `CandidateImpact.confidence` and not a probability.
6. Carry disagreements and missing coverage into counterevidence, invalidation conditions, data
   gaps, or abstention. Do not convert sentiment counts into an automatic signal or weight.
7. This method may summarize already admitted Evidence Items in model-authored rationale. It may
   not mint an Evidence Item, replace an Evidence Pack reference, treat its own summary as evidence,
   or cite any news outside the frozen input.
