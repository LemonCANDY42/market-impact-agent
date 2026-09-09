# 2026 US Technology Earnings and AI-Capex Diagnostic

## Status and finding

This is an **opened, hindsight-informed development diagnostic** covering selected March–May 2026
US technology earnings. It is not Strict-PIT, a frozen holdout, an enabled Skill or a trading
result.

The initial story—revenue rose but nearly every large technology stock fell because AI spending
had no visible near-term return—is not supported as a cohort statement. The April 29 hyperscaler
reports split the next session: Microsoft and Meta fell, while Alphabet and Amazon rose; Apple also
rose after its April 30 report. A better working hypothesis is:

> The market prices the **incremental evidence for monetization relative to incremental spending,
> expectations and positioning**, not revenue growth or AI capex in isolation.

## What happened

| Issuer | Reported operating evidence | Investment evidence | Next-session diagnostic |
| --- | --- | --- | --- |
| Microsoft, April 29 | Revenue USD 82.9B, up 18%; Azure up 40% | Q3 capex USD 31.9B; Q4 expected above USD 40B; cloud gross margin declined | down 3.9% |
| Meta, April 29 | Revenue USD 56.31B, up 33% | 2026 capex guide raised to USD 125–145B | down 8.7% |
| Alphabet, April 29 | Revenue USD 109.9B, up 22%; Cloud up 63%, profitable, backlog above USD 460B | 2026 capex guide raised to USD 180–190B | up 10% |
| Amazon, April 29 | Revenue USD 181.5B, up 17%; AWS up 28% | Q1 cash capex USD 43.2B; trailing free cash flow compressed materially | up 0.8% |
| Apple, April 30 | Revenue USD 111.2B, up 17%; EPS up 22% | No issuer-reported AI-only capex measure; six-month PP&E purchases declined year over year | up about 3.6% early May 1 |

The first four reaction figures use the same April 30 market session, in which the S&P 500 and
Nasdaq both rose. This makes a broad market decline an inadequate explanation for their dispersion.

Primary issuer evidence:

- [Microsoft FY26 Q3 results and call](https://www.microsoft.com/en-us/investor/events/fy-2026/earnings-fy-2026-q3)
- [Meta Q1 2026 release](https://investor.atmeta.com/investor-news/press-release-details/2026/Meta-Reports-First-Quarter-2026-Results/default.aspx)
- [Alphabet Q1 2026 SEC exhibit](https://www.sec.gov/Archives/edgar/data/1652044/000165204426000043/googexhibit991q12026.htm)
- [Amazon Q1 2026 Form 10-Q](https://www.sec.gov/Archives/edgar/data/1018724/000101872426000014/amzn-20260331.htm)
- [Apple FY26 Q2 release](https://www.apple.com/ca/newsroom/2026/04/apple-reports-second-quarter-results/)
  and [Form 10-Q](https://www.sec.gov/Archives/edgar/data/320193/000032019326000013/aapl-20260328.htm),
  plus [Reuters' market-reaction report](https://www.marketscreener.com/news/apple-shares-rise-on-strong-quarterly-sales-in-run-up-to-ceo-change-ce7f58d9db8bf525)
- [Adobe Q1 2026 SEC exhibit](https://www.sec.gov/Archives/edgar/data/796343/000079634326000048/adbeex991q126.htm)
  and [Reuters' CEO/AI-disruption report](https://www.marketscreener.com/news/adobe-s-longtime-ceo-to-exit-role-amid-ai-disruption-shares-fall-ce7e5fd2da8af72d)
- [Nvidia FY27 Q1 SEC release](https://www.sec.gov/Archives/edgar/data/1045810/000104581026000051/q1fy27pr.htm)
  and [Reuters' market-reaction report](https://www.investing.com/news/commodities-news/shrug-for-nvidia-but-ipos-excite-4703593)
- [AP's common-session market reactions and macro context](https://apnews.com/article/oil-trump-iran-stocks-markets-42120b305ce6298712931e79b66a20de)

## Causal interpretation

The evidence supports a multi-variable explanation:

1. **Expectation delta:** positive year-over-year revenue is not the event. The event is the result
   and guidance relative to already elevated estimates and the price-implied hurdle.
2. **Monetization evidence:** Alphabet paired larger investment with exceptional Cloud growth,
   operating income and backlog; Amazon paired spend with faster AWS growth and favorable guidance.
   Those are direct counterexamples to “high AI capex causes the stock to fall.”
3. **Margin and cash-flow burden:** Microsoft disclosed lower cloud gross margin alongside a large
   forward spending step-up. Meta raised its capex range while its forward revenue guide was less
   differentiated. The marginal return evidence looked weaker, even though reported revenue grew.
4. **Issuer-specific facts:** regulation, user trends, tax benefits, CEO transition, supply costs,
   competition and prior positioning can dominate a one-factor capex story.
5. **Timing:** some AI infrastructure installed now may monetize over later years. A short-horizon
   stock reaction measures changed expectations, not the realized lifetime return on that capital.

Apple is a useful capital-intensity contrast, but not causal proof. Its filings do not isolate AI
capex, and a company can rent models or cloud capacity, shifting AI cost from owned PP&E into
operating expense. Its positive reaction also had direct support from product demand, outlook and
capital-return news. “Apple spent little on AI, therefore it rose” must remain an unproven inference.

Two boundary cases prevent the event family from collapsing into a capex rule. Adobe reported
growing revenue and AI-related adoption in March but fell amid CEO-transition and AI-disruption
concerns without a comparable hyperscaler capex reset. Nvidia reported very strong May growth but
had a muted negative after-hours response as a supplier whose favorable news was already heavily
priced. Both support expectations, competitive position and monetization as separate variables.

## Smallest useful experiment

Do not add these known outcomes to the frozen eight-case A-share development cohort. Register a
new event-family development study with one root earnings cluster per issuer and preserve a fixed
denominator, including positive, negative and muted reactions.

For every case freeze only information available before the reaction window:

- reported result versus a timestamped consensus or typed unknown;
- old and new revenue, cloud/AI, capex, margin and cash-flow guidance;
- valuation, prior return, positioning proxy and option-implied move where legitimately available;
- company-specific regulatory, product and governance facts;
- an executable instrument identity and contemporaneous market/sector benchmark.

Run paired ablations over identical inputs:

1. revenue/earnings surprise only;
2. plus capex **change versus prior guidance**, not capex level alone;
3. plus monetization evidence such as cloud growth, profit, backlog and forward guide;
4. plus margin/free-cash-flow burden;
5. plus valuation, prior positioning and issuer-specific counterevidence.

Evaluate benchmark-adjusted 1-session and 5-session return, adverse/favorable excursion, direction,
calibration and incremental value of each information block. Use Apple, Adobe and Nvidia as
predeclared contrasts, not post-hoc exceptions. Only repeated paired value on later pristine cases
can nominate a limited `earnings_capex_monetization` Skill. Any A-share, Hong Kong or ETF
transmission requires its own evidence-backed issuer→variable→industry→instrument path; US price
reaction alone cannot manufacture that mapping.
