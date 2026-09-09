# Qlib boundary and fee sensitivity

This follow-up was declared before its fits, after observing the literal reproduction. It is a sensitivity analysis of an already observed historical test, not new confirmation or model selection on a fresh period.

- Keep published linear and LightGBM configurations, seed 17 and 16 threads; no hyperparameter search.
- The Alpha158 label is `Ref($close, -2) / Ref($close, -1) - 1`. Purge the final two trading feature dates at each train/validation boundary. Training and train-fitted preprocessing end 2014-12-29; validation ends 2016-12-28. Test starts 2017-01-01 as before.
- Run the same upstream account once at its configured costs, then rerun the actual upstream backtest on fixed predictions at doubled entry/exit/minimum fees (0.001 / 0.003 / 10). Do not approximate this by simply subtracting extra fees from the previous equity curve.
- Report Qlib's arithmetic annualized mean excess return, information ratio and arithmetic cumulative-excess drawdown. In this version `risk_analysis(freq='day')` uses 238 periods per year; these are not CAGR or compounded wealth drawdown.
- The runtime warns that adjusted-price data lack a usable factor and therefore the 100-share trade-unit rule is unsupported. This prevents treating the reproduction as native raw-price/lot-size/T+1 execution acceptance.
