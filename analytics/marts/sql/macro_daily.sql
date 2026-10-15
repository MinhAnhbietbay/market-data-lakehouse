SELECT date, fed_funds_rate, yield_2y, yield_10y,
       yield_10y - yield_2y AS curve_10y_2y,
       vix, credit_spread_hy, sp500_index
FROM lakehouse.curated.curated_macro_daily
WHERE date >= DATE '2016-10-01'
