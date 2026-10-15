SELECT vendor_sector AS sector,
       COUNT(DISTINCT security_id) AS n_stocks,
       ROUND(SUM(market_cap) / 1e9, 1) AS total_mcap_bn,
       ROUND(approx_percentile(pe_ratio, 0.5), 1) AS median_pe,
       ROUND(approx_percentile(CAST(roe AS double), 0.5) * 100, 1) AS median_roe_pct,
       ROUND(approx_percentile(CAST(net_margin AS double), 0.5) * 100, 1) AS median_net_margin_pct
FROM lakehouse.curated.curated_alpha_fields_daily
WHERE date = (SELECT MAX(date) FROM lakehouse.curated.curated_alpha_fields_daily)
  AND vendor_sector <> '' AND market_cap > 0
  AND pe_ratio BETWEEN 0 AND 200
GROUP BY 1
