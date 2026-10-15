WITH x AS (
  SELECT date, vendor_sector AS sector,
         LEAST(GREATEST(returns, -0.5), 1.0) AS r,
         LAG(market_cap) OVER (PARTITION BY security_id ORDER BY date) AS pcap
  FROM lakehouse.curated.curated_alpha_fields_daily
  WHERE vendor_sector <> ''
), d AS (
  SELECT year(date) AS yr, sector, date, AVG(r) AS avg_r
  FROM x WHERE r IS NOT NULL AND pcap >= 3e8 AND year(date) BETWEEN 2017 AND 2025
  GROUP BY 1, 2, 3
)
SELECT CAST(yr AS varchar) AS year, sector,
       ROUND(100 * (EXP(SUM(LN(1 + avg_r))) - 1), 1) AS return_pct
FROM d GROUP BY 1, 2
