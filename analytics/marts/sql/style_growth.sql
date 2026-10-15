WITH x AS (
  SELECT date, security_id,
         LEAST(GREATEST(returns, -0.5), 1.0) AS r,
         LAG(pe_ratio) OVER (PARTITION BY security_id ORDER BY date) AS ppe,
         LAG(market_cap) OVER (PARTITION BY security_id ORDER BY date) AS pcap
  FROM lakehouse.curated.curated_alpha_fields_daily
), d AS (
  SELECT date,
         CASE WHEN ppe > 0 AND ppe < 12 THEN '1 Value (P/E<12)'
              WHEN ppe >= 12 AND ppe < 30 THEN '2 Core (12-30)'
              WHEN ppe >= 30 THEN '3 Growth (P/E>=30)'
              ELSE '4 Loss-making (P/E n/a)' END AS bucket,
         AVG(r) AS avg_r
  FROM x WHERE r IS NOT NULL AND pcap >= 3e8 AND date > DATE '2016-10-03'
  GROUP BY 1, 2
)
SELECT date, bucket,
       EXP(SUM(LN(1 + avg_r)) OVER (PARTITION BY bucket ORDER BY date)) AS growth_of_1
FROM d
