WITH x AS (
  SELECT date, security_id,
         LEAST(GREATEST(returns, -0.5), 1.0) AS r,
         LAG(market_cap) OVER (PARTITION BY security_id ORDER BY date) AS pcap
  FROM lakehouse.curated.curated_alpha_fields_daily
), d AS (
  SELECT date,
         CASE WHEN pcap >= 10e9 THEN '1 Large (>10B)'
              WHEN pcap >= 2e9 THEN '2 Mid (2-10B)'
              WHEN pcap >= 3e8 THEN '3 Small (0.3-2B)' END AS bucket,
         AVG(r) AS avg_r
  FROM x WHERE r IS NOT NULL AND pcap >= 3e8 AND date > DATE '2016-10-03'
  GROUP BY 1, 2
)
SELECT date, bucket,
       EXP(SUM(LN(1 + avg_r)) OVER (PARTITION BY bucket ORDER BY date)) AS growth_of_1
FROM d
