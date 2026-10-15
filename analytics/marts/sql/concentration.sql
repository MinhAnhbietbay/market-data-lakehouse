WITH me AS (
  SELECT MAX(date) AS date FROM lakehouse.curated.curated_alpha_fields_daily
  GROUP BY date_trunc('month', date)
), r AS (
  SELECT t.date, t.market_cap,
         row_number() OVER (PARTITION BY t.date ORDER BY t.market_cap DESC) AS rk,
         SUM(t.market_cap) OVER (PARTITION BY t.date) AS tot
  FROM lakehouse.curated.curated_alpha_fields_daily t JOIN me ON t.date = me.date
  WHERE t.market_cap > 0
)
SELECT date,
       ROUND(100 * SUM(IF(rk <= 10, market_cap)) / MAX(tot), 2) AS top10_share_pct,
       ROUND(100 * SUM(IF(rk <= 50, market_cap)) / MAX(tot), 2) AS top50_share_pct,
       ROUND(100 * SUM(IF(rk <= 500, market_cap)) / MAX(tot), 2) AS top500_share_pct
FROM r GROUP BY date
