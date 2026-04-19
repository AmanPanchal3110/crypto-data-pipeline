WITH ranked AS(
    SELECT *,
           LAG(usd_price) OVER (PARTITION BY coin_id ORDER BY date) AS prev_price
    FROM {{ ref("fact_coin")}}
) 
SELECT *,
      (usd_price - prev_price) / prev_price AS price_growth_rate
FROM ranked
WHERE prev_price IS NOT NULL