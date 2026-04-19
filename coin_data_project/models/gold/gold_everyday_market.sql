SELECT date,
       AVG(usd_price) AS avg_price,
       SUM(market_cap) AS total_market_cap,
       SUM(total_volume) AS total_volume
FROM {{ ref("fact_coin")}}
GROUP BY date