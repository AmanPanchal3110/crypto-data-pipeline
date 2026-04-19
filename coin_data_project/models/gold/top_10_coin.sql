SELECT f.*,
       d.name,
       d.symbol,
       d.market_cap_category,
       d.volume_category,
       d.price_category
FROM {{ ref("fact_coin")}} AS f
JOIN {{ ref("dim_coin") }} AS d
ON f.coin_id = d.coin_id
ORDER BY f.market_cap DESC
LIMIT 10