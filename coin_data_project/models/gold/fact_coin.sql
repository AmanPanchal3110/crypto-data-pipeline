SELECT coin_id,
       DATE(snapshot_date) AS date,
         usd_price,
         market_cap,
         total_volume,
         volume_to_market_cap_ratio
FROM {{ ref('silver_coin') }}