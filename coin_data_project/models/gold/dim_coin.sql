SELECT coin_id,
       name,
       symbol,
       market_cap_category,
       volume_category,
       price_category
FROM {{ ref('silver_coin') }}