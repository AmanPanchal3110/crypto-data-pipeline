{{ config(
    materialized='incremental',
    unique_key=['coin_id','snapshot_date']
)}}

WITH base AS (
    SELECT 
        id AS coin_id,
        name,
        symbol,
        current_price::NUMERIC(18,6) AS usd_price,
        market_cap::BIGINT,
        total_volume::BIGINT,
        created_at::date AS snapshot_date,
        total_volume::NUMERIC / NULLIF(market_cap,0) 
            AS volume_to_market_cap_ratio
    FROM {{ ref('bronze_coin') }}
)

SELECT *,
    CASE
        WHEN market_cap >= 10000000000 THEN 'Large Cap'
        WHEN market_cap >= 1000000000 THEN 'Mid Cap'
        ELSE 'Small Cap'
    END AS market_cap_category,

    CASE
        WHEN volume_to_market_cap_ratio >= 0.15 THEN 'High Volume'
        WHEN volume_to_market_cap_ratio >= 0.05 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS volume_category,

    CASE
        WHEN usd_price >= 1000 THEN 'High Price'
        WHEN usd_price >= 100 THEN 'Medium Price'
        ELSE 'Low Price'
    END AS price_category

FROM base

{% if is_incremental() %}
WHERE snapshot_date > (
    SELECT MAX(snapshot_date) FROM {{ this }}
)
{% endif %}