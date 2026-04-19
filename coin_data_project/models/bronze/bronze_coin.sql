{{ config(
    materialized='incremental'
)}}

SELECT *
FROM {{ source('raw','stg_coin_data') }}

{% if is_incremental() %}
WHERE created_at > (
    SELECT COALESCE(MAX(created_at), '1900-01-01'::date)
    FROM {{ this }}
)
{% endif %}