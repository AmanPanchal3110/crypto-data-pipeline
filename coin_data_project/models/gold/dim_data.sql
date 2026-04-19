SELECT DATE(snapshot_date) AS date,
       EXTRACT(DAY FROM snapshot_date) AS day,
       EXTRACT(MONTH FROM snapshot_date) AS month,
       EXTRACT(YEAR FROM snapshot_date) AS year
FROM {{ ref('silver_coin') }}