-- models/staging/stg_products.sql

WITH base AS (
    SELECT
        id AS product_id,
        title,
        price,
        category,
        description,
        image,
        rating_rate,
        rating_count,
        loaded_at
    FROM {{ source('raw', 'raw_products') }}
)

SELECT
    product_id,
    title,
    price,
    category,
    description,
    image,
    rating_rate,
    rating_count,
    loaded_at
FROM base
