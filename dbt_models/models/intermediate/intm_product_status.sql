with last_info_products AS (
    -- Get the latest seen date for each product
    SELECT
        product_id,
        ANY_VALUE(title) AS title,
        MAX(loaded_at) AS last_seen_date
    FROM {{ ref('stg_products') }}
    GROUP BY product_id
),

product_status AS (
    -- Determine the product status based on recent data
    SELECT
        product_id,
        title,
        last_seen_date,
        CASE
            WHEN last_seen_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'active'
            ELSE 'out_of_sale'
        END AS product_status
    FROM last_info_products
)

SELECT
    *
FROM product_status
