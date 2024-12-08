WITH ranked_products AS (
    SELECT
        product_id,
        title,
        price,
        category,
        description,
        image,
        rating_rate,
        rating_count,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY loaded_at DESC) AS row_num
    FROM {{ ref('stg_products') }}
),

product_status as (
    SELECT * from {{ ref('intm_product_status') }}
),

latest_products as (
    SELECT
        product_id,
        title,
        price,
        category,
        description,
        image,
        rating_rate,
        rating_count
    FROM ranked_products
    WHERE row_num = 1
),

last_products_status as (
    SELECT
        lp.product_id,
        lp.title,
        lp.price,
        lp.category,
        lp.description,
        lp.image,
        lp.rating_rate,
        lp.rating_count,
        ps.last_seen_date,
        ps.product_status
    FROM latest_products lp
    JOIN product_status ps
    ON lp.product_id = ps.product_id
)

SELECT
    *
FROM last_products_status
