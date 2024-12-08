WITH historical_prices AS (
    -- Select relevant columns from the source staging model
    SELECT
        product_id,
        CAST(loaded_at AS DATE) AS ds, -- Rename loaded_at to ds (date field for Prophet)
        price AS y                    -- Rename price to y (target field for Prophet)
    FROM {{ ref('stg_products') }}
    WHERE price IS NOT NULL and price > 0  -- Ensure price is not NULL to avoid errors in Prophet
),

validated_data AS (
    -- Ensure we have sufficient data for each product
    SELECT
        product_id,
        ds,
        y,
        COUNT(*) OVER (PARTITION BY product_id) AS product_data_count
    FROM historical_prices
),

filtered_products AS (
    -- Exclude products with insufficient data (e.g., less than 30 records)
    SELECT
        product_id,
        ds,
        y
    FROM validated_data
    WHERE product_data_count >= 30   -- Only include products with at least 30 data points
),

deduplicated_data AS (
    -- Deduplicate rows if there are multiple prices for the same product on the same day
    SELECT
        product_id,
        ds,
        AVG(y) AS y -- Use the average price if multiple prices exist for the same day
    FROM filtered_products
    GROUP BY product_id, ds
),

active_products_only AS (
    -- Filter for active products based on the product_status model
    SELECT
        dp.product_id,
        dp.ds,
        dp.y
    FROM deduplicated_data dp
    JOIN {{ ref('intm_product_status') }} ps
    ON dp.product_id = ps.product_id
    WHERE ps.product_status = 'active'
)

SELECT
    product_id,
    ds,
    y
FROM active_products_only
ORDER BY product_id, ds
