-- models/marts/dim_products.sql

-- This model creates a product dimension table.
-- Each row represents a single product and includes aggregated metrics
-- about its sales performance and customer reviews.

-- Step 1: Aggregate sales and review metrics for each product from the orders fact table.
WITH product_metrics AS (
    SELECT
        product_id,
        
        -- Sales Metrics
        SUM(price) AS total_revenue_generated,
        COUNT(DISTINCT order_id) AS number_of_orders,
        COUNT(order_item_sk) AS total_units_sold,
        
        -- Review Metrics
        AVG(review_score) AS average_review_score,
        
        -- Time-based Metrics
        MIN(order_purchase_at) AS first_sale_date,
        MAX(order_purchase_at) AS last_sale_date
        
    FROM
        {{ ref('fct_orders') }}
    WHERE
        product_id IS NOT NULL
    GROUP BY
        1
)

-- Final Step: Join the aggregated metrics with the product master data AND the translation table.
SELECT
    -- Key from the products staging table
    p.product_id,
    
    -- *** THE FIX IS HERE: Get the English category name from the translation table (alias 't') ***
    t.product_category_name_english,
    
    -- Product Attributes from the staging table
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    
    -- Aggregated metrics from our CTE
    pm.total_revenue_generated,
    pm.number_of_orders,
    pm.total_units_sold,
    pm.average_review_score,
    pm.first_sale_date,
    pm.last_sale_date

FROM
    -- We start with the product staging table as our base.
    {{ ref('stg_olist_products') }} p
LEFT JOIN
    -- Join the metrics onto the product master data.
    product_metrics pm ON p.product_id = pm.product_id
-- *** THE FIX IS HERE: We must join the translation table to get the English name. ***
LEFT JOIN
    {{ ref('stg_product_category_name_translation') }} t ON p.product_category_name = t.product_category_name
