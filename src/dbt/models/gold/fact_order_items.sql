-- Fact: Order Items (Grain: one row per order-item)
-- Enables product-level drill-through, seller performance,
-- and basket analysis in Power BI.
-- Bridges fact_orders to dim_products and dim_sellers.

{{
    config(
        materialized='table',
        tags=['gold', 'fact', 'business_ready'],
        cluster_by=['order_date_key']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }}
                                                                AS order_item_key,
    oi.order_id,

    -- Foreign keys (surrogate)
    {{ dbt_utils.generate_surrogate_key(['oi.order_id']) }}     AS order_key,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }}   AS customer_key,
    {{ dbt_utils.generate_surrogate_key(['oi.product_id']) }}   AS product_key,
    {{ dbt_utils.generate_surrogate_key(['oi.seller_id']) }}    AS seller_key,
    DATE(o.order_purchase_timestamp)                            AS order_date_key,

    -- Degenerate dimensions
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,

    -- Financial measures
    ROUND(oi.price, 2)                                          AS price,
    ROUND(oi.freight_value, 2)                                  AS freight_value,
    ROUND(oi.total_item_value, 2)                               AS total_item_value,

    CURRENT_TIMESTAMP()                                         AS dbt_updated_at

FROM {{ ref('silver_order_items') }} oi
INNER JOIN {{ ref('silver_orders') }} o ON oi.order_id = o.order_id
