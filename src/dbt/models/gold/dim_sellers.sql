-- Dimension: Sellers

{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'business_ready']
    )
}}

WITH seller_base AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM {{ ref('silver_sellers') }}
),

seller_metrics AS (
    SELECT
        oi.seller_id,
        COUNT(DISTINCT oi.order_id)                         AS total_orders,
        COUNT(oi.order_item_key)                            AS total_items_sold,
        SUM(oi.price)                                       AS total_revenue,
        AVG(oi.price)                                       AS avg_item_price,
        MIN(o.order_purchase_timestamp)                     AS first_sale_date,
        MAX(o.order_purchase_timestamp)                      AS last_sale_date
    FROM {{ ref('silver_order_items') }} oi
    INNER JOIN {{ ref('silver_orders') }} o
        ON oi.order_id = o.order_id
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.seller_id']) }}  AS seller_key,
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    COALESCE(st.state_name, s.seller_state)                 AS seller_state_name,

    -- Metrics
    COALESCE(sm.total_orders, 0)                            AS total_orders,
    COALESCE(sm.total_items_sold, 0)                        AS total_items_sold,
    ROUND(COALESCE(sm.total_revenue, 0), 2)                 AS total_revenue,
    ROUND(COALESCE(sm.avg_item_price, 0), 2)                AS avg_item_price,
    sm.first_sale_date,
    sm.last_sale_date,

    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM seller_base s
LEFT JOIN seller_metrics sm ON s.seller_id = sm.seller_id
LEFT JOIN {{ ref('brazil_state_names') }} st
    ON UPPER(s.seller_state) = UPPER(st.state_code)
