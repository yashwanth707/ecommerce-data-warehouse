-- Fact: Orders (Grain: one row per order)
-- Measures: total_items, total_value, total_freight,
--           payment_value, delivery_days, profit_margin
-- Degenerate dims: order_status, payment_type

{{
    config(
        materialized='table',
        tags=['gold', 'fact', 'business_ready'],
        cluster_by=['order_purchase_date']
    )
}}

WITH order_items_agg AS (
    SELECT
        order_id,
        COUNT(*)                                            AS total_items,
        SUM(price)                                          AS total_product_value,
        SUM(freight_value)                                  AS total_freight,
        SUM(total_item_value)                               AS total_value,
        COUNT(DISTINCT product_id)                          AS distinct_products,
        COUNT(DISTINCT seller_id)                           AS distinct_sellers
    FROM {{ ref('silver_order_items') }}
    GROUP BY 1
),

payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value)                                  AS payment_value,
        COUNT(*)                                            AS payment_count,
        LISTAGG(DISTINCT payment_type, ', ')
            WITHIN GROUP (ORDER BY payment_type)            AS payment_types,
        MAX(payment_installments)                           AS max_installments
    FROM {{ ref('silver_payments') }}
    GROUP BY 1
),

reviews_agg AS (
    SELECT
        order_id,
        AVG(review_score)                                   AS avg_review_score,
        COUNT(*)                                            AS review_count
    FROM {{ ref('silver_reviews') }}
    GROUP BY 1
)

SELECT
    -- Keys
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}   AS order_key,
    o.order_id,

    -- Dimension foreign keys
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} AS customer_key,
    DATE(o.order_purchase_timestamp)                        AS order_date_key,

    -- Degenerate dimensions
    o.order_status,
    pa.payment_types                                        AS payment_type,

    -- Timestamps
    o.order_purchase_timestamp,
    o.order_purchase_date,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Item measures
    COALESCE(oia.total_items, 0)                            AS total_items,
    COALESCE(oia.distinct_products, 0)                      AS distinct_products,
    COALESCE(oia.distinct_sellers, 0)                        AS distinct_sellers,

    -- Financial measures
    ROUND(COALESCE(oia.total_product_value, 0), 2)          AS total_product_value,
    ROUND(COALESCE(oia.total_freight, 0), 2)                AS total_freight,
    ROUND(COALESCE(oia.total_value, 0), 2)                  AS total_value,
    ROUND(COALESCE(pa.payment_value, 0), 2)                 AS payment_value,
    COALESCE(pa.payment_count, 0)                           AS payment_count,
    COALESCE(pa.max_installments, 0)                        AS max_installments,

    -- Delivery measures
    o.actual_delivery_days                                  AS delivery_days,
    o.estimated_delivery_days,
    o.delivery_performance,

    -- Review measures
    ROUND(COALESCE(ra.avg_review_score, 0), 2)              AS avg_review_score,
    COALESCE(ra.review_count, 0)                            AS review_count,

    -- Calculated measures
    ROUND(
        COALESCE(pa.payment_value, 0) -
        COALESCE(oia.total_product_value, 0) -
        COALESCE(oia.total_freight, 0),
        2
    )                                                       AS profit_margin,

    CASE
        WHEN COALESCE(oia.total_value, 0) > 0
            THEN ROUND(
                (COALESCE(pa.payment_value, 0) - COALESCE(oia.total_value, 0))
                / oia.total_value * 100, 2
            )
        ELSE 0
    END                                                     AS profit_margin_pct,

    -- Freight ratio
    CASE
        WHEN COALESCE(oia.total_product_value, 0) > 0
            THEN ROUND(
                COALESCE(oia.total_freight, 0) /
                oia.total_product_value * 100, 2
            )
        ELSE 0
    END                                                     AS freight_ratio_pct,

    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM {{ ref('silver_orders') }} o
LEFT JOIN order_items_agg oia ON o.order_id = oia.order_id
LEFT JOIN payments_agg pa ON o.order_id = pa.order_id
LEFT JOIN reviews_agg ra ON o.order_id = ra.order_id
