-- Dimension: Products (with category hierarchy & review metrics)

{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'business_ready']
    )
}}

WITH product_metrics AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id)                         AS total_orders,
        SUM(oi.order_item_id)                               AS total_sold_quantity,
        AVG(oi.price)                                       AS avg_price,
        AVG(oi.freight_value)                               AS avg_freight
    FROM {{ ref('silver_order_items') }} oi
    GROUP BY 1
),

product_reviews AS (
    SELECT
        oi.product_id,
        AVG(r.review_score)                                 AS avg_review_score,
        COUNT(r.review_id)                                  AS total_reviews,
        COUNT_IF(r.sentiment_category = 'POSITIVE')         AS positive_reviews,
        COUNT_IF(r.sentiment_category = 'NEGATIVE')         AS negative_reviews
    FROM {{ ref('silver_order_items') }} oi
    INNER JOIN {{ ref('silver_reviews') }} r
        ON oi.order_id = r.order_id
    GROUP BY 1
)

-- Category translation from seed
SELECT
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} AS product_key,
    p.product_id,
    p.product_category_name,
    COALESCE(ct.product_category_name_english, 'Other')      AS category_name_english,

    -- Physical attributes
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,
    p.weight_category,
    p.product_photos_qty,

    -- Sales metrics
    COALESCE(pm.total_orders, 0)                            AS total_orders,
    COALESCE(pm.total_sold_quantity, 0)                      AS total_sold_quantity,
    ROUND(COALESCE(pm.avg_price, 0), 2)                     AS avg_price,
    ROUND(COALESCE(pm.avg_freight, 0), 2)                   AS avg_freight,

    -- Review metrics
    ROUND(COALESCE(pr.avg_review_score, 0), 2)              AS avg_review_score,
    COALESCE(pr.total_reviews, 0)                           AS total_reviews,
    COALESCE(pr.positive_reviews, 0)                        AS positive_reviews,
    COALESCE(pr.negative_reviews, 0)                        AS negative_reviews,

    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM {{ ref('silver_products') }} p
LEFT JOIN product_metrics pm ON p.product_id = pm.product_id
LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
LEFT JOIN {{ ref('product_category_name_translation') }} ct ON p.product_category_name = ct.product_category_name
