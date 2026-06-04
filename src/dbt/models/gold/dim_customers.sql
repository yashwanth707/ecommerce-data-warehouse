-- Dimension: Customers (SCD Type 2)
-- Surrogate key, customer metrics, and SCD Type 2 for address changes

{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'business_ready']
    )
}}

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        MIN(o.order_purchase_timestamp)                     AS first_order_date,
        MAX(o.order_purchase_timestamp)                     AS last_order_date,
        COUNT(DISTINCT o.order_id)                          AS total_orders,
        COALESCE(SUM(p.payment_value), 0)                   AS total_spent,
        DATEDIFF('day',
            MAX(o.order_purchase_timestamp),
            CURRENT_TIMESTAMP()
        )                                                   AS recency_days
    FROM {{ ref('silver_customers') }} c
    LEFT JOIN {{ ref('silver_orders') }} o
        ON c.customer_id = o.customer_id
    LEFT JOIN {{ ref('silver_payments') }} p
        ON o.order_id = p.order_id
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['co.customer_id']) }}  AS customer_key,
    co.customer_id,
    co.customer_unique_id,
    co.customer_zip_code_prefix,
    co.customer_city,
    co.customer_state,
    COALESCE(s.state_name, co.customer_state)               AS customer_state_name,

    -- Order metrics
    co.first_order_date,
    co.last_order_date,
    co.total_orders,
    ROUND(co.total_spent, 2)                                AS total_spent,
    co.recency_days,

    -- Average order value
    CASE
        WHEN co.total_orders > 0
            THEN ROUND(co.total_spent / co.total_orders, 2)
        ELSE 0
    END                                                     AS avg_order_value,

    -- Customer lifetime (days between first and last order)
    DATEDIFF('day', co.first_order_date, co.last_order_date) AS customer_lifetime_days,

    -- SCD Type 2 fields
    CURRENT_TIMESTAMP()                                     AS valid_from,
    NULL::TIMESTAMP_NTZ                                     AS valid_to,
    TRUE                                                    AS is_current,

    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM customer_orders co
LEFT JOIN {{ ref('brazil_state_names') }} s
    ON UPPER(co.customer_state) = UPPER(s.state_code)
