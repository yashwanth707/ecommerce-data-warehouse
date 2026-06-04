-- Silver Orders — Cleansed & Typed Order Data
-- Source: bronze.bronze_orders (VARIANT)
-- Strategy: Incremental merge on order_id
-- Deduplication: Keep latest record by ingestion_timestamp
-- Ruggedness: Infers missing orders from bronze_order_items

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        cluster_by=['order_purchase_date'],
        tags=['silver', 'incremental']
    )
}}

WITH source AS (
    SELECT
        raw_data,
        ingestion_timestamp,
        source_file,
        checksum
    FROM {{ source('bronze', 'bronze_orders') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:order_id::VARCHAR                          AS order_id,
        raw_data:customer_id::VARCHAR                       AS customer_id,
        raw_data:order_status::VARCHAR                      AS order_status,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:order_purchase_timestamp::VARCHAR
        )                                                   AS order_purchase_timestamp,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:order_approved_at::VARCHAR
        )                                                   AS order_approved_at,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:order_delivered_carrier_date::VARCHAR
        )                                                   AS order_delivered_carrier_date,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:order_delivered_customer_date::VARCHAR
        )                                                   AS order_delivered_customer_date,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:order_estimated_delivery_date::VARCHAR
        )                                                   AS order_estimated_delivery_date,
        ingestion_timestamp,
        source_file,
        checksum,
        0                                                   AS is_inferred
    FROM source
),

inferred AS (
    SELECT
        raw_data:order_id::VARCHAR                          AS order_id,
        'UNKNOWN'                                           AS customer_id,
        'UNAVAILABLE'                                       AS order_status,
        NULL::TIMESTAMP_NTZ                                AS order_purchase_timestamp,
        NULL::TIMESTAMP_NTZ                                AS order_approved_at,
        NULL::TIMESTAMP_NTZ                                AS order_delivered_carrier_date,
        NULL::TIMESTAMP_NTZ                                AS order_delivered_customer_date,
        NULL::TIMESTAMP_NTZ                                AS order_estimated_delivery_date,
        CURRENT_TIMESTAMP()                                 AS ingestion_timestamp,
        'inferred_from_items'                               AS source_file,
        'inferred'                                          AS checksum,
        1                                                   AS is_inferred
    FROM {{ source('bronze', 'bronze_order_items') }}
    WHERE raw_data:order_id::VARCHAR IS NOT NULL
      {% if is_incremental() %}
      AND ingestion_timestamp > (
          SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ)
          FROM {{ this }}
      )
      {% endif %}
),

combined AS (
    SELECT * FROM parsed
    UNION ALL
    SELECT * FROM inferred
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY is_inferred ASC, ingestion_timestamp DESC
        ) AS _row_num
    FROM combined
)

SELECT
    order_id,
    customer_id,
    UPPER(TRIM(order_status))                               AS order_status,
    order_purchase_timestamp,
    DATE(order_purchase_timestamp)                          AS order_purchase_date,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    -- Calculated delivery metrics
    DATEDIFF('day',
        order_purchase_timestamp,
        order_delivered_customer_date
    )                                                       AS actual_delivery_days,

    DATEDIFF('day',
        order_purchase_timestamp,
        order_estimated_delivery_date
    )                                                       AS estimated_delivery_days,

    CASE
        WHEN order_delivered_customer_date <= order_estimated_delivery_date
            THEN 'ON_TIME'
        WHEN order_delivered_customer_date > order_estimated_delivery_date
            THEN 'LATE'
        ELSE 'PENDING'
    END                                                     AS delivery_performance,
    
    is_inferred,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND order_id IS NOT NULL
