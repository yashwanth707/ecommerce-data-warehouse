-- Silver Order Items — Enriched Line-Level Data
-- Source: bronze.bronze_order_items (VARIANT)
-- Strategy: Incremental merge on composite key (order_id + order_item_id)
-- Calculations: item total, freight ratio

{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        tags=['silver', 'incremental']
    )
}}

WITH source AS (
    SELECT
        raw_data,
        ingestion_timestamp,
        source_file,
        checksum
    FROM {{ source('bronze', 'bronze_order_items') }}
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
        raw_data:order_item_id::INTEGER                     AS order_item_id,
        raw_data:product_id::VARCHAR                        AS product_id,
        raw_data:seller_id::VARCHAR                         AS seller_id,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:shipping_limit_date::VARCHAR
        )                                                   AS shipping_limit_date,
        TRY_TO_DOUBLE(raw_data:price::VARCHAR)              AS price,
        TRY_TO_DOUBLE(raw_data:freight_value::VARCHAR)      AS freight_value,
        ingestion_timestamp,
        source_file,
        checksum
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY ingestion_timestamp DESC
        ) AS _row_num
    FROM parsed
)

SELECT
    -- Composite key
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }}
                                                            AS order_item_key,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,

    -- Financial metrics
    ROUND(price, 2)                                         AS price,
    ROUND(freight_value, 2)                                 AS freight_value,
    ROUND(price + freight_value, 2)                         AS total_item_value,

    -- Freight as percentage of price
    CASE
        WHEN price > 0
            THEN ROUND(freight_value / price * 100, 2)
        ELSE 0
    END                                                     AS freight_percentage,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND order_id IS NOT NULL
  AND order_item_id IS NOT NULL
