-- Silver Payments — Validated Payment Data

{{
    config(
        materialized='incremental',
        unique_key='payment_key',
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
    FROM {{ source('bronze', 'bronze_payments') }}
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
        raw_data:payment_sequential::INTEGER                AS payment_sequential,
        raw_data:payment_type::VARCHAR                      AS payment_type,
        raw_data:payment_installments::INTEGER              AS payment_installments,
        TRY_TO_DOUBLE(raw_data:payment_value::VARCHAR)      AS payment_value,
        ingestion_timestamp,
        source_file,
        checksum
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, payment_sequential
            ORDER BY ingestion_timestamp DESC
        ) AS _row_num
    FROM parsed
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential']) }}
                                                            AS payment_key,
    order_id,
    payment_sequential,
    LOWER(TRIM(payment_type))                               AS payment_type,
    payment_installments,
    ROUND(payment_value, 2)                                 AS payment_value,

    -- Per-installment amount
    CASE
        WHEN payment_installments > 0
            THEN ROUND(payment_value / payment_installments, 2)
        ELSE payment_value
    END                                                     AS installment_value,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND order_id IS NOT NULL
