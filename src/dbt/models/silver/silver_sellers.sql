-- Silver Sellers — Validated Seller Data
-- Strategy: Incremental merge on seller_id
-- Ruggedness: Infers missing sellers from bronze_order_items

{{
    config(
        materialized='incremental',
        unique_key='seller_id',
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
    FROM {{ source('bronze', 'bronze_sellers') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:seller_id::VARCHAR                         AS seller_id,
        raw_data:seller_zip_code_prefix::VARCHAR            AS seller_zip_code_prefix,
        raw_data:seller_city::VARCHAR                       AS seller_city,
        raw_data:seller_state::VARCHAR                      AS seller_state,
        ingestion_timestamp,
        source_file,
        checksum,
        0                                                   AS is_inferred
    FROM source
),

inferred AS (
    SELECT
        raw_data:seller_id::VARCHAR                         AS seller_id,
        '00000'                                             AS seller_zip_code_prefix,
        'Unknown'                                           AS seller_city,
        'UN'                                                AS seller_state,
        CURRENT_TIMESTAMP()                                 AS ingestion_timestamp,
        'inferred_from_items'                               AS source_file,
        'inferred'                                          AS checksum,
        1                                                   AS is_inferred
    FROM {{ source('bronze', 'bronze_order_items') }}
    WHERE raw_data:seller_id::VARCHAR IS NOT NULL
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
            PARTITION BY seller_id
            ORDER BY is_inferred ASC, ingestion_timestamp DESC
        ) AS _row_num
    FROM combined
)

SELECT
    seller_id,
    LPAD(TRIM(seller_zip_code_prefix), 5, '0')              AS seller_zip_code_prefix,
    INITCAP(TRIM(seller_city))                              AS seller_city,
    UPPER(TRIM(seller_state))                               AS seller_state,
    
    is_inferred,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND seller_id IS NOT NULL
