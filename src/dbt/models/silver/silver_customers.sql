-- Silver Customers — Cleansed & Standardized Customer Data
-- Source: bronze.bronze_customers (VARIANT)
-- Strategy: Incremental merge on customer_id
-- Quality: Flag invalid zip codes, standardize city/state names
-- Ruggedness: Infers missing customers from bronze_orders to maintain referential integrity

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
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
    FROM {{ source('bronze', 'bronze_customers') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:customer_id::VARCHAR                       AS customer_id,
        raw_data:customer_unique_id::VARCHAR                AS customer_unique_id,
        raw_data:customer_zip_code_prefix::VARCHAR          AS customer_zip_code_prefix,
        raw_data:customer_city::VARCHAR                     AS customer_city,
        raw_data:customer_state::VARCHAR                    AS customer_state,
        ingestion_timestamp,
        source_file,
        checksum,
        0                                                   AS is_inferred
    FROM source
),

inferred AS (
    SELECT
        raw_data:customer_id::VARCHAR                       AS customer_id,
        'UNKNOWN'                                           AS customer_unique_id,
        '00000'                                             AS customer_zip_code_prefix,
        'Unknown'                                           AS customer_city,
        'UN'                                                AS customer_state,
        CURRENT_TIMESTAMP()                                 AS ingestion_timestamp,
        'inferred_from_orders'                              AS source_file,
        'inferred'                                          AS checksum,
        1                                                   AS is_inferred
    FROM {{ source('bronze', 'bronze_orders') }}
    WHERE raw_data:customer_id::VARCHAR IS NOT NULL
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
    UNION ALL
    -- Explicit fallback for inferred orders that use 'UNKNOWN' customer_id
    SELECT 
        'UNKNOWN' AS customer_id,
        'UNKNOWN' AS customer_unique_id,
        '00000' AS customer_zip_code_prefix,
        'Unknown' AS customer_city,
        'UN' AS customer_state,
        CURRENT_TIMESTAMP() AS ingestion_timestamp,
        'system' AS source_file,
        'system' AS checksum,
        1 AS is_inferred
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY is_inferred ASC, ingestion_timestamp DESC
        ) AS _row_num
    FROM combined
)

SELECT
    customer_id,
    customer_unique_id,

    -- Standardize zip code (must be 5 digits)
    LPAD(customer_zip_code_prefix, 5, '0')                  AS customer_zip_code_prefix,

    -- Standardize city name: title case, trim
    INITCAP(TRIM(customer_city))                            AS customer_city,

    -- Standardize state: uppercase, trim
    UPPER(TRIM(customer_state))                             AS customer_state,

    -- Data quality flags
    CASE
        WHEN customer_zip_code_prefix IS NULL
            OR LEN(TRIM(customer_zip_code_prefix)) < 3
            THEN TRUE
        ELSE FALSE
    END                                                     AS is_invalid_zip,

    CASE
        WHEN customer_city IS NULL
            OR TRIM(customer_city) = ''
            THEN TRUE
        ELSE FALSE
    END                                                     AS is_missing_city,
    
    is_inferred,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND customer_id IS NOT NULL
