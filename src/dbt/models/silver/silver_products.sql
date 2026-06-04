-- Silver Products — Enriched Product Data
-- Strategy: Incremental merge on product_id
-- Ruggedness: Infers missing products from bronze_order_items

{{
    config(
        materialized='incremental',
        unique_key='product_id',
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
    FROM {{ source('bronze', 'bronze_products') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:product_id::VARCHAR                        AS product_id,
        raw_data:product_category_name::VARCHAR             AS product_category_name,
        TRY_TO_NUMBER(
            raw_data:product_name_lenght::VARCHAR
        )                                                   AS product_name_length,
        TRY_TO_NUMBER(
            raw_data:product_description_lenght::VARCHAR
        )                                                   AS product_description_length,
        TRY_TO_NUMBER(
            raw_data:product_photos_qty::VARCHAR
        )                                                   AS product_photos_qty,
        TRY_TO_DOUBLE(raw_data:product_weight_g::VARCHAR)   AS product_weight_g,
        TRY_TO_DOUBLE(raw_data:product_length_cm::VARCHAR)  AS product_length_cm,
        TRY_TO_DOUBLE(raw_data:product_height_cm::VARCHAR)  AS product_height_cm,
        TRY_TO_DOUBLE(raw_data:product_width_cm::VARCHAR)   AS product_width_cm,
        ingestion_timestamp,
        source_file,
        checksum,
        0                                                   AS is_inferred
    FROM source
),

inferred AS (
    SELECT
        raw_data:product_id::VARCHAR                        AS product_id,
        'UNKNOWN'                                           AS product_category_name,
        1                                                   AS product_name_length,
        1                                                   AS product_description_length,
        0                                                   AS product_photos_qty,
        0.0                                                 AS product_weight_g,
        0.0                                                 AS product_length_cm,
        0.0                                                 AS product_height_cm,
        0.0                                                 AS product_width_cm,
        CURRENT_TIMESTAMP()                                 AS ingestion_timestamp,
        'inferred_from_items'                               AS source_file,
        'inferred'                                          AS checksum,
        1                                                   AS is_inferred
    FROM {{ source('bronze', 'bronze_order_items') }}
    WHERE raw_data:product_id::VARCHAR IS NOT NULL
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
            PARTITION BY product_id
            ORDER BY is_inferred ASC, ingestion_timestamp DESC
        ) AS _row_num
    FROM combined
)

SELECT
    product_id,
    LOWER(TRIM(product_category_name))                      AS product_category_name,
    product_name_length,
    product_description_length,
    COALESCE(product_photos_qty, 0)                         AS product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,

    -- Volume calculation (cm³)
    ROUND(
        COALESCE(product_length_cm, 0) *
        COALESCE(product_height_cm, 0) *
        COALESCE(product_width_cm, 0), 2
    )                                                       AS product_volume_cm3,

    -- Weight category
    CASE
        WHEN product_weight_g <= 500 THEN 'LIGHT'
        WHEN product_weight_g <= 2000 THEN 'MEDIUM'
        WHEN product_weight_g <= 10000 THEN 'HEAVY'
        ELSE 'VERY_HEAVY'
    END                                                     AS weight_category,
    
    is_inferred,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND product_id IS NOT NULL
