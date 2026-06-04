-- Silver Reviews — Deduplicated Review Data

{{
    config(
        materialized='incremental',
        unique_key='review_id',
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
    FROM {{ source('bronze', 'bronze_reviews') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:review_id::VARCHAR                         AS review_id,
        raw_data:order_id::VARCHAR                          AS order_id,
        TRY_TO_NUMBER(raw_data:review_score::VARCHAR)      AS review_score,
        raw_data:review_comment_title::VARCHAR              AS review_comment_title,
        raw_data:review_comment_message::VARCHAR            AS review_comment_message,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:review_creation_date::VARCHAR
        )                                                   AS review_creation_date,
        TRY_TO_TIMESTAMP_NTZ(
            raw_data:review_answer_timestamp::VARCHAR
        )                                                   AS review_answer_timestamp,
        ingestion_timestamp,
        source_file,
        checksum
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY ingestion_timestamp DESC
        ) AS _row_num
    FROM parsed
)

SELECT
    review_id,
    order_id,
    review_score,

    -- Sentiment category based on score
    CASE
        WHEN review_score >= 4 THEN 'POSITIVE'
        WHEN review_score = 3 THEN 'NEUTRAL'
        WHEN review_score <= 2 THEN 'NEGATIVE'
    END                                                     AS sentiment_category,

    -- Has comment flag
    CASE
        WHEN review_comment_message IS NOT NULL
            AND TRIM(review_comment_message) != ''
            THEN TRUE
        ELSE FALSE
    END                                                     AS has_comment,

    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,

    -- Response time in hours
    DATEDIFF('hour',
        review_creation_date,
        review_answer_timestamp
    )                                                       AS response_time_hours,

    -- Metadata
    ingestion_timestamp,
    source_file,
    checksum,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at

FROM deduplicated
WHERE _row_num = 1
  AND review_id IS NOT NULL
