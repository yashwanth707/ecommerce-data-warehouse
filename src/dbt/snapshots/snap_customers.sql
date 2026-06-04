-- Snapshot: Customer Address Changes (SCD Type 2)
-- Tracks changes to customer location over time.
-- Uses timestamp strategy based on ingestion_timestamp.

{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='ingestion_timestamp',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    is_invalid_zip,
    ingestion_timestamp
FROM {{ ref('silver_customers') }}

{% endsnapshot %}
