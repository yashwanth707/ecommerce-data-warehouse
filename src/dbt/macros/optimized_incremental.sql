-- Macro: Optimized Incremental Configuration
-- Generates incremental config with merge strategy and
-- partition pruning via timestamp clustering.

{% macro optimized_incremental(unique_key, cluster_key='ingestion_timestamp', strategy='merge') %}

    {{
        config(
            materialized='incremental',
            unique_key=unique_key,
            incremental_strategy=strategy,
            on_schema_change='append_new_columns',
            cluster_by=[cluster_key],
            transient=true
        )
    }}

{% endmacro %}


-- Incremental filter predicate
-- Reusable WHERE clause for incremental models.

{% macro incremental_filter(timestamp_column='ingestion_timestamp') %}
    {% if is_incremental() %}
    WHERE {{ timestamp_column }} > (
        SELECT COALESCE(MAX({{ timestamp_column }}), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
{% endmacro %}
