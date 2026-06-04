-- Macro: Generate Surrogate Key (Hash-based)
-- Wrapper around dbt_utils with consistent hashing.
-- Usage: {{ generate_sk('column_name') }}
--    or: {{ generate_sk(['col1', 'col2']) }}

{% macro generate_sk(field) %}
    {% if field is string %}
        {{ dbt_utils.generate_surrogate_key([field]) }}
    {% elif field is iterable %}
        {{ dbt_utils.generate_surrogate_key(field) }}
    {% endif %}
{% endmacro %}
