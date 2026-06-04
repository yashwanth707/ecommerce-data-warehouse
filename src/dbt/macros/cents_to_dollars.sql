-- Macro: Cents to Dollars (Currency Conversion)
-- Converts a value from centavos (BRL cents) to reais (BRL).
-- Usage: {{ cents_to_dollars('column_name') }}

{% macro cents_to_dollars(column_name, precision=2) %}
    ROUND({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}

-- Macro: BRL to USD (approximate)
-- Approximate conversion at a configurable exchange rate.
-- Usage: {{ brl_to_usd('column_name', rate=0.20) }}

{% macro brl_to_usd(column_name, rate=0.20, precision=2) %}
    ROUND({{ column_name }} * {{ rate }}, {{ precision }})
{% endmacro %}
