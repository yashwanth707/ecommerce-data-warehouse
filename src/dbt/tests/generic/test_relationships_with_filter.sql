-- Generic Test: Relationships with Filter
-- Tests referential integrity but only for records matching
-- a status filter (e.g., only DELIVERED orders).
--
-- Usage in schema.yml:
--   tests:
--     - relationships_with_filter:
--         to: ref('silver_orders')
--         field: order_id
--         from_condition: "order_status = 'DELIVERED'"
--         to_condition: "order_status = 'DELIVERED'"

{% test relationships_with_filter(model, column_name, to, field, from_condition="1=1", to_condition="1=1") %}

SELECT
    {{ column_name }} AS failing_value
FROM {{ model }}
WHERE {{ from_condition }}
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }} NOT IN (
      SELECT {{ field }}
      FROM {{ to }}
      WHERE {{ to_condition }}
  )

{% endtest %}
