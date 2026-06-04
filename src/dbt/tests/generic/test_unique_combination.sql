/*
  test_unique_combination.sql
  ===========================
  Generic dbt test: Asserts that a combination of columns is unique.
  Usage in schema.yml:
    tests:
      - unique_combination:
          combination_of_columns: ['order_id', 'product_id']
*/

{% test unique_combination(model, combination_of_columns) %}

WITH validation AS (
    SELECT
        {{ combination_of_columns | join(', ') }},
        COUNT(*) AS row_count
    FROM {{ model }}
    GROUP BY {{ combination_of_columns | join(', ') }}
    HAVING COUNT(*) > 1
)

SELECT * FROM validation

{% endtest %}
