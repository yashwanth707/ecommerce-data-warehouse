-- Generic Test: Amount Distribution (Outlier Detection)
-- Flags rows where a numeric value is more than N standard
-- deviations from the mean (statistical outlier test).
--
-- Usage in schema.yml:
--   tests:
--     - amount_distribution:
--         column_name: payment_value
--         sigma_threshold: 3

{% test amount_distribution(model, column_name, sigma_threshold=3) %}

WITH stats AS (
    SELECT
        AVG({{ column_name }})                              AS mean_val,
        STDDEV({{ column_name }})                           AS stddev_val
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
)

SELECT
    m.*
FROM {{ model }} m
CROSS JOIN stats s
WHERE m.{{ column_name }} IS NOT NULL
  AND s.stddev_val > 0
  AND ABS(m.{{ column_name }} - s.mean_val) > ({{ sigma_threshold }} * s.stddev_val)

{% endtest %}
