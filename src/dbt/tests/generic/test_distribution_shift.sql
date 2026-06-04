/*
  test_distribution_shift.sql
  ============================
  Generic dbt test: Detects data drift by comparing the current distribution
  of a numeric column against its historical baseline using statistical thresholds.
  
  This is a simplified Kolmogorov-Smirnov-inspired approach that flags when:
    - The mean shifts by more than 2 standard deviations from the historical mean
    - The current standard deviation is more than 3x the historical standard deviation
  
  Usage in schema.yml:
    tests:
      - distribution_shift:
          column_name: total_value
          expected_mean: 120.65
          expected_stddev: 153.42
          sensitivity: 2.0
*/

{% test distribution_shift(model, column_name, expected_mean, expected_stddev, sensitivity=2.0) %}

WITH current_stats AS (
    SELECT
        AVG({{ column_name }}) AS current_mean,
        STDDEV({{ column_name }}) AS current_stddev,
        COUNT(*) AS row_count
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
),

drift_check AS (
    SELECT
        current_mean,
        current_stddev,
        row_count,
        {{ expected_mean }} AS expected_mean,
        {{ expected_stddev }} AS expected_stddev,
        -- Mean shift: how many historical std devs away is the current mean?
        ABS(current_mean - {{ expected_mean }}) / NULLIF({{ expected_stddev }}, 0) AS mean_z_score,
        -- Variance ratio: is the spread dramatically different?
        current_stddev / NULLIF({{ expected_stddev }}, 0) AS stddev_ratio
    FROM current_stats
)

SELECT *
FROM drift_check
WHERE 
    -- Flag if mean shifted by more than `sensitivity` standard deviations
    mean_z_score > {{ sensitivity }}
    -- Or if standard deviation is 3x larger than expected (explosion in variance)
    OR stddev_ratio > 3.0
    -- Or if there are suspiciously few rows (possible data loss)
    OR row_count < 100

{% endtest %}
