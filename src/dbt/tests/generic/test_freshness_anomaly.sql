/*
  test_freshness_anomaly.sql
  ===========================
  Generic dbt test: Alerts if data arrives outside expected business hours
  or if the most recent data is stale beyond a threshold.
  
  Checks:
    1. Most recent timestamp is within `max_hours_stale` hours of now
    2. Data arriving outside business hours (6 AM - 11 PM BRT) is flagged
  
  Usage in schema.yml:
    tests:
      - freshness_anomaly:
          timestamp_column: ingestion_timestamp
          max_hours_stale: 48
          business_hours_start: 6
          business_hours_end: 23
*/

{% test freshness_anomaly(model, timestamp_column, max_hours_stale=48, business_hours_start=6, business_hours_end=23) %}

WITH freshness AS (
    SELECT
        MAX({{ timestamp_column }}) AS latest_record,
        TIMESTAMPDIFF(
            'hour', 
            MAX({{ timestamp_column }}), 
            CURRENT_TIMESTAMP()
        ) AS hours_since_latest,
        COUNT(*) AS total_records
    FROM {{ model }}
    WHERE {{ timestamp_column }} IS NOT NULL
),

off_hours_arrivals AS (
    SELECT COUNT(*) AS off_hours_count
    FROM {{ model }}
    WHERE {{ timestamp_column }} IS NOT NULL
      AND (
          HOUR({{ timestamp_column }}) < {{ business_hours_start }}
          OR HOUR({{ timestamp_column }}) > {{ business_hours_end }}
      )
      -- Only check recent data (last 7 days)
      AND {{ timestamp_column }} >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)

SELECT
    f.latest_record,
    f.hours_since_latest,
    f.total_records,
    o.off_hours_count,
    {{ max_hours_stale }} AS max_allowed_staleness_hours
FROM freshness f
CROSS JOIN off_hours_arrivals o
WHERE
    -- FAIL if data is stale beyond threshold
    f.hours_since_latest > {{ max_hours_stale }}
    -- OR if more than 10% of recent data arrived outside business hours
    OR (o.off_hours_count > 0 AND o.off_hours_count * 10 > f.total_records)

{% endtest %}
