-- Dimension: Time (Date Spine 2016–2025)
-- Generated via dbt_utils.date_spine
-- Includes Brazilian holidays, fiscal periods, day attributes

{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'business_ready']
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}
),

holidays AS (
    {% set holiday_list = var('brazil_holidays') %}
    SELECT date_day
    FROM (
        {% for h in holiday_list %}
        SELECT '{{ h }}'::DATE AS date_day
        {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
    )
)

SELECT
    d.date_day                                              AS date_key,

    -- Day attributes
    DAY(d.date_day)                                         AS day_of_month,
    DAYOFWEEK(d.date_day)                                   AS day_of_week,
    DAYNAME(d.date_day)                                     AS day_name,
    DAYOFYEAR(d.date_day)                                   AS day_of_year,

    -- Week
    WEEKOFYEAR(d.date_day)                                  AS week_of_year,
    DATE_TRUNC('week', d.date_day)                          AS week_start_date,

    -- Month
    MONTH(d.date_day)                                       AS month_number,
    MONTHNAME(d.date_day)                                   AS month_name,
    DATE_TRUNC('month', d.date_day)                         AS month_start_date,
    LAST_DAY(d.date_day, 'month')                           AS month_end_date,

    -- Quarter
    QUARTER(d.date_day)                                     AS quarter_number,
    'Q' || QUARTER(d.date_day)                              AS quarter_name,
    DATE_TRUNC('quarter', d.date_day)                       AS quarter_start_date,

    -- Year
    YEAR(d.date_day)                                        AS year_number,

    -- Fiscal period (assuming fiscal year = calendar year)
    'FY' || YEAR(d.date_day) || '-Q' || QUARTER(d.date_day) AS fiscal_period,

    -- Flags
    CASE
        WHEN DAYOFWEEK(d.date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                                     AS is_weekend,

    CASE
        WHEN h.date_day IS NOT NULL THEN TRUE
        ELSE FALSE
    END                                                     AS is_holiday,

    -- Relative period flags
    CASE
        WHEN d.date_day = CURRENT_DATE() THEN TRUE
        ELSE FALSE
    END                                                     AS is_today,

    CASE
        WHEN d.date_day >= DATE_TRUNC('month', CURRENT_DATE())
            AND d.date_day <= CURRENT_DATE()
            THEN TRUE
        ELSE FALSE
    END                                                     AS is_current_mtd

FROM date_spine d
LEFT JOIN holidays h ON d.date_day = h.date_day
