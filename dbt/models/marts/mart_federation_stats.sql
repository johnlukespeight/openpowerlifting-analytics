-- Mart: federation-level stats per year.
-- Shows average Dots score, athlete count, and meet count — good for bar charts.
{{
    config(
        materialized='table',
        cluster_by=["federation"]
    )
}}

SELECT
    federation,
    year,
    COUNT(DISTINCT athlete_name)  AS athlete_count,
    COUNT(DISTINCT meet_name)     AS meet_count,
    ROUND(AVG(dots_score), 2)     AS avg_dots_score,
    ROUND(AVG(total_kg), 2)       AS avg_total_kg
FROM {{ ref('stg_powerlifting') }}
WHERE dots_score IS NOT NULL
GROUP BY
    federation,
    year
