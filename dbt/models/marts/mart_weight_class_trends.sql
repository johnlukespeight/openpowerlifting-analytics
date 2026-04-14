-- Mart: average best total per weight class per year.
-- Good for time-series line charts in Looker Studio.
{{
    config(
        materialized='table',
        cluster_by=["weight_class_kg", "sex"]
    )
}}

SELECT
    weight_class_kg,
    sex,
    equipment,
    year,
    COUNT(*)                   AS lift_count,
    ROUND(AVG(total_kg), 2)    AS avg_total_kg,
    ROUND(MAX(total_kg), 2)    AS max_total_kg,
    ROUND(AVG(dots_score), 2)  AS avg_dots_score
FROM {{ ref('stg_powerlifting') }}
WHERE weight_class_kg IS NOT NULL
  AND year IS NOT NULL
GROUP BY
    weight_class_kg,
    sex,
    equipment,
    year
