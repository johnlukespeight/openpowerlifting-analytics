-- Mart: best total per athlete per weight class and equipment type.
-- Useful for all-time leaderboards.
{{
    config(
        materialized='table',
        partition_by={
            "field": "latest_competition",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["federation", "sex", "equipment"]
    )
}}

SELECT
    athlete_name,
    sex,
    weight_class_kg,
    equipment,
    federation,
    country,
    MAX(total_kg)    AS best_total_kg,
    MAX(dots_score)  AS best_dots,
    MAX(wilks_score) AS best_wilks,
    MAX(lift_date)   AS latest_competition,
    COUNT(*)         AS total_competitions
FROM {{ ref('stg_powerlifting') }}
GROUP BY
    athlete_name,
    sex,
    weight_class_kg,
    equipment,
    federation,
    country
