-- Intermediate model: full powerlifting events only (SBD = Squat + Bench + Deadlift).
-- Adds a total_category bucket to group totals for analysis.
{{ config(materialized='view') }}

SELECT
    *,
    -- Bucket total_kg into readable performance tiers
    CASE
        WHEN total_kg < 300  THEN 'Under 300kg'
        WHEN total_kg < 400  THEN '300-400kg'
        WHEN total_kg < 500  THEN '400-500kg'
        WHEN total_kg < 600  THEN '500-600kg'
        WHEN total_kg < 700  THEN '600-700kg'
        WHEN total_kg < 800  THEN '700-800kg'
        WHEN total_kg < 900  THEN '800-900kg'
        ELSE '900kg+'
    END AS total_category
FROM {{ ref('stg_powerlifting') }}
-- SBD = full powerlifting (squat + bench + deadlift)
WHERE event = 'SBD'
