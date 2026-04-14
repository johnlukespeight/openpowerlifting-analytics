-- Staging model: 1-to-1 view of raw_lifts with renamed columns and basic filters.
-- This is the entry point for all downstream models.
{{ config(materialized='view') }}

SELECT
    Name              AS athlete_name,
    Sex               AS sex,
    Event             AS event,
    Equipment         AS equipment,
    Age               AS age,
    AgeClass          AS age_class,
    BodyweightKg      AS bodyweight_kg,
    WeightClassKg     AS weight_class_kg,
    Best3SquatKg      AS best_squat_kg,
    Best3BenchKg      AS best_bench_kg,
    Best3DeadliftKg   AS best_deadlift_kg,
    TotalKg           AS total_kg,
    Place             AS place,
    Dots              AS dots_score,
    Wilks             AS wilks_score,
    Tested            AS is_tested,
    Country           AS country,
    Federation        AS federation,
    lift_date,
    EXTRACT(YEAR FROM lift_date) AS year,
    MeetName          AS meet_name
FROM {{ source('powerlifting', 'raw_lifts') }}
WHERE TotalKg IS NOT NULL
  AND Place NOT IN ('DQ', 'NS', 'DD')
