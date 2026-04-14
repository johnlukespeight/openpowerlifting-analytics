/* @bruin

name: powerlifting.quality_raw_lifts
type: bq.sql
connection: gcp-default

description: "Quality checks on the raw ingested lifts table."

materialization:
  type: view

columns:
  - name: athlete_name
    description: "Competitor's full name"
    checks:
      - name: not_null
  - name: total_kg
    description: "Total weight lifted (squat + bench + deadlift)"
    checks:
      - name: not_null
      - name: positive
  - name: lift_date
    description: "Date of the competition"
    checks:
      - name: not_null
  - name: federation
    description: "Powerlifting federation that sanctioned the meet"
    checks:
      - name: not_null
  - name: equipment
    description: "Equipment category (Raw, Wraps, Single-ply, Multi-ply)"
    checks:
      - name: not_null
      - name: accepted_values
        value: ["Raw", "Wraps", "Single-ply", "Multi-ply", "Unlimited", "Straps"]

@bruin */

SELECT
    Name        AS athlete_name,
    TotalKg     AS total_kg,
    lift_date,
    Federation  AS federation,
    Equipment   AS equipment
FROM `powerlifting.raw_lifts`
WHERE TotalKg IS NOT NULL
  AND Place NOT IN ('DQ', 'NS', 'DD')
