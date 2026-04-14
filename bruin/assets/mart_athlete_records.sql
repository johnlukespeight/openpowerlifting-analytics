/* @bruin

name: powerlifting.quality_mart_athlete_records
type: bq.sql
connection: gcp-default

description: "Quality checks on the athlete personal records mart."

materialization:
  type: view

depends:
  - powerlifting.quality_raw_lifts

columns:
  - name: athlete_name
    description: "Competitor's full name"
    checks:
      - name: not_null
  - name: best_total_kg
    description: "Athlete's best competition total"
    checks:
      - name: not_null
      - name: positive
  - name: best_dots
    description: "Best Dots score achieved"
    checks:
      - name: positive
  - name: equipment
    description: "Equipment category"
    checks:
      - name: not_null
  - name: weight_class_kg
    description: "Bodyweight class (nullable — not all OPL records include a weight class)"

@bruin */

SELECT
    athlete_name,
    best_total_kg,
    best_dots,
    equipment,
    weight_class_kg,
    sex,
    federation,
    latest_competition
FROM `powerlifting.mart_athlete_records`
