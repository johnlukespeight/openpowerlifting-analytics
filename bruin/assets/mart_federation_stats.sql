/* @bruin

name: powerlifting.quality_mart_federation_stats
type: bq.sql
connection: gcp-default

description: "Quality checks on the per-federation annual statistics mart."

materialization:
  type: view

depends:
  - powerlifting.quality_raw_lifts

columns:
  - name: federation
    description: "Powerlifting federation"
    checks:
      - name: not_null
  - name: year
    description: "Year of competition"
    checks:
      - name: not_null
      - name: positive
  - name: avg_dots_score
    description: "Average Dots score for the federation that year"
    checks:
      - name: positive
  - name: athlete_count
    description: "Number of unique athletes that competed"
    checks:
      - name: not_null
      - name: positive

@bruin */

SELECT
    federation,
    year,
    avg_dots_score,
    avg_total_kg,
    athlete_count,
    meet_count
FROM `powerlifting.mart_federation_stats`
