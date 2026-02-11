WITH dim_customers AS (
  /* Customer dimensions table */
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'dim_customers') }}
), untitled_sql AS (
  SELECT
    *
  FROM dim_customers
)
SELECT
  *
FROM untitled_sql