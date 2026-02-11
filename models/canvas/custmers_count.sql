WITH dim_customers AS (
  /* Customer dimensions table */
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'dim_customers') }}
), count_the_customers AS (
  SELECT
    COUNT(DISTINCT CUSTOMER_ID) AS countd_CUSTOMER_ID
  FROM dim_customers
), custmers_count_sql AS (
  SELECT
    *
  FROM count_the_customers
)
SELECT
  *
FROM custmers_count_sql