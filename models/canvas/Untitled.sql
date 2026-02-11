WITH dim_customers AS (
  /* This model consolidates customer information with their order history to provide insights into customer behavior and value. It includes personal details, order dates, and categorizes customers into tiers based on their total order value, which can be used for targeted marketing and customer relationship management. */
  SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME
  FROM {{ ref('dbt_developer_pathway', 'dim_customers', v=1) }}
), fct_orders AS (
  /* orders fact table */
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'fct_orders') }}
), join_1 AS (
  SELECT
    *
  FROM dim_customers
  JOIN fct_orders
    USING (CUSTOMER_ID)
), aggregate_1 AS (
  SELECT
    CUSTOMER_ID,
    SUM(ORDER_COUNT) AS sum_ORDER_COUNT
  FROM join_1
  GROUP BY
    CUSTOMER_ID
), untitled_sql AS (
  SELECT
    *
  FROM aggregate_1
)
SELECT
  *
FROM untitled_sql