WITH dim_customers AS (
  /* Customer dimensions table */
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'dim_customers') }}
), fct_orders AS (
  /* orders fact table */
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'fct_orders') }}
), "join" AS (
  SELECT
    *
  FROM fct_orders
  LEFT JOIN dim_customers
    USING (CUSTOMER_ID)
), count_the_customers AS (
  SELECT
    CUSTOMER_ID,
    SUM(ORDER_COUNT) AS sum_ORDER_COUNT,
    SUM(NET_ITEM_SALES_AMOUNT) AS sum_NET_ITEM_SALES_AMOUNT
  FROM "join"
  GROUP BY
    CUSTOMER_ID
), custmers_count_sql AS (
  SELECT
    *
  FROM count_the_customers
)
SELECT
  *
FROM custmers_count_sql