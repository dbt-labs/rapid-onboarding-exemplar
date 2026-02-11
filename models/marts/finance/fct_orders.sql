WITH int_order_items_joined AS (
  SELECT
    *
  FROM {{ ref('int_order_items_joined') }}
), formula_8632 AS (
  SELECT
    *,
    1 AS ORDER_COUNT
  FROM int_order_items_joined
), aggregation_38a3 AS (
  SELECT
    SUM(GROSS_ITEM_SALES_AMOUNT) AS GROSS_ITEM_SALES_AMOUNT,
    SUM(ITEM_DISCOUNT_AMOUNT) AS ITEM_DISCOUNT_AMOUNT,
    SUM(ITEM_TAX_AMOUNT) AS ITEM_TAX_AMOUNT,
    SUM(NET_ITEM_SALES_AMOUNT) AS NET_ITEM_SALES_AMOUNT
  FROM formula_8632
), projection_b38c AS (
  SELECT
    ORDER_ID,
    ORDER_DATE,
    CUSTOMER_ID,
    ORDER_STATUS_CODE,
    PRIORITY_CODE,
    CLERK_NAME,
    SHIP_PRIORITY,
    ORDER_COUNT,
    GROSS_ITEM_SALES_AMOUNT,
    ITEM_DISCOUNT_AMOUNT,
    ITEM_TAX_AMOUNT,
    NET_ITEM_SALES_AMOUNT
  FROM aggregation_38a3
), order_f4b4 AS (
  SELECT
    *
  FROM projection_b38c
  ORDER BY
    ORDER_DATE ASC
), fct_orders AS (
  SELECT
    *
  FROM order_f4b4
)
SELECT
  *
FROM fct_orders