SELECT column_name
FROM raw.information_schema.columns
WHERE table_schema = 'STRIPE'
  AND table_name = 'PAYMENT'
