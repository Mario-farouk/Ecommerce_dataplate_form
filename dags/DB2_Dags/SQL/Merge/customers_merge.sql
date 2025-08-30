MERGE `ready-de26.project_landing.customers_mario` T
USING (
  SELECT 
    customer_id,
    ANY_VALUE(customer_unique_id) as customer_unique_id,
    ANY_VALUE(CAST(customer_zip_code_prefix AS INT64)) as customer_zip_code_prefix,
    ANY_VALUE(customer_city) as customer_city,
    ANY_VALUE(customer_state) as customer_state,
    ANY_VALUE(CAST(updated_at_timestamp AS TIMESTAMP)) as updated_at_timestamp
  FROM `ready-de26.project_landing.customers_stage_mario`
  GROUP BY customer_id
) S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN
  UPDATE SET
    T.customer_unique_id = S.customer_unique_id,
    T.customer_zip_code_prefix = S.customer_zip_code_prefix,
    T.customer_city = S.customer_city,
    T.customer_state = S.customer_state,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state, updated_at_timestamp
  )
  VALUES (
    S.customer_id, S.customer_unique_id, S.customer_zip_code_prefix,
    S.customer_city, S.customer_state, S.updated_at_timestamp
  );