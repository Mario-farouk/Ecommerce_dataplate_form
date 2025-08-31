MERGE `ready-de26.project_landing.leads_qualified_mario` T
USING (
  SELECT 
    mql_id as lead_id,
    ANY_VALUE(customer_id) as customer_id,
    ANY_VALUE(lead_status) as lead_status,
    ANY_VALUE(CAST(qualified_date AS TIMESTAMP)) as qualified_date,
    ANY_VALUE(product_interest) as product_interest,
    ANY_VALUE(CAST(estimated_value AS NUMERIC)) as estimated_value,
    ANY_VALUE(assigned_rep_id) as assigned_rep_id,
    ANY_VALUE(CAST(updated_at_timestamp AS TIMESTAMP)) as updated_at_timestamp
  FROM `ready-de26.project_landing.leads_qualified_stage_mario`
  GROUP BY lead_id
) S
ON T.lead_id = S.lead_id
WHEN MATCHED THEN
  UPDATE SET
    T.customer_id = S.customer_id,
    T.lead_status = S.lead_status,
    T.qualified_date = S.qualified_date,
    T.product_interest = S.product_interest,
    T.estimated_value = S.estimated_value,
    T.assigned_rep_id = S.assigned_rep_id,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    lead_id, customer_id, lead_status, qualified_date,
    product_interest, estimated_value, assigned_rep_id, updated_at_timestamp
  )
  VALUES (
    S.lead_id, S.customer_id, S.lead_status, S.qualified_date,
    S.product_interest, S.estimated_value, S.assigned_rep_id, S.updated_at_timestamp
  );