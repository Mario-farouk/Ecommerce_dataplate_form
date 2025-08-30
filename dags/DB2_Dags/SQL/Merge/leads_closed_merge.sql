MERGE `ready-de26.project_landing.leads_closed_mario` T
USING (
  SELECT 
    lead_id,
    ANY_VALUE(customer_id) as customer_id,
    ANY_VALUE(lead_status) as lead_status,
    ANY_VALUE(closed_reason) as closed_reason,
    ANY_VALUE(CAST(closed_date AS TIMESTAMP)) as closed_date,
    ANY_VALUE(sales_rep_id) as sales_rep_id,
    ANY_VALUE(CAST(updated_at_timestamp AS TIMESTAMP)) as updated_at_timestamp
  FROM `ready-de26.project_landing.leads_closed_stage_mario`
  GROUP BY lead_id
) S
ON T.lead_id = S.lead_id
WHEN MATCHED THEN
  UPDATE SET
    T.customer_id = S.customer_id,
    T.lead_status = S.lead_status,
    T.closed_reason = S.closed_reason,
    T.closed_date = S.closed_date,
    T.sales_rep_id = S.sales_rep_id,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    lead_id, customer_id, lead_status, closed_reason,
    closed_date, sales_rep_id, updated_at_timestamp
  )
  VALUES (
    S.lead_id, S.customer_id, S.lead_status, S.closed_reason,
    S.closed_date, S.sales_rep_id, S.updated_at_timestamp
  );