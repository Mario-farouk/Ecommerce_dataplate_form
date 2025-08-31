MERGE `ready-de26.project_landing.leads_qualified_mario` T
USING (
  SELECT 
    mql_id,
    ANY_VALUE(first_contact_date) as first_contact_date,
    ANY_VALUE(landing_page_id) as landing_page_id,
    ANY_VALUE(origin) as origin,
    ANY_VALUE(CAST(updated_at_timestamp AS TIMESTAMP)) as updated_at_timestamp
  FROM `ready-de26.project_landing.leads_qualified_stage_mario`
  GROUP BY mql_id
) S
ON T.mql_id = S.mql_id
WHEN MATCHED THEN
  UPDATE SET
    T.first_contact_date = S.first_contact_date,
    T.landing_page_id = S.landing_page_id,
    T.origin = S.origin,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    mql_id, first_contact_date, landing_page_id, 
    origin, updated_at_timestamp
  )
  VALUES (
    S.mql_id, S.first_contact_date, S.landing_page_id, 
    S.origin, S.updated_at_timestamp
  );