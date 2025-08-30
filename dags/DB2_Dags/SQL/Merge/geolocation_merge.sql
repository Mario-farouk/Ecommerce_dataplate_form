MERGE `ready-de26.project_landing.geolocation_mario` T
USING (
  SELECT 
    geolocation_zip_code_prefix,
    ANY_VALUE(CAST(geolocation_lat AS NUMERIC)) as geolocation_lat,
    ANY_VALUE(CAST(geolocation_lng AS NUMERIC)) as geolocation_lng,
    ANY_VALUE(geolocation_city) as geolocation_city,
    ANY_VALUE(geolocation_state) as geolocation_state,
    ANY_VALUE(CAST(updated_at_timestamp AS TIMESTAMP)) as updated_at_timestamp
  FROM `ready-de26.project_landing.geolocation_stage_mario`
  GROUP BY geolocation_zip_code_prefix
) S
ON T.geolocation_zip_code_prefix = S.geolocation_zip_code_prefix
WHEN MATCHED THEN
  UPDATE SET
    T.geolocation_lat = S.geolocation_lat,
    T.geolocation_lng = S.geolocation_lng,
    T.geolocation_city = S.geolocation_city,
    T.geolocation_state = S.geolocation_state,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
    geolocation_city, geolocation_state, updated_at_timestamp
  )
  VALUES (
    S.geolocation_zip_code_prefix, S.geolocation_lat, S.geolocation_lng,
    S.geolocation_city, S.geolocation_state, S.updated_at_timestamp
  );