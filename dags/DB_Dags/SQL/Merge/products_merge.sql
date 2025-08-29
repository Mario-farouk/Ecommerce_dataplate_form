MERGE `ready-de26.project_landing.products_mario` T
USING `ready-de26.project_landing.products_stage_mario` S
ON T.product_id = S.product_id
WHEN MATCHED THEN
  UPDATE SET
    T.product_category_name = S.product_category_name,
    T.product_name_length = CAST(S.product_name_lenght AS INT64),  -- ← Note: "lenght" not "length"
    T.product_description_length = CAST(S.product_description_lenght AS INT64),  -- ← "lenght"
    T.product_photos_qty = CAST(S.product_photos_qty AS INT64),
    T.product_weight_g = CAST(S.product_weight_g AS INT64),
    T.product_length_cm = CAST(S.product_length_cm AS INT64),
    T.product_height_cm = CAST(S.product_height_cm AS INT64),
    T.product_width_cm = CAST(S.product_width_cm AS INT64),
    T.updated_at_timestamp = CAST(S.updated_at_timestamp AS TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT (
    product_id, product_category_name, product_name_length, 
    product_description_length, product_photos_qty, product_weight_g,
    product_length_cm, product_height_cm, product_width_cm, updated_at_timestamp
  )
  VALUES (
    S.product_id, S.product_category_name, CAST(S.product_name_lenght AS INT64),  -- ← "lenght"
    CAST(S.product_description_lenght AS INT64),  -- ← "lenght"
    CAST(S.product_photos_qty AS INT64), 
    CAST(S.product_weight_g AS INT64), CAST(S.product_length_cm AS INT64), 
    CAST(S.product_height_cm AS INT64), CAST(S.product_width_cm AS INT64), 
    CAST(S.updated_at_timestamp AS TIMESTAMP)
  );