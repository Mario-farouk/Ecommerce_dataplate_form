MERGE `ready-de26.project_landing.products_mario` T
USING `ready-de26.project_stage.products_stage_mario` S
ON T.product_id = S.product_id
WHEN MATCHED THEN
  UPDATE SET
    T.product_category_name = S.product_category_name,
    T.product_name_length = S.product_name_length,
    T.product_description_length = S.product_description_length,
    T.product_photos_qty = S.product_photos_qty,
    T.product_weight_g = S.product_weight_g,
    T.product_length_cm = S.product_length_cm,
    T.product_height_cm = S.product_height_cm,
    T.product_width_cm = S.product_width_cm,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    product_id, product_category_name, product_name_length, 
    product_description_length, product_photos_qty, product_weight_g,
    product_length_cm, product_height_cm, product_width_cm, updated_at_timestamp
  )
  VALUES (
    S.product_id, S.product_category_name, S.product_name_length, 
    S.product_description_length, S.product_photos_qty, S.product_weight_g,
    S.product_length_cm, S.product_height_cm, S.product_width_cm, S.updated_at_timestamp
  );