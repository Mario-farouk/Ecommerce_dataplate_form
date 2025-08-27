MERGE `ready-de26.project_landing.product_category_name_translation_mario` T
USING `ready-de26.project_stage.product_category_name_translation_stage_mario` S
ON T.product_category_name = S.product_category_name
WHEN MATCHED THEN
  UPDATE SET
    product_category_name_english = S.product_category_name_english,
    updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT ROW;
