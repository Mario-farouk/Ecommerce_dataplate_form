MERGE `ready-de26.project_landing.order_items_mario` T
USING `ready-de26.project_stage.order_items_stage_mario` S
ON T.order_item_id = S.order_item_id
WHEN MATCHED THEN
  UPDATE SET
    order_id = S.order_id,
    product_id = S.product_id,
    seller_id = S.seller_id,
    shipping_limit_date = S.shipping_limit_date,
    price = S.price,
    freight_value = S.freight_value,
    updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT ROW;
