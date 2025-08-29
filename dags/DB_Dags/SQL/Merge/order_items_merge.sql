MERGE `ready-de26.project_landing.order_items_mario` T
USING `ready-de26.project_landing.order_items_stage_mario` S
ON T.order_item_id = S.order_item_id
WHEN MATCHED THEN
  UPDATE SET
    T.order_id = S.order_id,
    T.product_id = S.product_id,
    T.seller_id = S.seller_id,
    T.shipping_limit_date = S.shipping_limit_date,
    T.price = S.price,
    T.freight_value = S.freight_value,
    T.updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    order_item_id, order_id, product_id, seller_id, 
    shipping_limit_date, price, freight_value, updated_at_timestamp
  )
  VALUES (
    S.order_item_id, S.order_id, S.product_id, S.seller_id, 
    S.shipping_limit_date, S.price, S.freight_value, S.updated_at_timestamp
  );