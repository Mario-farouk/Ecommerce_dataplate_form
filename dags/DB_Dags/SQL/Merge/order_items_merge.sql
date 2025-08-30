MERGE `ready-de26.project_landing.order_items_mario` T
USING `ready-de26.project_landing.order_items_stage_mario` S
ON T.order_item_id = S.order_item_id 
AND DATE(T.updated_at_timestamp) = DATE(S.updated_at_timestamp)  -- ← PARTITIONING KEY
WHEN MATCHED THEN
  UPDATE SET
    T.order_id = S.order_id,
    T.product_id = S.product_id,
    T.seller_id = S.seller_id,
    T.shipping_limit_date = CAST(S.shipping_limit_date AS TIMESTAMP),
    T.price = CAST(S.price AS NUMERIC),
    T.freight_value = CAST(S.freight_value AS NUMERIC),
    T.updated_at_timestamp = CAST(S.updated_at_timestamp AS TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT (
    order_item_id, order_id, product_id, seller_id, 
    shipping_limit_date, price, freight_value, updated_at_timestamp
  )
  VALUES (
    S.order_item_id, S.order_id, S.product_id, S.seller_id, 
    CAST(S.shipping_limit_date AS TIMESTAMP),
    CAST(S.price AS NUMERIC),
    CAST(S.freight_value AS NUMERIC),
    CAST(S.updated_at_timestamp AS TIMESTAMP)
  );