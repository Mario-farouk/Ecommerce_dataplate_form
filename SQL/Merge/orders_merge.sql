MERGE `ready-de26.project_landing.orders_mario` T
USING `ready-de26.project_stage.orders_stage_mario` S
ON T.order_id = S.order_id
WHEN MATCHED THEN
  UPDATE SET
    customer_id = S.customer_id,
    order_status = S.order_status,
    order_purchase_timestamp = S.order_purchase_timestamp,
    order_approved_at = S.order_approved_at,
    order_delivered_carrier_date = S.order_delivered_carrier_date,
    order_delivered_customer_date = S.order_delivered_customer_date,
    order_estimated_delivery_date = S.order_estimated_delivery_date,
    updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT ROW;
