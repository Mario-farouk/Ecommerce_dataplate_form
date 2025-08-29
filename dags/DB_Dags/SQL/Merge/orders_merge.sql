MERGE `ready-de26.project_landing.orders_mario` T
USING `ready-de26.project_landing.orders_stage_mario` S
ON T.order_id = S.order_id
WHEN MATCHED THEN
  UPDATE SET
    T.customer_id = S.customer_id,
    T.order_status = S.order_status,
    T.order_purchase_timestamp = CAST(S.order_purchase_timestamp AS TIMESTAMP),
    T.order_approved_at = CAST(S.order_approved_at AS TIMESTAMP),
    T.order_delivered_carrier_date = CAST(S.order_delivered_carrier_date AS TIMESTAMP),
    T.order_delivered_customer_date = CAST(S.order_delivered_customer_date AS TIMESTAMP),
    T.order_estimated_delivery_date = CAST(S.order_estimated_delivery_date AS TIMESTAMP),
    T.updated_at_timestamp = CAST(S.updated_at_timestamp AS TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT (
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date, updated_at_timestamp
  )
  VALUES (
    S.order_id, S.customer_id, S.order_status, 
    CAST(S.order_purchase_timestamp AS TIMESTAMP),
    CAST(S.order_approved_at AS TIMESTAMP),
    CAST(S.order_delivered_carrier_date AS TIMESTAMP),
    CAST(S.order_delivered_customer_date AS TIMESTAMP),
    CAST(S.order_estimated_delivery_date AS TIMESTAMP),
    CAST(S.updated_at_timestamp AS TIMESTAMP)
  );