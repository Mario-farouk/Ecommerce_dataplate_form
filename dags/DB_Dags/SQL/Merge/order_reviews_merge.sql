MERGE `ready-de26.project_landing.order_reviews_mario` T
USING (
  SELECT 
    review_id,
    ANY_VALUE(order_id) as order_id,
    ANY_VALUE(review_score) as review_score,
    ANY_VALUE(review_comment_title) as review_comment_title,
    ANY_VALUE(review_comment_message) as review_comment_message,
    ANY_VALUE(review_creation_date) as review_creation_date,
    ANY_VALUE(review_answer_timestamp) as review_answer_timestamp,
    ANY_VALUE(updated_at_timestamp) as updated_at_timestamp
  FROM `ready-de26.project_landing.order_reviews_stage_mario`
  GROUP BY review_id
) S
ON T.review_id = S.review_id
WHEN MATCHED THEN
  UPDATE SET
    T.order_id = S.order_id,
    T.review_score = CAST(S.review_score AS INT64),
    T.review_comment_title = S.review_comment_title,
    T.review_comment_message = S.review_comment_message,
    T.review_creation_date = CAST(S.review_creation_date AS TIMESTAMP),
    T.review_answer_timestamp = CAST(S.review_answer_timestamp AS TIMESTAMP),
    T.updated_at_timestamp = CAST(S.updated_at_timestamp AS TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT (
    review_id, order_id, review_score, review_comment_title, 
    review_comment_message, review_creation_date, review_answer_timestamp, 
    updated_at_timestamp
  )
  VALUES (
    S.review_id, S.order_id, CAST(S.review_score AS INT64), S.review_comment_title, 
    S.review_comment_message, CAST(S.review_creation_date AS TIMESTAMP), 
    CAST(S.review_answer_timestamp AS TIMESTAMP), CAST(S.updated_at_timestamp AS TIMESTAMP)
  );