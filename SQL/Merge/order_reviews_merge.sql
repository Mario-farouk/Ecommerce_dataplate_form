MERGE `ready-de26.project_landing.order_reviews_mario` T
USING `ready-de26.project_stage.order_reviews_stage_mario` S
ON T.review_id = S.review_id
WHEN MATCHED THEN
  UPDATE SET
    order_id = S.order_id,
    review_score = S.review_score,
    review_comment_title = S.review_comment_title,
    review_comment_message = S.review_comment_message,
    review_creation_date = S.review_creation_date,
    review_answer_timestamp = S.review_answer_timestamp,
    updated_at_timestamp = S.updated_at_timestamp
WHEN NOT MATCHED THEN
  INSERT ROW;
