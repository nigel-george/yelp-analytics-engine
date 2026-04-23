SELECT 
    review_id,
    business_id,
    user_id,
    stars,
    date as review_date,
    text
FROM {{ source('raw_data', 'fact_review') }}