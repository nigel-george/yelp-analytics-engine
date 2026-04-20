{{ config(materialized='table') }}

SELECT 
    u.is_elite_user,
    COUNT(r.review_id) as total_reviews,
    AVG(r.stars) as avg_given_rating,
    AVG(LENGTH(r.review_text)) as avg_review_length
FROM {{ ref('stg_review') }} r
JOIN {{ ref('stg_user') }} u ON r.user_id = u.user_id
GROUP BY 1