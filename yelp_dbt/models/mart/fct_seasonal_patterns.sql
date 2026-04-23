{{ config(materialized='table') }}

SELECT 
    date_trunc('month', r.date) as review_month,
    b.categories as category,
    b.city,
    COUNT(r.review_id) as monthly_review_count,
    AVG(r.stars) as monthly_avg_rating
FROM {{ source('raw_data', 'fact_review') }} r
JOIN {{ source('raw_data', 'dim_business') }} b ON r.business_id = b.business_id
GROUP BY 1, 2, 3