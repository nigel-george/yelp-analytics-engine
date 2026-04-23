{{ config(materialized='table') }}

SELECT 
    categories as category,
    is_open,
    COUNT(business_id) as business_count,
    AVG(stars) as avg_stars,
    AVG(review_count) as avg_review_volume
FROM {{ source('raw_data', 'dim_business') }}
GROUP BY 1, 2