{{ config(materialized='table') }}

WITH monthly_city_counts AS (
    
    SELECT 
        b.city,
        b.state,
        DATE_TRUNC('month', r.review_date) as review_month,
        COUNT(r.review_id) as monthly_reviews
    FROM {{ ref('stg_business') }} b
    JOIN {{ ref('stg_review') }} r ON b.business_id = r.business_id
    GROUP BY 1, 2, 3
),

growth_calculation AS (
    
    SELECT 
        city,
        state,
        review_month,
        monthly_reviews,
        LAG(monthly_reviews) OVER (PARTITION BY city ORDER BY review_month) as prev_month_reviews
    FROM monthly_city_counts
)


SELECT 
    city,
    state,
    review_month,
    monthly_reviews,
    COALESCE(prev_month_reviews, 0) as prev_month_reviews,
    ROUND(
        CASE 
            WHEN prev_month_reviews IS NULL OR prev_month_reviews = 0 THEN 0
            ELSE ((monthly_reviews - prev_month_reviews)::numeric / prev_month_reviews) * 100 
        END, 2
    ) as review_growth_rate_pct
FROM growth_calculation
ORDER BY review_month DESC, review_growth_rate_pct DESC