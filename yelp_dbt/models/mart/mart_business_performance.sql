{{ config(materialized='table') }}

WITH business_stats AS (
    SELECT 
        b.business_id,
        b.business_name,
        b.city,
        b.stars AS total_avg_stars,
        COUNT(r.review_id) AS total_reviews,
        ROUND(
            COUNT(CASE WHEN u.is_elite_user THEN 1 END)::DECIMAL / 
            NULLIF(COUNT(r.review_id), 0), 2
        ) AS elite_review_ratio
    FROM {{ ref('stg_business') }} b
    LEFT JOIN {{ ref('stg_review') }} r ON b.business_id = r.business_id
    LEFT JOIN {{ ref('stg_user') }} u ON r.user_id = u.user_id
    GROUP BY 1, 2, 3, 4
),

rating_trends AS (
    -- Dynamically comparing 2021 vs 2022
    SELECT 
        business_id,
        AVG(CASE WHEN EXTRACT(YEAR FROM review_date) = 2021 THEN stars END) AS avg_stars_2021,
        AVG(CASE WHEN EXTRACT(YEAR FROM review_date) = 2022 THEN stars END) AS avg_stars_2022
    FROM {{ ref('stg_review') }}
    GROUP BY 1
)

SELECT 
    bs.*,
    ROUND(rt.avg_stars_2021::numeric, 2) AS rating_prev_year,
    ROUND(rt.avg_stars_2022::numeric, 2) AS rating_latest_year,
    CASE 
        WHEN rt.avg_stars_2022 > rt.avg_stars_2021 THEN 'Improving'
        WHEN rt.avg_stars_2022 < rt.avg_stars_2021 THEN 'Declining'
        ELSE 'Stable/No Trend'
    END AS business_direction
FROM business_stats bs
LEFT JOIN rating_trends rt ON bs.business_id = rt.business_id