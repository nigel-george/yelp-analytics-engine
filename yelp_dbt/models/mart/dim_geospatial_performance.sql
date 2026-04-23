{{ config(materialized='table') }}

WITH postal_stats AS (
    SELECT 
        postal_code,
        city,
        state,
        COUNT(business_id) as total_businesses,
        AVG(stars) as avg_rating,
        SUM(review_count) as total_reviews
    FROM {{ source('raw_data', 'dim_business') }}
    WHERE postal_code IS NOT NULL AND postal_code != ''
    GROUP BY 1, 2, 3
),

city_benchmarks AS (
    SELECT 
        *,
        AVG(avg_rating) OVER(PARTITION BY city) as city_avg_rating,
        STDDEV(avg_rating) OVER(PARTITION BY city) as city_stddev_rating
    FROM postal_stats
)

SELECT 
    postal_code,
    city,
    state,
    total_businesses,
    avg_rating,
    total_reviews,
    city_avg_rating,
    -- Z-Score: Highlights how many standard deviations a postal code is from the city mean.
    CASE 
        WHEN city_stddev_rating = 0 OR city_stddev_rating IS NULL THEN 0 
        ELSE (avg_rating - city_avg_rating) / city_stddev_rating 
    END as rating_z_score
FROM city_benchmarks