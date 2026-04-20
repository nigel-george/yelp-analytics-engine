SELECT 
    user_id,
    name as user_name,
    review_count,
    yelping_since,
    
    CURRENT_DATE - yelping_since::date as days_since_joined,
    
    CASE 
        WHEN fans > 10 OR review_count > 100 THEN TRUE 
        ELSE FALSE 
    END AS is_elite_user
FROM {{ source('raw_data', 'dim_user') }}