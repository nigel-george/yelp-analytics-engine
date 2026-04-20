SELECT 
    review_id,
    business_id,
    user_id,
    stars::int as stars,
    TRIM(text) as review_text,
    date as review_date,
    -- Flagging reviews under 20 chars
    CASE 
        WHEN LENGTH(text) < 20 THEN TRUE 
        ELSE FALSE 
    END AS is_low_quality
FROM {{ source('raw_data', 'fact_review') }}