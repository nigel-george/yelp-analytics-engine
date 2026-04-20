SELECT 
    business_id,
    UPPER(name) as business_name,
    COALESCE(address, 'No Address Provided') as address,
    INITCAP(city) as city,
    UPPER(state) as state,
    stars,
    review_count,
    categories
FROM {{ source('raw_data', 'dim_business') }}