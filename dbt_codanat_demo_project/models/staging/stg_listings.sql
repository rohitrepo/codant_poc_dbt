with source as (
    select * from {{ source('raw_airbnb_data', 'RAW_LISTINGS') }}
),

renamed_casted as (
    select
        id as listing_id,
        trim(listing_url) as listing_url,
        trim(name) as listing_name,
        trim(room_type) as room_type,
        try_cast(minimum_nights as integer) as minimum_nights,
        host_id,
        -- Clean and cast price: remove '$', ',', and convert to numeric
        try_cast(replace(replace(price, '$', ''), ',', '') as decimal(10, 2)) as price_usd,
        created_at as listing_created_at,
        updated_at as listing_updated_at
    from source
)

select * from renamed_casted