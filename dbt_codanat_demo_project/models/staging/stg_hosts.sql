with source as (
    select * from {{ source('raw_airbnb_data', 'RAW_HOSTS') }}
),

renamed_casted as (
    select
        id as host_id,
        trim(name) as host_name,
        -- Convert 't'/'f' to boolean
        iff(lower(trim(is_superhost)) = 't', true, false) as is_superhost,
        created_at as host_created_at,
        updated_at as host_updated_at
    from source
)

select * from renamed_casted