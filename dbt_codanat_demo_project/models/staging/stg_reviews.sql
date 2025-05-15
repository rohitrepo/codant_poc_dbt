with source as (
    select * from {{ source('raw_airbnb_data', 'RAW_REVIEWS') }}
),

renamed_casted as (
    select
        listing_id,
        "DATE" as review_date, -- "DATE" needs to be quoted as it's a reserved keyword
        trim(reviewer_name) as reviewer_name,
        trim(comments) as review_comment,
        lower(trim(sentiment)) as review_sentiment,
        -- Generate a surrogate key for reviews as one doesn't exist
        {{ dbt_utils.generate_surrogate_key(['listing_id', 'reviewer_name', '"DATE"', 'comments']) }} as review_id
    from source
    where "DATE" is not null -- Ensure reviews have a date
)

select * from renamed_casted