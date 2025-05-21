with raw_availability as (
    -- Assuming a raw table like RAW_AVAILABILITY with a 'notes' column
    select * from {{ source('raw_airbnb_data', 'RAW_AVAILABILITY') }}
),

transformed_availability as (
    select
        listing_id,
        available_date,
        is_available,
        notes as listing_notes_column, -- Assuming this column exists in your raw data
        {{ map_listing_unavailability_reason('notes', var('customer_name')) }} as unavailability_reason
    from raw_availability
)

select * from transformed_availability