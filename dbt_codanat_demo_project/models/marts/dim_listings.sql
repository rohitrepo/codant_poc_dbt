with listings as (
    select * from {{ ref('stg_listings') }}
),
hosts as (
    select host_id, host_name, is_superhost from {{ ref('dim_hosts') }}
)

select
    l.listing_id,
    l.listing_url,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price_usd,
    l.listing_created_at,
    l.listing_updated_at,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost, -- aliasing to avoid confusion
    datediff(day, l.listing_created_at, current_date()) as listing_age_days
from listings l
left join hosts h on l.host_id = h.host_id