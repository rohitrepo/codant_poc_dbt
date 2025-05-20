with hosts as (
    select * from {{ ref('stg_hosts') }}
)

select
    host_id,
    host_name,
    is_superhost,
    host_created_at,
    host_updated_at,
    datediff(day, host_created_at, current_date()) as host_tenure_days
from hosts