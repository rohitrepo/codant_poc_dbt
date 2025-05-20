with reviews as (
    select * from {{ ref('stg_reviews') }}
),
listings as (
    select listing_id, host_id from {{ ref('dim_listings') }}
)

select
    r.review_id,
    r.listing_id,
    l.host_id,
    r.review_date,
    r.reviewer_name,
    r.review_comment,
    r.review_sentiment,
    case
        when r.review_sentiment = 'positive' then 1
        when r.review_sentiment = 'negative' then -1
        when r.review_sentiment = 'neutral' then 0
        else null -- Or some other default for unexpected values
    end as sentiment_score
from reviews r
join listings l on r.listing_id = l.listing_id