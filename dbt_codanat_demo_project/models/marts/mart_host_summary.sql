with hosts as (
    select * from {{ ref('dim_hosts') }}
),
listings as (
    select * from {{ ref('dim_listings') }}
),
reviews as (
    select * from {{ ref('fct_reviews') }}
),

host_listing_metrics as (
    select
        host_id,
        count(distinct listing_id) as total_listings,
        avg(price_usd) as avg_listing_price,
        min(listing_created_at) as first_listing_date,
        max(listing_created_at) as last_listing_date
    from listings
    group by 1
),

host_review_metrics as (
    select
        host_id,
        count(distinct review_id) as total_reviews_on_listings,
        avg(sentiment_score) as avg_review_sentiment_score,
        sum(case when review_sentiment = 'positive' then 1 else 0 end) as total_positive_reviews,
        sum(case when review_sentiment = 'negative' then 1 else 0 end) as total_negative_reviews,
        sum(case when review_sentiment = 'neutral' then 1 else 0 end) as total_neutral_reviews
    from reviews
    group by 1
)

select
    h.host_id,
    h.host_name,
    h.is_superhost,
    h.host_tenure_days,
    coalesce(hlm.total_listings, 0) as total_listings,
    hlm.avg_listing_price,
    hlm.first_listing_date,
    hlm.last_listing_date,
    coalesce(hrm.total_reviews_on_listings, 0) as total_reviews_on_listings,
    hrm.avg_review_sentiment_score,
    coalesce(hrm.total_positive_reviews, 0) as total_positive_reviews,
    coalesce(hrm.total_negative_reviews, 0) as total_negative_reviews,
    coalesce(hrm.total_neutral_reviews, 0) as total_neutral_reviews
from hosts h
left join host_listing_metrics hlm on h.host_id = hlm.host_id
left join host_review_metrics hrm on h.host_id = hrm.host_id
order by h.host_id