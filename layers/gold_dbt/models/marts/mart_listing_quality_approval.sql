SELECT
    listing_id,
    listing_name,
    {% set market = 'new_york' %}  -- or get from context
    {{ get_is_listing_quality_approved(
        listing_name = 'listing_name',
        room_type = 'room_type',
        price = 'price',
        minimum_nights = 'minimum_nights',
        review_count = 'number_of_reviews',
        review_score = 'review_scores_rating'
    ) }} as is_quality_approved
FROM
    {{ ref('dim_listings') }}