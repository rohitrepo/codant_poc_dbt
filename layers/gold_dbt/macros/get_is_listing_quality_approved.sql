{%- macro get_is_listing_quality_approved(
    listing_name = 'listing_name',
    room_type = 'room_type',
    price = 'price',
    minimum_nights = 'minimum_nights',
    review_count = 'review_count',
    review_score = 'review_score'
) %}
    {#-
        Returns true if listing meets quality standards for the given market.
        Criteria vary by market/city and include:
        - Valid listing name
        - Approved room types
        - Price within market range
        - Reasonable minimum nights
        - Sufficient reviews and scores
        
        Parameters:
        - listing_name: Name/title of the listing
        - room_type: Type of room/space
        - price: Nightly price in USD
        - minimum_nights: Minimum stay requirement
        - review_count: Number of reviews
        - review_score: Average review score (typically 1-5)
    #}
    case
        when
            {%- if var('market') == 'new_york' %}
                -- New York City specific criteria
                {{ listing_name }} is not null
                and {{ room_type }} in ('Entire home/apt', 'Private room')
                and {{ price }} between 50 and 1000
                and {{ minimum_nights }} <= 30
                and (
                    {{ review_count }} >= 5 and {{ review_score }} >= 4.0
                    or {{ review_count }} = 0  -- New listings without reviews
                )
                and not {{ listing_name }} ilike any (
                    '%illegal%',
                    '%unauthorized%',
                    '%not real%',
                    '%test%',
                    '%fake%'
                )

            {%- elif var('market') == 'san_francisco' %}
                -- San Francisco specific criteria
                {{ listing_name }} is not null
                and {{ room_type }} in ('Entire home/apt', 'Private room', 'Shared room')
                and {{ price }} between 75 and 1200
                and {{ minimum_nights }} <= 14
                and {{ review_score }} >= 3.8
                and not {{ listing_name }} ilike any (
                    '%short term%',
                    '%under 30 days%',
                    '%lease%'
                )

            {%- elif var('market') == 'tokyo' %}
                -- Tokyo specific criteria
                {{ listing_name }} is not null
                and {{ room_type }} in ('Entire home/apt', 'Private room')
                and {{ price }} between 3000 and 50000  -- JPY converted to USD equivalent
                and {{ minimum_nights }} <= 7
                and {{ review_count }} >= 3
                and not {{ listing_name }} ilike any (
                    '%minpaku%',  -- Japanese term that might indicate unlicensed
                    '%違法%'      -- Illegal in Japanese
                )

            {%- elif var('market') == 'london' %}
                -- London specific criteria
                {{ listing_name }} is not null
                and {{ room_type }} in ('Entire home/apt', 'Private room')
                and {{ price }} between 40 and 800  -- GBP converted to USD equivalent
                and {{ minimum_nights }} <= 90  -- London allows longer stays
                and (
                    {{ review_count }} >= 2 and {{ review_score }} >= 4.2
                    or {{ review_count }} = 0
                )
                and not {{ listing_name }} ilike any (
                    '%council flat%',
                    '%housing association%',
                    '%sublet%'
                )

            {%- else %}
                -- Default criteria for other markets
                {{ listing_name }} is not null
                and {{ room_type }} in ('Entire home/apt', 'Private room', 'Shared room', 'Hotel room')
                and {{ price }} between 20 and 2000
                and {{ minimum_nights }} <= 365
                and (
                    {{ review_count }} >= 1 and {{ review_score }} >= 3.5
                    or {{ review_count }} = 0
                )
                and not {{ listing_name }} ilike any (
                    '%not real%',
                    '%test%',
                    '%fake%',
                    '%scam%'
                )
            {%- endif %}
        then true
        else false
    end
{%- endmacro %}