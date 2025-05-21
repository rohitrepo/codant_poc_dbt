{#
    macros/map_listing_unavailability_reason.sql

    This macro categorizes the reason for a listing's unavailability based on free-text notes.
    It mirrors the structure of the `map_unavailable_reason_from_stickies` macro,
    adapting the logic for an Airbnb-like use case.

    Arguments:
    - `listing_notes_column`: The column containing free-text notes or comments about listing unavailability.
    - `customer_name_var`: The dbt variable that holds the customer's name (e.g., `var('customer_name')`).
                           This allows for dynamic logic based on the client or property type.
#}
{% macro map_listing_unavailability_reason(listing_notes_column, customer_name_var) -%}
case
    -- Placeholder for a function that might determine if a listing is "open despite notes".
    -- If such a concept exists in your Airbnb data, you would define and call a macro here.
    -- For now, it's commented out as it requires specific data context.
    -- when {{ map_is_open_despite_listing_notes(listing_notes_column) }} then null

    -- Categorization for Holiday Closures
    when {{ listing_notes_column }} ilike any (
        {%- if customer_name_var == 'airbnb_corporate_client' %}
            'Christmas%',
            'New Year%',
            'Thanksgiving%',
            'Independence Day%',
            'Labor Day%',
            'Memorial Day%'
        {%- elif customer_name_var != var('excluded_client_for_staff_service', 'generic_excluded_client') %}
            -- General holiday keywords for clients not matching 'generic_excluded_client'
            '%HOLIDAY%',
            '%CHRISTMAS%',
            '%NEW YEAR%',
            '%THANKSGIVING%',
            '%INDEPENDENCE DAY%',
            '%LABOR DAY%',
            '%MEMORIAL DAY%',
            '%BANK HOLIDAY%'
        {%- endif %}
    ) then 'HOLIDAY_CLOSURE'

    -- Categorization for Host Personal Use or General Host Unavailability
    when {{ listing_notes_column }} ilike any (
        {%- if customer_name_var == 'airbnb_managed_properties' %}
            -- Specific keywords for centrally managed properties
            '%owner occupied%',
            '%personal use%',
            '%family visit%',
            '%staffing issue%' -- Could be relevant for managed properties
        {%- else %}
            -- General host unavailability keywords
            '%host unavailable%',
            '%personal leave%',
            '%owner use%',
            '%blocked by owner%',
            '%not accepting bookings%'
        {%- endif %}
    ) then 'HOST_PERSONAL_USE'

    -- Categorization for Maintenance, Repairs, or Renovations
    when {{ listing_notes_column }} ilike any (
        'CLOSED FOR REPAIR',
        '% REPAIR%',
        '%CONSTRUCTION%', 'CONSTR%',
        '%RENOVATION%',
        '%PAINTING%',
        '%UPDATE%',
        '% EQUIPMENT%',
        '%UPGRADE%',
        '%MAINTENANCE%',
        '%MODERNIZATION%',
        '%INSTALL%',
        '%BROKEN%',
        '%SERVICE CALLED%'
    ) then 'MAINTENANCE_OR_REPAIR'

    -- Categorization for Cleaning or Turnover
    when {{ listing_notes_column }} ilike any (
        '%TURNOVER%', 'TURN OVER%', 'SET%UP%',
        '% clean%', '%CLEANING%', '%GUEST PREP%', '%SANITIZ%'
    ) then 'CLEANING_OR_TURNOVER'

    -- Categorization for General Listing Suspension or Closure
    when {{ listing_notes_column }} ilike any (
        '%CLOSE%','%CLOSURE%', 'CLOSE', 'CLOISED', 'CLOSEWD',
        '%SUSPENDED%', '%NOT ACTIVE%', '%INACTIVE%'
    ) then 'LISTING_SUSPENDED'

    -- Client-specific overrides (mimicking the structure from your original macro)
    {%- if customer_name_var == 'airbnb_boutique_hotels' %}
        when {{ listing_notes_column }} ilike any ('%NO MORE BOOKINGS%')
            then 'LISTING_SUSPENDED'
    {%- endif %}

    {%- if customer_name_var == 'airbnb_vacation_rentals' %}
        when {{ listing_notes_column }} ilike any ('%OWNER OUT OF TOWN%')
            then 'HOST_PERSONAL_USE'
    {%- endif %}

    {%- if customer_name_var == 'airbnb_special_events' %}
        when {{ listing_notes_column }} ilike any ('%EVENT BLOCK%', '%FESTIVAL%', '%CITY EVENT%')
            then 'EVENT_BLOCK'
    {%- endif %}

    -- Default category if no other conditions are met
    else 'OTHER_UNAVAILABILITY'
end
{%- endmacro %}
