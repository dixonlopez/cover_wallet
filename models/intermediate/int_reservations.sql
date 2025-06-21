
{{ config(
    materialized='table'
) }}

with calendar_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['reservation_id', 'listing_id']) }} as reservation_surrogate_key,
        listing_id,
        calendar_date,
        is_available,
        reservation_id,
        price_per_night_calendar,
        minimum_nights,
        maximum_nights
    from {{ ref('stg_calendar') }}
    where reservation_id is not null -- Only consider actual reservations
)

select
    reservation_surrogate_key,
    any_value(reservation_id) as reservation_id,
    any_value(listing_id) as listing_id,
    min(calendar_date) as reservation_start_date,
    max(calendar_date) as reservation_end_date,
    -- Calculate reservation duration (inclusive of start and end dates)
    date_diff(max(calendar_date), min(calendar_date), day) + 1 as reservation_duration_days,
    sum(price_per_night_calendar) as total_reservation_price,
    -- Determine the most restrictive minimum and maximum nights for the reservation period
    max(minimum_nights) as applicable_minimum_nights,
    min(maximum_nights) as applicable_maximum_nights,
    -- Check if the reservation duration falls within the allowed limits
    case
        when
            date_diff(max(calendar_date), min(calendar_date), day) + 1
            >= max(minimum_nights)
            and date_diff(max(calendar_date), min(calendar_date), day) + 1
            <= min(maximum_nights)
        then true
        else false
    end as is_duration_within_limits
from calendar_data 
group by
    reservation_surrogate_key