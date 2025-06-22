{{ config(
    materialized='view'
) }}

with base_calendar_reservations as (
    select
        listing_id,
        calendar_date,
        reservation_id,
        reservation_uid,
        price_per_night_usd,
        minimum_nights,
        maximum_nights
    from {{ ref('base_calendar') }}
    where reservation_id is not null
),

aggregated_reservations as (
    select
        reservation_uid,
        any_value(listing_id) as listing_id,
        any_value(reservation_id) as reservation_id,
        min(calendar_date) as reservation_start_date,
        max(calendar_date) as reservation_end_date,
        sum(price_per_night_usd) as total_reservation_price_usd,
        max(minimum_nights) as applicable_minimum_nights,
        min(maximum_nights) as applicable_maximum_nights
    from base_calendar_reservations
    group by
        reservation_uid
)

select
    reservation_uid,
    listing_id,
    reservation_id,
    reservation_start_date,
    reservation_end_date,
    date_diff(reservation_end_date, reservation_start_date, day) + 1 as reservation_duration_days,
    total_reservation_price_usd,
    applicable_minimum_nights,
    applicable_maximum_nights,
    case
        when
            date_diff(reservation_end_date, reservation_start_date, day) + 1
            BETWEEN applicable_minimum_nights AND applicable_maximum_nights
        then true
        else false
    end as is_duration_within_limits
from aggregated_reservations
