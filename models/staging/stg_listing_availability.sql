{{ config(
    materialized='view'
) }}

select
    listing_availability_uid,
    listing_id,
    calendar_date,
    is_available,
    reservation_id,
    reservation_uid,
    price_per_night_usd,
    minimum_nights,
    maximum_nights
from {{ ref('base_calendar') }}


