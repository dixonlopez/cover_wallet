{{ config(materialized="view") }}

with
    source as (select * from {{ source("raw_semrush", "calendar") }}),

    transformed_calendar as (
        select
            {{ dbt_utils.generate_surrogate_key(['listing_id', 'reservation_id', 'date']) }} as calendar_day_surrogate_key,
            cast(listing_id as string) as listing_id,
            cast(date as date) as calendar_date,
            available as is_available,
            -- I've confirmed that when there's a reservation id, is_available is false
            nullif(cast(reservation_id as string), 'NULL') as reservation_id,
            coalesce(cast(price as numeric), 0) as price_per_night_calendar,
            cast(minimum_nights as int64) as minimum_nights,
            cast(maximum_nights as int64) as maximum_nights
        from source
    )

select *
from transformed_calendar
