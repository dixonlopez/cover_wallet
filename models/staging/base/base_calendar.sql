{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw_semrush', 'calendar') }}
)

select
    -- Generate a unique ID for each calendar day entry (unique day for a listing)
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'date']) }} as listing_availability_uid,
    -- Generate a unique ID for the reservation itself ONLY if reservation_id is not NULL
    case
        when nullif(cast(reservation_id as string), 'NULL') is not null
        then {{ dbt_utils.generate_surrogate_key(['reservation_id', 'listing_id']) }}
        else null
    end as reservation_uid,

    cast(listing_id as string) as listing_id,
    cast(date as date) as calendar_date,
    cast(available as boolean) as is_available,
    nullif(cast(reservation_id as string), 'NULL') as reservation_id,
    coalesce(cast(price as numeric), 0) as price_per_night_usd,
    cast(minimum_nights as int64) as minimum_nights,
    cast(maximum_nights as int64) as maximum_nights
from source