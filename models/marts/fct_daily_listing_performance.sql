{{ config(
    materialized='view'
) }}

select
    *,
    'Air conditioning' IN UNNEST(amenities_list) AS has_air_conditioning,
    'First aid kit' IN UNNEST(amenities_list) AS has_first_aid_kit,
    'Lockbox' IN UNNEST(amenities_list) AS has_lockbox

from {{ ref('int_daily_listing_performance') }}
