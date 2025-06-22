{{ config(
    materialized='table'
) }}

with available_dates as (
    select
        listing_id,
        listing_name,
        calendar_date
    from {{ ref('fct_daily_listing_performance') }}
    where is_available = true
),

grouped_consecutive_dates as (
    select
        listing_id,
        listing_name,
        calendar_date,
        date_sub(
            calendar_date,
            interval row_number() over (partition by listing_id order by calendar_date) day
        ) as consecutive_group_key
    from available_dates
),

-- Calculate consecutive availability periods with their start and end dates
consecutive_availability_periods as (
    select
        listing_id,
        any_value(listing_name) as listing_name,
        min(calendar_date) as valid_from_date,
        max(calendar_date) as valid_to_date,
        count(calendar_date) as consecutive_available_days_count
    from grouped_consecutive_dates
    group by
        listing_id,
        consecutive_group_key
)

select
    listing_id,
    listing_name,
    valid_from_date,
    valid_to_date,
    consecutive_available_days_count as max_consecutive_availability_days
from consecutive_availability_periods
qualify row_number() over (
    partition by listing_id
    order by consecutive_available_days_count desc, valid_from_date asc
) = 1
order by
    max_consecutive_availability_days desc
