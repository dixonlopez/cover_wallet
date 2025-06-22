{{ config(
    materialized='table'
) }}

with revenue_by_segment as (
  select
    calendar_month,
    'Air conditioning' in unnest(amenities_list) as has_air_conditioning,
    sum(daily_price_usd) as segment_revenue_usd
  from
    {{ ref('fct_daily_listing_performance') }}
  where
    -- Ensure we're only considering occupied days for revenue calculation
    is_available = false -- If is_available is false, it means it was reserved/occupied
  group by
    calendar_month,
    has_air_conditioning
),

revenue_pivot as (
  select
    calendar_month,
    sum(case when has_air_conditioning then segment_revenue_usd else 0 end) as revenue_with_ac,
    sum(case when not has_air_conditioning then segment_revenue_usd else 0 end) as revenue_without_ac,
    sum(segment_revenue_usd) as total_month_revenue
  from
    revenue_by_segment
  group by
    calendar_month
)

select
  calendar_month,
  total_month_revenue,
  revenue_with_ac,
  revenue_without_ac,
  round(safe_divide(revenue_with_ac, total_month_revenue) * 100.0, 1) as pct_with_ac,
  round(safe_divide(revenue_without_ac, total_month_revenue) * 100.0, 1) as pct_without_ac
from
  revenue_pivot
order by
  calendar_month
