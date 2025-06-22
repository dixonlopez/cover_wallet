{{ config(
    materialized='table'
) }}

with avg_daily_prices as (
  select
    neighborhood,
    extract(year from calendar_month) as year,
    avg(daily_price_usd) as avg_daily_price_usd
  from
    {{ ref('fct_daily_listing_performance') }}
  where
    calendar_date between date('2021-07-12') and date('2022-07-11')
  group by
    neighborhood,
    year
),

pivoted as (
  select
    neighborhood,
    max(case when year = 2021 then avg_daily_price_usd end) as avg_daily_price_2021_usd,
    max(case when year = 2022 then avg_daily_price_usd end) as avg_daily_price_2022_usd
  from
    avg_daily_prices
  group by
    neighborhood
)

select
  neighborhood,
  round(avg_daily_price_2021_usd, 2) as avg_daily_price_2021_usd,
  round(avg_daily_price_2022_usd, 2) as avg_daily_price_2022_usd,
  round(avg_daily_price_2022_usd - avg_daily_price_2021_usd, 2) as daily_price_increase_usd,
  round(safe_divide(avg_daily_price_2022_usd - avg_daily_price_2021_usd, avg_daily_price_2021_usd) * 100, 1) as pct_daily_price_increase
from
  pivoted
order by
  pct_daily_price_increase desc