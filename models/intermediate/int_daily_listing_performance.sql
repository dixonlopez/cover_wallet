-- models/intermediate/int_daily_listing_performance.sql

-- This intermediate table combines data from staging models to create a daily snapshot
-- of listing performance and attributes. It serves as the base for the final mart layer.
-- Grain: day and listing_id

{{ config(
    materialized='table'    
) }}

with listings as (
    select
        listing_id,
        listing_name,
        host_id,
        host_name,
        host_since_date,
        host_location,
        host_verifications_list,
        neighborhood,
        property_type,
        room_type,
        accommodates_capacity,
        bathrooms_count,
        bedrooms_count,
        beds_count,
        amenities_list,
        price_per_night,
        total_number_of_reviews,
        first_review_date,
        last_review_date,
        review_scores_rating

    from {{ ref('stg_listings') }}
),

calendar as (
    select
        listing_id,
        calendar_date,
        is_available,
        reservation_id, 
        price_per_night_calendar,
        minimum_nights,
        maximum_nights
   from {{ ref('stg_calendar') }}
),

reservations as (
    select
        reservation_id,
        listing_id,
        reservation_start_date,
        reservation_end_date,
        reservation_duration_days,
        total_reservation_price,
        is_duration_within_limits,
        applicable_minimum_nights,
        applicable_maximum_nights
   from {{ ref('int_reservations') }}
)

select
    cal.calendar_date,
    l.listing_id,
    l.listing_name,
    l.host_id,
    l.host_name,
    l.host_since_date,
    l.host_location,
    l.neighborhood,
    l.property_type,
    l.room_type,
    l.accommodates_capacity,
    l.bathrooms_count,
    l.bedrooms_count,
    l.beds_count,
    l.amenities_list,
    l.has_air_conditioning,
    l.has_lockbox,
    l.has_first_aid_kit,
    coalesce(cal.price_per_night_calendar, l.price_per_night) as daily_price,
    cal.is_available,
    -- is_occupied now directly uses reservation_id to signify a booking
    case when cal.reservation_id is not null then 1 else 0 end as is_occupied,
    cal.reservation_id, -- New: Expose reservation_id at the daily grain
    res.reservation_start_date, -- New: Reservation start date
    res.reservation_end_date,   -- New: Reservation end date
    res.reservation_duration_days, -- New: Total days of the reservation
    res.total_reservation_price, -- New: Total price for the reservation
    res.is_duration_within_limits, -- New: Flag if reservation duration is within limits
    res.applicable_minimum_nights, -- New: Applicable minimum nights for the reservation
    res.applicable_maximum_nights, -- New: Applicable maximum nights for the reservation

    l.total_number_of_reviews,
    l.first_review_date,
    l.last_review_date,
    l.review_scores_rating,
    l.is_email_verified,
    l.is_phone_verified,
    l.has_reviews_verification,
    l.is_kba_verified,
    l.is_work_email_verified,
    l.is_google_connected,
    l.is_facebook_connected,
    l.is_linkedin_connected,
    l.is_jumio_verified,
    l.has_offline_government_id,
    l.has_online_government_id,
    l.has_manual_identity_check,
    l.has_online_identity_check,
    l.is_amex_verified,
    l.is_weibo_connected,
    l.has_selfie_verification,
    l.is_sesame_connected,
    l.is_zhima_id_connected,
    l.is_apple_connected
from
    calendar cal
inner join
    listings l on cal.listing_id = l.listing_id
left join -- Use left join as not all calendar entries will be part of a reservation
    reservations res on cal.reservation_id = res.reservation_id
