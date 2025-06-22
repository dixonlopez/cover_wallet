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
        price_per_night
    from {{ ref('stg_listings') }}
),

listing_availability as (
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
    from {{ ref('stg_listing_availability') }}
),

reservations as (
    select
        reservation_uid,
        listing_id,
        reservation_id,
        reservation_start_date,
        reservation_end_date,
        reservation_duration_days,
        total_reservation_price_usd,
        is_duration_within_limits,
        applicable_minimum_nights,
        applicable_maximum_nights
    from {{ ref('stg_reservations') }}
),

overall_reviews_summary as (
    select
        listing_id,
        count(review_id) as total_number_of_reviews,
        min(review_date) as first_review_date,
        max(review_date) as last_review_date,
        avg(review_score) as review_scores_rating
    from {{ ref('stg_reviews') }}
    group by listing_id
),

daily_reviews_snapshot as (
    select
        la.listing_id,
        la.calendar_date,
        count(sr.review_id) as cumulative_total_reviews,
        min(sr.review_date) as cumulative_first_review_date,
        max(sr.review_date) as cumulative_last_review_date,
        avg(sr.review_score) as cumulative_review_scores_rating
    from listing_availability la
    left join {{ ref('stg_reviews') }} sr
        on la.listing_id = sr.listing_id
        and sr.review_date <= la.calendar_date
    group by
        la.listing_id,
        la.calendar_date
),

amenity_changes as (
    select
        listing_id,
        amenities_list,
        valid_from_date,
        valid_to_date
    from {{ ref('stg_amenities_changelog') }}
)

select
    listing_availability.listing_availability_uid,
    listing_availability.calendar_date,
    date_trunc(listing_availability.calendar_date, month) AS calendar_month,
    date_trunc(listing_availability.calendar_date, year) AS calendar_year,
    listings.listing_id,
    listings.listing_name,
    listings.host_id,
    listings.host_name,
    listings.host_since_date,
    listings.host_location,
    listings.host_verifications_list,
    listings.neighborhood,
    listings.property_type,
    listings.room_type,
    listings.accommodates_capacity,
    listings.bathrooms_count,
    listings.bedrooms_count,
    listings.beds_count,
    amenity_changes.amenities_list,
    coalesce(listing_availability.price_per_night_usd, listings.price_per_night) as daily_price_usd,
    listing_availability.is_available,
    listing_availability.reservation_id,
    reservations.reservation_uid,
    reservations.reservation_start_date,
    reservations.reservation_end_date,
    reservations.reservation_duration_days,
    reservations.total_reservation_price_usd,
    reservations.is_duration_within_limits,
    reservations.applicable_minimum_nights,
    reservations.applicable_maximum_nights,

    -- Overall review information
    coalesce(overall_reviews_summary.total_number_of_reviews, 0) as overall_total_number_of_reviews,
    overall_reviews_summary.first_review_date as overall_first_review_date,
    overall_reviews_summary.last_review_date as overall_last_review_date,
    overall_reviews_summary.review_scores_rating as overall_review_scores_rating,

    -- Cumulative review information
    coalesce(daily_reviews_snapshot.cumulative_total_reviews, 0) as cumulative_total_reviews,
    daily_reviews_snapshot.cumulative_first_review_date,
    daily_reviews_snapshot.cumulative_last_review_date,
    coalesce(daily_reviews_snapshot.cumulative_review_scores_rating, 0) as cumulative_review_scores_rating
from
    listing_availability
inner join
    listings on listing_availability.listing_id = listings.listing_id
left join
    reservations on listing_availability.reservation_uid = reservations.reservation_uid
left join
    amenity_changes
    on listing_availability.listing_id = amenity_changes.listing_id
    and listing_availability.calendar_date between amenity_changes.valid_from_date and coalesce(amenity_changes.valid_to_date, '9999-12-31')
left join
    overall_reviews_summary on listings.listing_id = overall_reviews_summary.listing_id
left join
    daily_reviews_snapshot
    on listing_availability.listing_id = daily_reviews_snapshot.listing_id
    and listing_availability.calendar_date = daily_reviews_snapshot.calendar_date