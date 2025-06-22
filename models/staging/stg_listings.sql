{{ config(materialized="view") }}

with
    source as (select * from {{ source("raw_semrush", "listings") }}),

    transformed_listings as (
        select
            cast(id as string) as listing_id,
            trim(name) as listing_name,

            -- Host Information
            cast(host_id as string) as host_id,
            trim(host_name) as host_name,
            cast(host_since as date) as host_since_date,
            trim(host_location) as host_location,
            array(
                select replace(element, '\"', '')
                from unnest(json_extract_array(host_verifications)) as element
            ) as host_verifications_list,

            -- Property Details
            trim(neighborhood) as neighborhood,
            trim(property_type) as property_type,
            trim(room_type) as room_type,
            cast(accommodates as int64) as accommodates_capacity,
            bathrooms_text,
            safe_cast(
                split(trim(bathrooms_text), ' ')[offset(0)] as float64
            ) as bathrooms_count,
            cast(bedrooms as int64) as bedrooms_count,
            cast(beds as int64) as beds_count,
            array(
                select replace(element, '\"', '')
                from unnest(json_extract_array(amenities)) as element
            ) as amenities_list,
            coalesce(cast(price as numeric), 0) as price_per_night,

            -- Review Scores
            coalesce(cast(number_of_reviews as int64), 0) as total_number_of_reviews,
            cast(first_review as date) as first_review_date,
            cast(last_review as date) as last_review_date,
            cast(review_scores_rating as numeric) as review_scores_rating

        from source
    )

select *
from transformed_listings
