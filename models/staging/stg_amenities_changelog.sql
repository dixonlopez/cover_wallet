{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw_semrush', 'amenities_changelog') }}
),

transformed_changelog as (
    select
        cast(listing_id as string) as listing_id,
        cast(change_at as date) as change_date,
        array(
            select replace(element, '\"', '')
            from unnest(json_extract_array(amenities)) as element
        ) as amenities_list,
    from
        source
),

-- Calculate valid_from_date and valid_to_date for each amenity change
add_validity_dates as (
    select
        listing_id,
        change_date,
        amenities_list,
        change_date as valid_from_date,
        -- Calculate the next change_date for the same listing_id
        -- Subtract 1 day to ensure the period is non-overlapping
        -- If no next change, valid_to_date will be NULL, indicating the current (latest) configuration
        date_sub(
            lead(change_date) over (partition by listing_id order by change_date),
            interval 1 day
        ) as valid_to_date
    from transformed_changelog
)

select * from add_validity_dates