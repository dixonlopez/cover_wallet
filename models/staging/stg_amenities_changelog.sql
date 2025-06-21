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
)

select * from transformed_changelog
