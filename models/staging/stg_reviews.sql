{{ config(materialized="view") }}

with
    source as (
        select *
        from {{ source("raw_semrush", "generated_reviews") }}
        where id is not null
    ),
    
    /*
There are two null review_ids. It's worth reviewing these with the business team to understand what's happening,
especially since their listing_ids don't correspond to any existing listings
*/
    transformed_reviews as (
        select
            cast(id as string) as review_id,
            cast(listing_id as string) as listing_id,
            cast(review_score as numeric) as review_score,
            cast(review_date as date) as review_date

        from source
    )

select *
from transformed_reviews
