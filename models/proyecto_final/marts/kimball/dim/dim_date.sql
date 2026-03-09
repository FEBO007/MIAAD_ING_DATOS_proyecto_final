with bounds as (

    select
        least(
            coalesce(
                (select min(cast(view_date as date))
                 from {{ ref('stg_wiki_pageviews_by_article') }}
                 where view_date is not null),
                current_date
            ),
            coalesce(
                (select min(cast(published_date as date))
                 from {{ ref('stg_nyt_hardcover_fiction') }}
                 where published_date is not null),
                current_date
            )
        ) as start_date,

        greatest(
            coalesce(
                (select max(cast(view_date as date))
                 from {{ ref('stg_wiki_pageviews_by_article') }}
                 where view_date is not null),
                current_date
            ),
            coalesce(
                (select max(cast(strptime(cast(snapshot_date_key as varchar), '%Y%m%d') as date))
                 from {{ ref('fct_google_books_snapshot') }}
                 where snapshot_date_key is not null),
                current_date
            ),
            current_date
        ) + interval '2 day' as end_date

),

date_spine as (
    select
        cast(gs as date) as date_day
    from bounds
    cross join generate_series(start_date, end_date, interval '1 day') as t(gs)
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    cast(strftime(date_day, '%Y') as integer) as year,
    cast(strftime(date_day, '%m') as integer) as month,
    cast(strftime(date_day, '%d') as integer) as day
from date_spine
order by date_day
