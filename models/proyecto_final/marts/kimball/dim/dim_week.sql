with weeks as (
    select distinct
        cast(published_date as date) as week_date
    from {{ ref('stg_nyt_hardcover_fiction') }}
    where published_date is not null
)

select
    cast(strftime(week_date, '%Y%m%d') as integer) as week_key,
    week_date,
    cast(strftime(week_date, '%Y') as integer) as year,
    cast(strftime(week_date, '%m') as integer) as month
from weeks