with src as (
    select
        cast(published_date as date) as week_date,
        regexp_replace(cast(isbn13 as varchar), '\\.0+$', '') as isbn13_clean,
        nullif(trim(book_uri), '') as book_uri,

        rank,

        -- ✅ 0 en NYT = "no rank last week" (equivalente a NULL para cálculos)
        nullif(rank_last_week, 0) as rank_last_week,

        weeks_on_list,

        case
            when nullif(rank_last_week, 0) is null then null
            else (nullif(rank_last_week, 0) - rank)
        end as rank_delta,

        case
            when nullif(rank_last_week, 0) is null and weeks_on_list = 1 then true
            else false
        end as is_new,

        extracted_at
    from {{ ref('stg_nyt_hardcover_fiction') }}
),

keys as (
    select
        k.book_key,
        cast(strftime(src.week_date, '%Y%m%d') as integer) as week_key,
        src.rank,
        src.rank_last_week,
        src.weeks_on_list,
        src.rank_delta,
        src.is_new,
        src.extracted_at,
        row_number() over (
            partition by k.book_key, cast(strftime(src.week_date, '%Y%m%d') as integer)
            order by src.extracted_at desc nulls last
        ) as rn
    from src
    join {{ ref('int_book_keys') }} k
      on k.book_nk = coalesce(nullif(src.isbn13_clean,''), nullif(src.book_uri,''))
    where src.week_date is not null
      and coalesce(nullif(src.isbn13_clean,''), nullif(src.book_uri,'')) is not null
)

select
    book_key,
    week_key,
    rank,
    rank_last_week,
    weeks_on_list,
    rank_delta,
    is_new,
    extracted_at
from keys
where rn = 1