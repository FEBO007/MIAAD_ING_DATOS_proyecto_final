with src as (
    select
        gb.isbn13_clean,
        cast(gb.extracted_at as date) as snapshot_date,
        gb.average_rating,
        gb.ratings_count,
        gb.gbooks_id,
        gb.gbooks_selflink,
        gb.extracted_at
    from {{ ref('int_google_books_best_volume') }} gb
    where gb.isbn13_clean is not null
),

keys as (
    select
        k.book_key,
        cast(strftime(src.snapshot_date, '%Y%m%d') as integer) as snapshot_date_key,
        src.average_rating,
        src.ratings_count,
        src.gbooks_id,
        src.gbooks_selflink,
        src.extracted_at,
        row_number() over (
            partition by k.book_key, cast(strftime(src.snapshot_date, '%Y%m%d') as integer)
            order by src.extracted_at desc nulls last
        ) as rn
    from src
    join {{ ref('int_book_keys') }} k
        on k.isbn13_clean = src.isbn13_clean
    where src.snapshot_date is not null
)

select
    book_key,
    snapshot_date_key,
    average_rating,
    ratings_count,
    gbooks_id,
    gbooks_selflink,
    extracted_at
from keys
where rn = 1