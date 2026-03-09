with nyt as (
    select
        nullif(trim(cast(isbn13 as varchar)), '') as isbn13_raw,
        nullif(trim(book_uri), '') as book_uri
    from {{ ref('stg_nyt_hardcover_fiction') }}
),

nyt_clean as (
    select
        regexp_replace(isbn13_raw, '\\.0+$', '') as isbn13_clean,
        book_uri
    from nyt
),

-- ✅ usar Google Books deduplicado
gb as (
    select
        nullif(trim(cast(isbn13_clean as varchar)), '') as isbn13_clean,
        cast(null as varchar) as book_uri
    from {{ ref('int_google_books_best_volume') }}
),

unioned as (
    select isbn13_clean, book_uri from nyt_clean
    union all
    select isbn13_clean, book_uri from gb
),

-- ✅ definir book_nk primero (llave natural) y consolidar
final as (
    select
        coalesce(nullif(isbn13_clean,''), nullif(book_uri,'')) as book_nk,

        -- consolidación: si hay varias filas con mismo book_nk, quedate con algún valor no nulo
        max(nullif(isbn13_clean,'')) as isbn13_clean,
        max(nullif(book_uri,'')) as book_uri
    from unioned
    where coalesce(nullif(isbn13_clean,''), nullif(book_uri,'')) is not null
    group by 1
)

select
    md5(cast(book_nk as varchar)) as book_key,
    book_nk,
    isbn13_clean,
    book_uri
from final