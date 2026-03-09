with keys as (
    select book_key, book_nk, isbn13_clean, book_uri
    from {{ ref('int_book_keys') }}
),

gb_isbn as (
    select
        isbn13_clean,
        gbooks_id,
        gbooks_selflink,
        title as title_gb,
        subtitle,
        publisher as publisher_gb,
        language,
        category_1,
        extracted_at as extracted_at_gb
    from {{ ref('int_google_books_best_volume_isbn') }}
),

gb_search as (
    select
        isbn13_clean,
        average_rating,
        ratings_count,
        extracted_at as extracted_at_ratings
    from {{ ref('int_google_books_best_volume') }}
),

nyt_raw as (
    select
        regexp_replace(cast(isbn13 as varchar), '\\.0+$', '') as isbn13_clean,
        book_uri,
        published_date,
        title_nyt,
        author_nyt,
        publisher_nyt,
        amazon_product_url,
        book_image,
        extracted_at
    from {{ ref('stg_nyt_hardcover_fiction') }}
),

nyt as (
    select *
    from (
        select
            nyt_raw.*,
            row_number() over (
                partition by coalesce(nullif(isbn13_clean,''), nullif(book_uri,''))
                order by extracted_at desc nulls last, published_date desc nulls last
            ) as rn
        from nyt_raw
    )
    where rn = 1
),

joined as (
    select
        k.book_key,
        k.book_nk,
        k.isbn13_clean,
        k.book_uri,

        -- ✅ título/metadata: ISBN-stream si existe, si no NYT
        coalesce(nyt.title_nyt, gbi.title_gb) as title,
        gbi.subtitle,
        coalesce(nyt.publisher_nyt, gbi.publisher_gb) as publisher,
        gbi.language,
        gbi.category_1,

        -- ✅ ratings: preferimos search-stream (más probable), fallback a null
        gbs.average_rating,
        gbs.ratings_count,

        -- ids/links: preferimos ISBN-stream
        gbi.gbooks_id,
        gbi.gbooks_selflink,
        nyt.amazon_product_url,
        nyt.book_image,

        greatest(
            coalesce(gbi.extracted_at_gb, timestamp '1970-01-01'),
            coalesce(gbs.extracted_at_ratings, timestamp '1970-01-01'),
            coalesce(nyt.extracted_at, timestamp '1970-01-01')
        ) as extracted_at,

        nyt.title_nyt as title_nyt,
        gbi.title_gb as title_gb,
        nyt.publisher_nyt as publisher_nyt,
        gbi.publisher_gb as publisher_gb,

    from keys k
    left join gb_isbn gbi
        on gbi.isbn13_clean = k.isbn13_clean
    left join gb_search gbs
        on gbs.isbn13_clean = k.isbn13_clean
    left join nyt
        on (
            (k.isbn13_clean is not null and nyt.isbn13_clean = k.isbn13_clean)
            or (k.isbn13_clean is null and k.book_uri is not null and nyt.book_uri = k.book_uri)
        )
)

select
    book_key,
    book_nk,
    isbn13_clean,
    book_uri,
    title,
    title_nyt,
    title_gb,
    subtitle,
    publisher,
    publisher_nyt,
    publisher_gb,
    language,
    category_1,
    average_rating,
    ratings_count,
    gbooks_id,
    gbooks_selflink,
    amazon_product_url,
    book_image,
    extracted_at
from joined