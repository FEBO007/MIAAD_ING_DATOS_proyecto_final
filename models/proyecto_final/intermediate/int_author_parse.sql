with base as (
    select
        k.book_key,
        k.book_nk,

        -- Google Books: authors viene como JSON array dentro de volumeinfo.
        -- Lo convertimos a string CSV simple: ["A","B"] -> A,B
        coalesce(
            nullif(
                trim(
                    replace(
                        replace(
                            replace(
                                cast(json_extract(gb.volumeinfo, '$.authors') as varchar),
                                '[', ''
                            ),
                            ']', ''
                        ),
                        '"', ''
                    )
                ),
                ''
            ),
            nullif(trim(nyt.author_nyt), '')
        ) as authors_raw

    from {{ ref('int_book_keys') }} k

    -- ✅ usar la versión deduplicada (1 fila por isbn13_clean)
    left join {{ ref('int_google_books_best_volume') }} gb
        on gb.isbn13_clean = k.isbn13_clean

    -- ✅ join directo si stg_nyt ya tiene isbn13 limpio
    left join {{ ref('stg_nyt_hardcover_fiction') }} nyt
        on nyt.isbn13 = k.isbn13_clean
),

split_authors as (
    -- separa por ',' y también por 'and' / '&' (normalizado a coma)
    select
        book_key,
        book_nk,
        authors_raw,
        unnest(
            string_split(
                replace(
                    replace(
                        replace(authors_raw, ' & ', ' and '),
                        ' AND ', ' and '
                    ),
                    ' and ', ','
                ),
                ','
            )
        ) as author_piece
    from base
    where authors_raw is not null
),

cleaned as (
    select
        book_key,
        book_nk,
        trim(author_piece) as author_name_raw,
        regexp_replace(trim(lower(author_piece)), '\\s+', ' ') as author_name_norm
    from split_authors
    where trim(author_piece) <> ''
)

select
    book_key,
    book_nk,
    author_name_raw,
    author_name_norm,
    row_number() over (partition by book_key order by author_name_norm) as author_order,
    case
        when row_number() over (partition by book_key order by author_name_norm) = 1 then true
        else false
    end as is_primary
from cleaned