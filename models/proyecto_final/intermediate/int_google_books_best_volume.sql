with base as (
  select
    *
  from {{ ref('stg_google_books_volumes') }}
  where isbn13_clean is not null
),

enriched as (
  select
    base.*,

    -- ✅ 1 si el ISBN-13 del item coincide con isbn13_clean
    case
      when exists (
        select 1
        from json_each(base.volumeinfo, '$.industryIdentifiers') as t
        where json_extract_string(t.value, '$.type') = 'ISBN_13'
          and json_extract_string(t.value, '$.identifier') = base.isbn13_clean
      ) then 1
      else 0
    end as has_isbn_match,

    -- flags auxiliares para ordenar
    case
      when lower(coalesce(json_extract_string(base.saleinfo, '$.isEbook'), 'false')) = 'true' then 1
      else 0
    end as is_ebook_flag,

    -- publishedDate puede venir como 'YYYY', 'YYYY-MM-DD', etc.
    try_cast(json_extract_string(base.volumeinfo, '$.publishedDate') as date) as published_date

  from base
),

ranked as (
  select
    *,
    row_number() over (
      partition by isbn13_clean
      order by
        has_isbn_match desc,                 -- ✅ prioridad absoluta: edición que matchea ISBN
        ratings_count desc nulls last,
        average_rating desc nulls last,
        is_ebook_flag desc,
        published_date desc nulls last,
        extracted_at desc nulls last,
        gbooks_id asc
    ) as rn
  from enriched
)

select
  isbn13_clean,
  has_isbn_match,
  gbooks_id,
  gbooks_selflink,
  extracted_at,
  title,
  subtitle,
  publisher,
  published_date_raw,
  language,
  category_1,
  average_rating,
  ratings_count,
  volumeinfo,
  saleinfo,
  accessinfo,
  searchinfo
from ranked
where rn = 1