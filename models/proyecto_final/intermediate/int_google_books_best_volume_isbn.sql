with base as (
  select
    *
  from {{ ref('stg_google_books_volumes_isbn') }}
  where isbn13_clean is not null
),

ranked as (
  select
    *,
    case
      when lower(coalesce(json_extract_string(saleinfo, '$.isEbook'), 'false')) = 'true' then 1
      else 0
    end as is_ebook_flag,

    try_cast(json_extract_string(volumeinfo, '$.publishedDate') as date) as published_date,

    row_number() over (
      partition by isbn13_clean
      order by
        -- para ISBN-stream ya debería ser exacto, así que priorizamos “mejor info”
        ratings_count desc nulls last,
        average_rating desc nulls last,
        is_ebook_flag desc,
        published_date desc nulls last,
        extracted_at desc nulls last,
        gbooks_id asc
    ) as rn
  from base
)

select
  isbn13_clean,
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