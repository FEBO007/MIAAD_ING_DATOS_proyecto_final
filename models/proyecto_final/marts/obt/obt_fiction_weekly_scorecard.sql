with nyt as (
    select
        week_key,
        book_key,
        rank,
        rank_last_week,
        weeks_on_list,
        rank_delta,
        is_new,
        extracted_at
    from {{ ref('fct_nyt_weekly_rank') }}
),

wk as (
    select
        week_key,
        week_date as published_date
    from {{ ref('dim_week') }}
),

b as (
    select
        book_key,
        isbn13_clean as isbn13,
        title,
        publisher,
        language,
        category_1,
        average_rating,
        ratings_count,
        gbooks_id,
        gbooks_selflink,
        amazon_product_url,
        book_image,
        extracted_at as book_extracted_at
    from {{ ref('dim_book') }}
),

v as (
    select
        week_key,
        book_key,
        views_7d
    from {{ ref('fct_wiki_book_views_7d_weekly') }}
),

final as (
    select
        -- ✅ keys
        nyt.week_key,
        nyt.book_key,

        wk.published_date,

        nyt.rank,
        nyt.rank_last_week,
        nyt.weeks_on_list,
        nyt.rank_delta,
        nyt.is_new,

        b.isbn13,
        b.title,
        b.publisher,
        b.language,
        b.category_1,
        b.average_rating,
        b.ratings_count,
        b.gbooks_id,
        b.gbooks_selflink,
        b.amazon_product_url,
        b.book_image,

        coalesce(v.views_7d, 0) as views_7d,

        -- ✅ timestamp “más fresco” disponible
        greatest(
            coalesce(nyt.extracted_at, timestamp '1970-01-01'),
            coalesce(b.book_extracted_at, timestamp '1970-01-01')
        ) as extracted_at
    from nyt
    join wk
      on wk.week_key = nyt.week_key
    join b
      on b.book_key = nyt.book_key
    left join v
      on v.week_key = nyt.week_key
     and v.book_key = nyt.book_key
)

select *
from final