with a7 as (
    select
        week_key,
        author_key,
        views_7d
    from {{ ref('fct_wiki_author_views_7d_weekly') }}
),

ba as (
    select
        book_key,
        author_key
    from {{ ref('bridge_book_author') }}
),

joined as (
    select
        ba.book_key,
        a7.week_key,
        sum(a7.views_7d) as views_7d
    from ba
    join a7
      on a7.author_key = ba.author_key
    group by 1,2
)

select *
from joined