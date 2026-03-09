with src as (
    select
        w.article as wiki_article,
        w.view_date,
        w.views,
        w.access,
        w.agent,
        w.project,
        w.granularity,
        w.extracted_at
    from {{ ref('stg_wiki_pageviews_by_article') }} w
    where w.view_date is not null
),

mapped as (
    select
        m.author_key,
        cast(strftime(s.view_date, '%Y%m%d') as integer) as date_key,
        s.views,
        s.access,
        s.agent,
        s.project,
        s.granularity,
        s.extracted_at
    from src s
    left join {{ ref('int_wiki_author_map') }} m
        on m.wiki_article = s.wiki_article
)

select *
from mapped
where author_key is not null