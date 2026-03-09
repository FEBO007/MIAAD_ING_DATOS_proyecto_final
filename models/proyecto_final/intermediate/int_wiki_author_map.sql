with src as (
    select distinct
        article as wiki_article,

        -- Normalización del título del artículo para hacer match con dim_author
        regexp_replace(
            trim(
                lower(
                    replace(article, '_', ' ')
                )
            ),
            '\\s+',
            ' '
        ) as author_name_norm

    from {{ ref('stg_wiki_pageviews_by_article') }}
    where article is not null
      and trim(article) <> ''
),

final as (
    select
        md5(cast(author_name_norm as varchar)) as author_key,
        wiki_article,
        author_name_norm
    from src
    where author_name_norm is not null
      and author_name_norm <> ''
)

select *
from final