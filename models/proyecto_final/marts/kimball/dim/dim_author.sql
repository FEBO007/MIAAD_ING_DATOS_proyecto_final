with from_books as (
    select
        author_name_norm,
        author_name_raw,
        cast(null as varchar) as wiki_article
    from {{ ref('int_author_parse') }}
    where author_name_norm is not null
),

from_wiki as (
    select
        author_name_norm,
        cast(null as varchar) as author_name_raw,
        wiki_article
    from {{ ref('int_wiki_author_map') }}
    where author_name_norm is not null
),

unioned as (
    select * from from_books
    union all
    select * from from_wiki
),

ranked as (
    select
        *,
        row_number() over (
            partition by author_name_norm
            order by
                case when wiki_article is not null then 1 else 0 end desc,
                case when author_name_raw is not null then 1 else 0 end desc,
                length(coalesce(author_name_raw, '')) desc,
                coalesce(wiki_article, '') desc,
                coalesce(author_name_raw, '') desc
        ) as rn
    from unioned
)

select
    md5(cast(author_name_norm as varchar)) as author_key,
    author_name_norm,
    author_name_raw,
    wiki_article,
    case when wiki_article is not null then true else false end as is_wiki_resolved
from ranked
where rn = 1