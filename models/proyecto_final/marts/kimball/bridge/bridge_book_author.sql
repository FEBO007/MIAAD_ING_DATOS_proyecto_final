with src as (
    select
        p.book_key,
        d.author_key,
        p.author_order,
        p.is_primary
    from {{ ref('int_author_parse') }} p
    join {{ ref('dim_author') }} d
      on d.author_name_norm = p.author_name_norm
    where p.book_key is not null
      and p.author_name_norm is not null
),

dedup as (
    select
        *,
        row_number() over (
            partition by book_key, author_key
            order by
                is_primary desc,
                author_order asc
        ) as rn
    from src
)

select
    book_key,
    author_key,
    author_order,
    is_primary
from dedup
where rn = 1