with daily as (
    select
        w.author_key,
        d.date_day,
        w.views
    from {{ ref('fct_wiki_pageviews_daily') }} w
    join {{ ref('dim_date') }} d
      on d.date_key = w.date_key
),

weeks as (
    select
        week_key,
        week_date
    from {{ ref('dim_week') }}
),

final as (
    select
        wk.week_key,
        wk.week_date,
        dy.author_key,
        sum(dy.views) as views_7d
    from daily dy
    join weeks wk
      on dy.date_day between (wk.week_date - interval 6 day) and wk.week_date
    group by 1,2,3
)

select *
from final