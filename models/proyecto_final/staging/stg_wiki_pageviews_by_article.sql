with src as (
  select
    project,
    article,
    access,
    agent,
    granularity,
    timestamp,
    -- timestamp viene como YYYYMMDD00 => tomamos YYYYMMDD y lo convertimos a DATE
    try_strptime(substr(timestamp, 1, 8), '%Y%m%d')::date as view_date,
    try_cast(views as bigint) as views,
    _airbyte_extracted_at as extracted_at
  from {{ source('raw', 'wiki_pageviews_by_article') }} as src
)
select * from src
where view_date is not null