with src as (
  select
    cast(published_date as date) as published_date,
    book_uri,
    nullif(primary_isbn13, '') as isbn13,
    nullif(title, '') as title_nyt,
    nullif(author, '') as author_nyt,
    nullif(publisher, '') as publisher_nyt,

    try_cast(rank as integer) as rank,
    try_cast(rank_last_week as integer) as rank_last_week,
    try_cast(weeks_on_list as integer) as weeks_on_list,

    amazon_product_url,
    book_image,

    _airbyte_extracted_at as extracted_at
  from {{ source('raw', 'nyt_hardcover_fiction_by_date') }} as src
)
select * from src
where isbn13 is not null