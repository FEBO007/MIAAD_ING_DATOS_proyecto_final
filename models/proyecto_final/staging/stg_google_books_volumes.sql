with src as (
  select
    -- isbn13 viene como DECIMAL(38,9) en raw => normalizar a texto sin decimales
    case
      when isbn13 is null then null
      else cast(cast(isbn13 as bigint) as varchar)
    end as isbn13_clean,

    id as gbooks_id,
    selflink as gbooks_selflink,
    _airbyte_extracted_at as extracted_at,

    -- campos principales desde volumeinfo (JSON)
    json_extract_string(volumeinfo, '$.title') as title,
    json_extract_string(volumeinfo, '$.subtitle') as subtitle,
    json_extract_string(volumeinfo, '$.publisher') as publisher,
    json_extract_string(volumeinfo, '$.publishedDate') as published_date_raw,
    json_extract_string(volumeinfo, '$.language') as language,
    json_extract_string(volumeinfo, '$.categories[0]') as category_1,

    -- ratings (opcionales en Google Books)
    try_cast(json_extract_string(volumeinfo, '$.averageRating') as double) as average_rating,
    try_cast(json_extract_string(volumeinfo, '$.ratingsCount') as bigint) as ratings_count,

    -- mantener JSON crudo por trazabilidad
    volumeinfo,
    saleinfo,
    accessinfo,
    searchinfo

  from {{ source('raw', 'google_books_by_nyt') }} as src
)
select * from src