-- Regla de negocio: para consumir noticias en BI, published_ts no debería ser NULL.
select *
from {{ ref('int_latest_news_clean') }}
where published_ts is null