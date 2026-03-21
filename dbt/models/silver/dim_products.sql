
select distinct on (provider_id, product_id, as_of_date)
  provider_id,
  brand_name,
  as_of_date,
  product_id,
  product_name,
  product_description,
  product_category,
  product_last_updated,
  effective_from,
  effective_to,
  product_brand,
  product_brand_group,
  is_tailored
from {{ ref('stg_products_raw') }}
order by provider_id, product_id, as_of_date, fetched_at desc
