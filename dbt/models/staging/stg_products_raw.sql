
with src as (
  select
    run_id,
    provider_id,
    brand_name,
    endpoint,
    url,
    page_num,
    http_status,
    responded_xv,
    fetched_at,
    fetched_at::date as as_of_date,
    payload
  from {{ source('raw','products_raw') }}
  where http_status = 200
),
products as (
  select
    run_id,
    provider_id,
    brand_name,
    fetched_at,
    as_of_date,
    endpoint,
    url,
    responded_xv,
    jsonb_array_elements(coalesce(payload->'data'->'products','[]'::jsonb)) as product
  from src
)
select
  run_id,
  provider_id,
  brand_name,
  fetched_at,
  as_of_date,
  endpoint,
  url,
  responded_xv,
  product->>'productId'                as product_id,
  product->>'name'                     as product_name,
  product->>'description'              as product_description,
  product->>'productCategory'          as product_category,
  {{ to_timestamptz("product->>'lastUpdated'") }}     as product_last_updated,
  {{ to_timestamptz("product->>'effectiveFrom'") }}   as effective_from,
  {{ to_timestamptz("product->>'effectiveTo'") }}     as effective_to,
  product->>'brand'                    as product_brand,
  nullif(product->>'brandGroup','')    as product_brand_group,
  (case when nullif(product->>'isTailored','') is null then null else (product->>'isTailored')::boolean end) as is_tailored,
  product->'fees'                      as fees_json,
  product->'depositRates'              as deposit_rates_json,
  product->'lendingRates'              as lending_rates_json
from products
where product->>'productId' is not null
