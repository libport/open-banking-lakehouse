
with latest_day as (
  select max(fetched_at::date) as as_of_date
  from {{ source('raw','products_raw') }}
),
brands as (
  select distinct on (data_holder_brand_id)
    data_holder_brand_id as provider_id,
    brand_name,
    coalesce(product_base_uri, public_base_uri) as expected_base_uri
  from {{ source('bronze','data_holder_brand') }}
  order by data_holder_brand_id, extracted_at desc
),
calls as (
  select
    provider_id,
    max(http_status) filter (where endpoint = 'banking:get-products') as last_http_status,
    max(error) filter (where endpoint = 'banking:get-products') as last_error
  from {{ source('bronze','api_call_log') }}
  where fetched_at::date = (select as_of_date from latest_day)
  group by provider_id
),
pages as (
  select
    provider_id,
    count(*) filter (where http_status=200) as products_pages_ok,
    max(http_status) as last_http_status
  from {{ source('raw','products_raw') }}
  where fetched_at::date = (select as_of_date from latest_day)
  group by provider_id
),
rows as (
  select
    provider_id,
    count(*) as products_rows
  from {{ ref('dim_products') }}
  where as_of_date = (select as_of_date from latest_day)
  group by provider_id
)
select
  (select as_of_date from latest_day) as as_of_date,
  b.provider_id,
  b.brand_name,
  b.expected_base_uri,
  coalesce(p.products_pages_ok, 0) as products_pages_ok,
  coalesce(r.products_rows, 0) as products_rows,
  coalesce(c.last_http_status, p.last_http_status) as last_http_status,
  c.last_error as last_error
from brands b
left join pages p on p.provider_id = b.provider_id
left join rows r on r.provider_id = b.provider_id
left join calls c on c.provider_id = b.provider_id
order by b.brand_name
