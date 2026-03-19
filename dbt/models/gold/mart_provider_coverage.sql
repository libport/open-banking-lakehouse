
with run_dates as (
  select distinct run_date as as_of_date
  from {{ source('bronze','pipeline_run') }}
),
brand_runs as (
  select
    pr.run_date as as_of_date,
    pr.run_started_at,
    b.data_holder_brand_id as provider_id,
    b.brand_name,
    coalesce(b.product_base_uri, b.public_base_uri) as expected_base_uri
  from {{ source('bronze','data_holder_brand') }} b
  join {{ source('bronze','pipeline_run') }} pr
    on pr.run_id = b.run_id
),
brands as (
  select
    as_of_date,
    provider_id,
    brand_name,
    expected_base_uri
  from (
    select
      as_of_date,
      provider_id,
      brand_name,
      expected_base_uri,
      row_number() over (partition by as_of_date, provider_id order by run_started_at desc) as rn
    from brand_runs
  ) t
  where rn = 1
),
calls as (
  select
    fetched_at::date as as_of_date,
    provider_id,
    http_status as last_http_status,
    error as last_error
  from (
    select
      fetched_at::date as as_of_date,
      provider_id,
      http_status,
      error,
      row_number() over (partition by fetched_at::date, provider_id order by fetched_at desc) as rn
    from {{ source('bronze','api_call_log') }}
    where endpoint = 'banking:get-products'
  ) t
  where rn = 1
),
pages as (
  select
    fetched_at::date as as_of_date,
    provider_id,
    count(*) filter (where http_status=200) as products_pages_ok,
    max(http_status) as last_http_status
  from {{ source('raw','products_raw') }}
  group by fetched_at::date, provider_id
),
rows as (
  select
    as_of_date,
    provider_id,
    count(*) as products_rows
  from {{ ref('dim_products') }}
  group by as_of_date, provider_id
)
select
  d.as_of_date,
  b.provider_id,
  b.brand_name,
  b.expected_base_uri,
  coalesce(p.products_pages_ok, 0) as products_pages_ok,
  coalesce(r.products_rows, 0) as products_rows,
  coalesce(c.last_http_status, p.last_http_status) as last_http_status,
  c.last_error as last_error
from run_dates d
join brands b on b.as_of_date = d.as_of_date
left join pages p on p.as_of_date = d.as_of_date and p.provider_id = b.provider_id
left join rows r on r.as_of_date = d.as_of_date and r.provider_id = b.provider_id
left join calls c on c.as_of_date = d.as_of_date and c.provider_id = b.provider_id
order by d.as_of_date, b.brand_name
