
with dates as (
  select max(as_of_date) as current_date
  from {{ ref('fct_product_rates') }}
),
prev as (
  select max(as_of_date) as previous_date
  from {{ ref('fct_product_rates') }}
  where as_of_date < (select current_date from dates)
),
cur as (
  select * from {{ ref('fct_product_rates') }}
  where as_of_date = (select current_date from dates)
),
prv as (
  select * from {{ ref('fct_product_rates') }}
  where as_of_date = (select previous_date from prev)
),
joined as (
  select
    coalesce(cur.provider_id, prv.provider_id) as provider_id,
    coalesce(cur.brand_name, prv.brand_name) as brand_name,
    coalesce(cur.product_id, prv.product_id) as product_id,
    coalesce(cur.product_name, prv.product_name) as product_name,
    coalesce(cur.product_category, prv.product_category) as product_category,
    coalesce(cur.rate_kind, prv.rate_kind) as rate_kind,
    coalesce(cur.rate_type, prv.rate_type) as rate_type,
    coalesce(cur.tier_name, prv.tier_name) as tier_name,
    (select previous_date from prev) as previous_as_of_date,
    (select current_date from dates) as current_as_of_date,
    prv.rate as previous_rate,
    cur.rate as current_rate
  from cur
  full outer join prv
    on cur.provider_id = prv.provider_id
   and cur.product_id = prv.product_id
   and cur.rate_kind = prv.rate_kind
   and cur.rate_type = prv.rate_type
   and coalesce(cur.tier_name,'') = coalesce(prv.tier_name,'')
)
select *
from joined
where
  previous_as_of_date is not null
  and (previous_rate is distinct from current_rate)
