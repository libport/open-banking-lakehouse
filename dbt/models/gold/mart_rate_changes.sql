
with provider_dates as (
  select
    provider_id,
    as_of_date as current_date,
    lag(as_of_date) over (partition by provider_id order by as_of_date) as previous_date
  from {{ ref('fct_product_rates') }}
  group by provider_id, as_of_date
),
cur as (
  select
    d.provider_id,
    d.current_date,
    d.previous_date,
    r.*
  from {{ ref('fct_product_rates') }} r
  join provider_dates d
    on r.provider_id = d.provider_id
   and r.as_of_date = d.current_date
  where d.previous_date is not null
),
prv as (
  select
    d.provider_id,
    d.current_date,
    d.previous_date,
    r.*
  from {{ ref('fct_product_rates') }} r
  join provider_dates d
    on r.provider_id = d.provider_id
   and r.as_of_date = d.previous_date
  where d.previous_date is not null
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
    coalesce(cur.tier_unit_of_measure, prv.tier_unit_of_measure) as tier_unit_of_measure,
    coalesce(cur.tier_minimum_value, prv.tier_minimum_value) as tier_minimum_value,
    coalesce(cur.tier_maximum_value, prv.tier_maximum_value) as tier_maximum_value,
    coalesce(cur.previous_date, prv.previous_date) as previous_as_of_date,
    coalesce(cur.current_date, prv.current_date) as current_as_of_date,
    prv.rate as previous_rate,
    cur.rate as current_rate
  from cur
  full outer join prv
    on cur.provider_id = prv.provider_id
   and cur.current_date = prv.current_date
   and cur.previous_date = prv.previous_date
   and cur.product_id = prv.product_id
   and cur.rate_kind = prv.rate_kind
   and cur.rate_type = prv.rate_type
   and coalesce(cur.tier_name,'') = coalesce(prv.tier_name,'')
   and coalesce(cur.tier_unit_of_measure,'') = coalesce(prv.tier_unit_of_measure,'')
   and coalesce(cur.tier_minimum_value, -1) = coalesce(prv.tier_minimum_value, -1)
   and coalesce(cur.tier_maximum_value, -1) = coalesce(prv.tier_maximum_value, -1)
)
select *
from joined
where
  previous_as_of_date is not null
  and (previous_rate is distinct from current_rate)
