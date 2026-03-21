
with p as (
  select *
  from {{ ref('stg_products_raw') }}
),
deposit as (
  select
    provider_id,
    brand_name,
    as_of_date,
    product_id,
    product_name,
    product_category,
    'deposit'::text as rate_kind,
    jsonb_array_elements(coalesce(deposit_rates_json,'[]'::jsonb)) as rate
  from p
),
lending as (
  select
    provider_id,
    brand_name,
    as_of_date,
    product_id,
    product_name,
    product_category,
    'lending'::text as rate_kind,
    jsonb_array_elements(coalesce(lending_rates_json,'[]'::jsonb)) as rate
  from p
),
unioned as (
  select * from deposit
  union all
  select * from lending
)
select
  provider_id,
  brand_name,
  as_of_date,
  product_id,
  product_name,
  product_category,
  rate_kind,
  coalesce(rate->>'depositRateType', rate->>'lendingRateType') as rate_type,
  {{ to_numeric("rate->>'rate'") }} as rate_value,
  {{ to_numeric("rate->>'comparisonRate'") }} as comparison_rate,
  rate->>'calculationFrequency' as calculation_frequency,
  rate->>'applicationFrequency' as application_frequency,
  rate->>'additionalValue' as additional_value,
  rate->>'additionalInfo' as additional_info,
  rate->>'additionalInfoUri' as additional_info_uri,
  rate->'tiers' as tiers_json,
  rate->'conditions' as conditions_json,
  rate as rate_json
from unioned
where coalesce(rate->>'depositRateType', rate->>'lendingRateType') is not null
