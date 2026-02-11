
with r as (
  select *
  from {{ ref('stg_rates_raw') }}
),
tiers as (
  select
    provider_id,
    brand_name,
    as_of_date,
    product_id,
    product_name,
    product_category,
    rate_kind,
    rate_type,
    rate_value,
    comparison_rate,
    calculation_frequency,
    application_frequency,
    additional_value,
    additional_info,
    additional_info_uri,
    conditions_json,
    tier
  from r
  left join lateral jsonb_array_elements(coalesce(tiers_json,'[]'::jsonb)) as jt(tier) on true
)
select
  provider_id,
  brand_name,
  as_of_date,
  product_id,
  product_name,
  product_category,
  rate_kind,
  rate_type,
  rate_value,
  comparison_rate,
  calculation_frequency,
  application_frequency,
  additional_value,
  additional_info,
  additional_info_uri,
  nullif(tier->>'name','') as tier_name,
  nullif(tier->>'unitOfMeasure','') as tier_unit_of_measure,
  {{ to_numeric("tier->>'minimumValue'") }} as tier_minimum_value,
  {{ to_numeric("tier->>'maximumValue'") }} as tier_maximum_value,
  tier->>'rateApplicationMethod' as tier_rate_application_method,
  tier->>'applicability' as tier_applicability,
  tier->>'additionalInfo' as tier_additional_info,
  tier->>'additionalInfoUri' as tier_additional_info_uri,
  conditions_json
from tiers
