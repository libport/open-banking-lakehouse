
select
  provider_id,
  brand_name,
  as_of_date,
  product_id,
  product_name,
  product_category,
  rate_kind,
  rate_type,
  tier_name,
  tier_unit_of_measure,
  tier_minimum_value,
  tier_maximum_value,
  rate_value as rate,
  comparison_rate,
  calculation_frequency,
  application_frequency,
  additional_value,
  additional_info,
  additional_info_uri,
  conditions_json
from {{ ref('stg_rate_tiers_raw') }}
