
select
  provider_id,
  brand_name,
  as_of_date,
  product_id,
  fee_name,
  fee_type,
  amount,
  currency,
  additional_info,
  additional_info_uri,
  fee_json
from {{ ref('stg_fees_raw') }}
