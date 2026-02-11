
with p as (
  select *
  from {{ ref('stg_products_raw') }}
),
fees as (
  select
    provider_id,
    brand_name,
    as_of_date,
    product_id,
    product_name,
    product_category,
    jsonb_array_elements(coalesce(fees_json,'[]'::jsonb)) as fee
  from p
)
select
  provider_id,
  brand_name,
  as_of_date,
  product_id,
  product_name,
  product_category,
  fee->>'name'              as fee_name,
  fee->>'feeType'           as fee_type,
  fee->>'additionalInfo'    as additional_info,
  fee->>'additionalInfoUri' as additional_info_uri,
  {{ to_numeric("fee->>'amount'") }} as amount,
  nullif(fee->>'currency','') as currency,
  fee as fee_json
from fees
