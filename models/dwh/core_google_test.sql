{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'pk_core_google',
    cluster_by = ['product_id']
  )
}}
SELECT
  {{dbt_utils.surrogate_key(['date', 'product_id', 'country_region'])}} as pk_core_google,
  'android' AS platform,
  date,
  t_product.product_id AS product_id,
  t_google.country_region AS country_iso2, -- To be controled when is ZZ or Other
  t_google.store_listing_visitors AS store_page_views,
  store_listing_acquisitions AS store_downloads
FROM
  `dwh-prod-ingestion.google_play.stats_store_performance_country` t_google
JOIN
  `dwh-adev-aso.batchsql.r_product` t_product
ON
  t_google.package_name = t_product.package_name

WHERE 1=1
-- and country_region != 'DK'
{% if is_incremental() -%}
  AND date >= date_sub(CURRENT_DATE(), INTERVAL {{ var('offset_days') }} DAY)
  -- and date between '{{ var('start_date') }}' and '{{var('end_date')}}'
  -- hola
{% endif %} 
