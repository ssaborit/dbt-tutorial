-- Reviews
--{% if is_incremental() -%}
--     {% set start_date = modules.datetime.date.fromisoformat(var('start_date')) - modules.datetime.timedelta(days=var('offset_days')) %}
--     {% set end_date = modules.datetime.date.fromisoformat(var('end_date'))  %}
--{% endif %}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'pk_reviews',
    cluster_by = ['product_id']
  )
}}
WITH
  apple AS (
  SELECT
    'ios' as platform,
    p.product_id          AS product_id,
    date(last_modified)   AS date,
    r.app_version_string  AS app_version,
    rating,
    title                 AS review_title,

    country_code,
    total_views,
    helpful_views,

    cast(null as timestamp)     as developer_reply_date_and_time,
    cast(null as string)        as developer_reply_text,
    cast(null as string)        as device,
    cast(null as string)        as review_link,
    last_modified               as review_submit_date_and_time,
    content                     as review_text,
    cast(null as string)        as reviewer_language,
    id                          as unique_value

  FROM
    `dwh-prod-ingestion.itunes_connect.review` r
  JOIN
    `dwh-adev-aso.batchsql.r_product` p
  ON
    p.app_id = r.app_id
  WHERE 1=1
  {% if is_incremental() -%}
    AND date(last_modified) >= date_sub(CURRENT_DATE(), INTERVAL {{ var('offset_days') }} DAY)
    --AND date(last_modified) between '{{ var('start_date') }}' and '{{var('end_date')}}'
  {% endif %}
  ),

  google AS (
  SELECT
    'android' as platform,
    p.product_id,
    date(review_last_update_date_and_time) as date,
    app_version_name as app_version,
    star_rating as rating,
    review_title,

    cast(null as string)                as country_code,
    cast(null as integer)               as total_views,
    cast(null as integer)               as helpful_views,

    developer_reply_date_and_time as developer_reply_date_and_time,
    developer_reply_text,
    device,
    review_link,
    review_submit_date_and_time   as review_submit_date_and_time,
    review_text,
    reviewer_language,
    review_last_update_millis_since_epoch as unique_value

  FROM
    `dwh-prod-ingestion.google_play.reviews` r
  Join
    `dwh-prod-core.pub.v_d_product` p
  on
    r.package_name = p.google_product_id
  WHERE 1=1
  {% if is_incremental() -%}
    AND date(review_last_update_date_and_time) >= date_sub(CURRENT_DATE(), INTERVAL {{ var('offset_days') }} DAY)
    --AND date(review_last_update_date_and_time) between '{{ var('start_date') }}' and '{{var('end_date')}}'
  {% endif %}

),

google_apple as (
select * from apple
union all
select * from google )

select ga.*, {{dbt_utils.surrogate_key(['platform', 'date', 'product_id', 'app_version', 'country_code', 'device', 'reviewer_language', 'unique_value'])}} as pk_reviews
from google_apple ga
