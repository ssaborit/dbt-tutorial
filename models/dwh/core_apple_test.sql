-- Core Apple
{% if not var('backfill') %}
    {% set start_date = modules.datetime.date.fromisoformat(var('start_date')) - modules.datetime.timedelta(days=var('offset_days')) %}
{% else %}
    {% set start_date = modules.datetime.date.fromisoformat(var('start_date')) %}
{% endif %}
{% set end_date = modules.datetime.date.fromisoformat(var('end_date'))  %}
{{ date_range_load(start_date, end_date) }}
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'date', 'data_type': 'date'},
    cluster_by = ['product_id']

  )
}}

WITH
  totals AS (
    -- TEMPORAL TOTAL APP AND WEB REFERRER
  SELECT
    DATE(t_store.date)                  AS date,
    t_store.app_id,
    t_store.territory,
    t_store.source_type,
    t_store.impressions_unique_device   AS store_impressions,
    t_store.page_views_unique_device    AS store_page_views,
    t_store.impressions                 AS store_impressions_raw,
    t_store.page_views                  AS store_page_views_raw,
    t_units.app_units                   AS store_downloads,
    t_units.in_app_purchases            AS store_in_app_purchases,
    t_usage.installations               AS store_installs,
    t_usage.sessions                    AS store_sessions,
    t_usage.active_devices              AS store_active_devices
  FROM
    `dwh-prod-ingestion.itunes_connect.app_store_territory_source_type_report` t_store
  JOIN
    `dwh-prod-ingestion.itunes_connect.app_units_territory_source_type_report` t_units
  ON
    t_store.app_id = t_units.app_id
    AND DATE(t_store.date) = DATE(t_units.date)
    AND t_store.source_type = t_units.source_type
    AND t_store.territory = t_units.territory
  JOIN
    `dwh-prod-ingestion.itunes_connect.usage_territory_source_type_report` t_usage
  ON
    t_store.app_id = t_usage.app_id
    AND DATE(t_store.date) = DATE(t_usage.date)
    AND t_store.source_type = t_usage.source_type
    AND t_store.territory = t_usage.territory
  JOIN -- To be removed when Fivetran's data is ok
    `dwh-adev-aso.batchsql.r_product` r_product
  ON
    t_store.app_id = r_product.app_id
  WHERE
    1=1
    --AND DATE(t_store.date) >= date_sub(CURRENT_DATE(), INTERVAL {{ var('offset_days') }} DAY)
    and DATE(t_store.date) between '{{ start_date }}' and '{{ end_date }}'

    ),

  referrer AS (
  SELECT
    DATE(t_store_app.date)                  AS date,
    t_store_app.app_id,
    t_store_app.territory,
    'App Referrer'                          AS source_type,
    t_store_app.app_referrer                AS app_referrer,
    NULL AS web_referrer,
    t_store_app.impressions_unique_device   AS store_impressions,
    t_store_app.page_views_unique_device    AS store_page_views,
    t_store_app.impressions                 AS store_impressions_raw,
    t_store_app.page_views                  AS store_page_views_raw,
    t_units_app.app_units                   AS store_downloads,
    t_units_app.in_app_purchases            AS store_in_app_purchases,
    t_usage_app.installations               AS store_installs,
    t_usage_app.sessions                    AS store_sessions,
    t_usage_app.active_devices              AS store_active_devices
  FROM
    `dwh-prod-ingestion.itunes_connect.app_store_territory_app_referrer_report` t_store_app
  JOIN
    `dwh-prod-ingestion.itunes_connect.app_units_territory_app_referrer_report` t_units_app
  ON
    t_store_app.app_id = t_units_app.app_id
    AND DATE(t_store_app.date) = DATE(t_units_app.date)
    AND t_store_app.territory = t_units_app.territory
    AND t_store_app.app_referrer = t_units_app.app_referrer
  JOIN
    `dwh-prod-ingestion.itunes_connect.usage_territory_app_referrer_report` t_usage_app
  ON
    t_store_app.app_id = t_usage_app.app_id
    AND DATE(t_store_app.date) = DATE(t_usage_app.date)
    AND t_store_app.territory = t_usage_app.territory
    AND t_store_app.app_referrer = t_usage_app.app_referrer
  JOIN -- To be removed when Fivetran's data is ok
    `dwh-adev-aso.batchsql.r_product` r_product
  ON
    t_store_app.app_id = r_product.app_id
  WHERE
    1=1

    --AND DATE(t_store_app.date) >= date_sub(CURRENT_DATE(), INTERVAL {{ var('offset_days') }} DAY)
    and DATE(t_store_app.date) between '{{ start_date }}' and '{{ end_date }}'


  UNION ALL
    -- TEMP TOTALES WEB REFERRER
  SELECT
    DATE(t_store_web.date)                  AS date,
    t_store_web.app_id,
    t_store_web.territory,
    'Web Referrer'                          AS source_type,
    NULL AS app_referrer,
    t_store_web.web_referrer                AS web_referrer,
    t_store_web.impressions_unique_device   AS store_impressions,
    t_store_web.page_views_unique_device    AS store_page_views,
    t_store_web.impressions                 AS store_impressions_raw,
    t_store_web.page_views                  AS store_page_views_raw,
    t_units_web.app_units                   AS store_downloads,
    t_units_web.in_app_purchases            AS store_in_app_purchases,
    t_usage_web.installations               AS store_installs,
    t_usage_web.sessions                    AS store_sessions,
    t_usage_web.active_devices              AS store_active_devices
  FROM
    `dwh-prod-ingestion.itunes_connect.app_store_territory_web_referrer_report` t_store_web
  JOIN
    `dwh-prod-ingestion.itunes_connect.app_units_territory_web_referrer_report` t_units_web
  ON
    t_store_web.app_id = t_units_web.app_id
    AND DATE(t_store_web.date) = DATE(t_units_web.date)
    AND t_store_web.territory = t_units_web.territory
    AND t_store_web.web_referrer = t_units_web.web_referrer
  JOIN
    `dwh-prod-ingestion.itunes_connect.usage_territory_web_referrer_report` t_usage_web
  ON
    t_store_web.app_id = t_usage_web.app_id
    AND DATE(t_store_web.date) = DATE(t_usage_web.date)
    AND t_store_web.territory = t_usage_web.territory
    AND t_store_web.web_referrer = t_usage_web.web_referrer
  JOIN -- To be removed when Fivetran's data is ok
    `dwh-adev-aso.batchsql.r_product` r_product
  ON
    t_store_web.app_id = r_product.app_id
  WHERE
    1=1

    --AND DATE(t_store_web.date) >= date_sub(CURRENT_DATE(), INTERVAL {{ var('offset_days') }} DAY)
    and DATE(t_store_web.date) between '{{ start_date }}' and '{{ end_date }}'


  ),
  referrer_totals AS (
    -- TEMPORAL TOTALS APP REFERRER
  SELECT
    date,
    app_id,
    territory,
    source_type,
    SUM(store_impressions)      AS store_impressions,
    SUM(store_page_views)       AS store_page_views,
    SUM(store_impressions_raw)  AS store_impressions_raw,
    SUM(store_page_views_raw)   AS store_page_views_raw,
    SUM(store_downloads)        AS store_downloads,
    SUM(store_in_app_purchases) AS store_in_app_purchases,
    SUM(store_installs)         AS store_installs,
    SUM(store_sessions)         AS store_sessions,
    SUM(store_active_devices)   AS store_active_devices
  FROM
    referrer

  GROUP BY
    1,
    2,
    3,
    4
  )
, others AS
(
SELECT
  totals.date AS date,
  totals.app_id,
  totals.territory,
  totals.source_type,
  CASE
    WHEN totals.source_type = 'App Referrer' THEN 'Others'
    ELSE NULL
  END AS app_referrer,
  CASE
    WHEN totals.source_type = 'Web Referrer' THEN 'Others'
    ELSE NULL
  END AS web_referrer,
  coalesce(totals.store_impressions, 0) - coalesce(referrer_totals.store_impressions, 0)            as store_impressions,
  coalesce(totals.store_page_views, 0) - coalesce(referrer_totals.store_page_views, 0)              as store_page_views,
  coalesce(totals.store_impressions_raw, 0) - coalesce(referrer_totals.store_impressions_raw, 0)    as store_impressions_raw,
  coalesce(totals.store_page_views_raw, 0) - coalesce(referrer_totals.store_page_views_raw, 0)      as store_page_views_raw,
  coalesce(totals.store_downloads, 0) - coalesce(referrer_totals.store_downloads, 0)                as store_downloads,
  coalesce(totals.store_in_app_purchases, 0) - coalesce(referrer_totals.store_in_app_purchases, 0)  as store_in_app_purchases,
  coalesce(totals.store_installs, 0) - coalesce(referrer_totals.store_installs, 0)                  as store_installs,
  coalesce(totals.store_sessions, 0) - coalesce(referrer_totals.store_sessions, 0)                  as store_sessions,
  coalesce(totals.store_active_devices, 0) - coalesce(referrer_totals.store_active_devices, 0)      as store_active_devices
FROM
  totals
JOIN
  referrer_totals
ON
  totals.app_id = referrer_totals.app_id
  AND totals.date = referrer_totals.date
  AND totals.source_type = referrer_totals.source_type
  AND totals.territory = referrer_totals.territory
WHERE
  1=1
),
referrer_with_others as
(
SELECT * FROM referrer
UNION ALL
SELECT * FROM others

), final_table as(
SELECT
    totals.date,
    'ios' as platform,
    r_product.product_id as product_id,
    totals.app_id,
    totals.territory,
    totals.source_type,
    app_referrer,
    web_referrer,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_impressions ELSE totals.store_impressions END              AS store_impressions,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_page_views ELSE totals.store_page_views END                AS store_page_views,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_impressions_raw ELSE totals.store_impressions_raw END      AS store_impressions_raw,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_page_views_raw ELSE totals.store_page_views_raw END        AS store_page_views_raw,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_downloads ELSE totals.store_downloads END                  AS store_downloads,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_in_app_purchases ELSE totals.store_in_app_purchases END    AS store_in_app_purchases,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_installs ELSE totals.store_installs END                    AS store_installs,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_sessions ELSE totals.store_sessions END                    AS store_sessions,
    CASE WHEN (app_referrer is not null or web_referrer is not null) then referrer_with_others.store_active_devices ELSE totals.store_active_devices END        AS store_active_devices
FROM totals
LEFT JOIN referrer_with_others
ON  totals.app_id = referrer_with_others.app_id
    AND totals.date = referrer_with_others.date
    AND totals.territory = referrer_with_others.territory
    AND totals.source_type  = referrer_with_others.source_type
JOIN -- To be changed to left join when Fivetran's data is ok
    `dwh-adev-aso.batchsql.r_product` r_product
  ON
    totals.app_id = r_product.app_id
)
SELECT *
FROM final_table f
