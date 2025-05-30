
{{
  config(
    alias= '5t-sa360-campaign_performance-v1'
  )
}}
with exchange_source AS (
    select 
        day,
        currency_code,
        ex_rate
    from {{ref('openexchange_rates','stg_openexchange_rates__openexchange_report_v1')}}
)
SELECT
SAFE_CAST( source_a.segments_date AS DATE) day, -- added 'a.Day',
SAFE_CAST( customer_engine_id AS STRING) account_engine_id,
SAFE_CAST( customer_descriptive_name AS STRING) account_name,
SAFE_CAST( customer_id AS STRING) account_id,
SAFE_CAST( campaign_network_settings_target_search_network AS BOOL) network_target_search_network,
SAFE_CAST( campaign_network_settings_target_content_network AS BOOL) network_target_content_network,
SAFE_CAST( campaign_network_settings_target_google_search AS BOOL) network_target_google_search,
SAFE_CAST( campaign_network_settings_target_partner_search_network AS BOOL) network_target_partner_search_network,
SAFE_CAST( segments_device AS STRING) device_segment,
SAFE_CAST( campaign_engine_id AS STRING) campaign_engine_id,
SAFE_CAST( metrics_client_account_conversions AS FLOAT64) adwords_conversions,
SAFE_CAST( campaign_name AS STRING) campaign_name,
SAFE_CAST( campaign_id AS STRING) campaign_id,
SAFE_CAST( metrics_top_impression_percentage AS FLOAT64) top_impression_percentage,
SAFE_CAST( NULL AS STRING) advertiser_id, -- Behaviour change
SAFE_CAST( metrics_absolute_top_impression_percentage AS FLOAT64) absolute_top_impression_percentage,
SAFE_CAST( metrics_conversions AS FLOAT64) conversions,
SAFE_CAST( metrics_conversions_value AS FLOAT64) conversions_value,
SAFE_CAST( metrics_all_conversions AS FLOAT64) all_conversions,
SAFE_CAST( metrics_cross_device_conversions AS FLOAT64) cross_device_conversions,
(SAFE_CAST( metrics_all_conversions_value AS FLOAT64)
- SAFE_CAST( metrics_cross_device_conversions AS FLOAT64)) dfa_actions,
SAFE_CAST( campaign_advertising_channel_type AS STRING) campaign_type,
SAFE_CAST( campaign_bidding_strategy_type AS STRING) adwords_bid_strategy,
SAFE_CAST( NULL AS STRING) advertiser, -- Behaviour change
SAFE_CAST( metrics_client_account_conversions_value AS FLOAT64)	adwords_conversion_value,
SAFE_CAST( customer_account_type AS STRING) account_type,
SAFE_CAST( metrics_client_account_view_through_conversions AS INT64) adwords_view_through_conversions,
SAFE_CAST( campaign_start_date AS DATE) campaign_Start_date,
SAFE_CAST( campaign_end_date AS DATE) campaign_end_date,
SAFE_CAST( metrics_search_absolute_top_impression_share AS FLOAT64) search_absolute_top_impression_share,
SAFE_CAST( metrics_search_top_impression_share AS FLOAT64) search_top_impression_share,
SAFE_CAST( campaign_status AS STRING) campaign_status,
SAFE_CAST( metrics_all_conversions_value AS FLOAT64) all_conversions_value,
SAFE_CAST( metrics_cross_device_conversions_value AS FLOAT64) cross_device_conversions_value,
(SAFE_CAST( metrics_all_conversions_value AS FLOAT64)
  - SAFE_CAST( metrics_cross_device_conversions_value AS FLOAT64)) dfa_revenue,
(SAFE_CAST( metrics_all_conversions AS FLOAT64)
- SAFE_CAST( metrics_cross_device_conversions AS FLOAT64)) dfa_transactions,
SAFE_CAST( metrics_clicks AS INT64) AS clicks,
SAFE_DIVIDE( SAFE_CAST( metrics_cost_micros AS FLOAT64), 1000000) cost,
SAFE_CAST( metrics_impressions AS FLOAT64) impressions,
SAFE_CAST( metrics_search_impression_share AS FLOAT64) search_impression_share,
SAFE_CAST(TRIM(customer_currency_code) AS STRING) statistics_currency_code,
'sa360-campaign_performance-v1' AS raw_origin,
SAFE_DIVIDE(SAFE_DIVIDE(SAFE_CAST(metrics_cost_micros AS FLOAT64), 1000000), exchange_source.ex_rate) _gbp_cost,
SAFE_DIVIDE((SAFE_CAST( metrics_all_conversions_value AS FLOAT64)
  - SAFE_CAST( metrics_cross_device_conversions_value AS FLOAT64)), exchange_source.ex_rate) _gbp_revenue,

/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
{{add_fields("campaign_name")}} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and 
openexchange table is being referred as exchange_source */

  {{source('sa360', 'campaign_performance_v1')}} source_a


LEFT JOIN exchange_source

ON

SAFE_CAST(source_a.segments_date AS DATE) = exchange_source.day
/* Jinja var if default field has null value.Replace the default field based on the report */
AND LOWER(IFNULL(TRIM(customer_currency_code),'{{var('account_currency')}}')) = exchange_source.currency_code
/*When the currency value doesn't exist in the default report we add
account currency variable by default */

