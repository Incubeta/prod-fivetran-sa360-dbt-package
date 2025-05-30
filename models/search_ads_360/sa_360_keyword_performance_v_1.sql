{{
  config(
    alias= '5t-sa360-keyword_performance-v1'
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
    SAFE_CAST( campaign_id AS STRING ) campaign_id,
    SAFE_CAST( NULL AS STRING ) advertiser, -- Behaviour change
    SAFE_CAST( source_a.segments_date AS DATE ) day,
    SAFE_CAST( segments_device AS STRING ) device_segment,
    SAFE_CAST( metrics_clicks AS INT64 ) clicks,
    SAFE_CAST( ad_group_criterion_keyword_match_type AS STRING ) keyword_match_type,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_cpa_cpc_bid_ceiling_micros AS FLOAT64), 1000000) AS bidding_strategy_target_cpa_cpc_bid_ceiling,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_roas_cpc_bid_ceiling_micros AS FLOAT64), 1000000) AS bidding_strategy_target_roas_cpc_bid_ceiling,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_impression_share_cpc_bid_ceiling_micros AS FLOAT64), 1000000) AS bidding_strategy_target_impression_share_cpc_bid_ceiling,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_outrank_share_cpc_bid_ceiling_micros AS FLOAT64), 1000000) AS bidding_strategy_target_outrank_share_cpc_bid_ceiling,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_spend_cpc_bid_ceiling_micros AS FLOAT64), 1000000) AS bidding_strategy_target_spend_cpc_bid_ceiling,
    -- Getting the largest value from the network channel bid ceiling
    -- values to mimic the old keyword_max_bid field
    GREATEST(
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_cpa_cpc_bid_ceiling_micros AS FLOAT64), 1000000), 0),
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_roas_cpc_bid_ceiling_micros AS FLOAT64), 1000000), 0),
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_impression_share_cpc_bid_ceiling_micros AS FLOAT64), 1000000), 0),
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_outrank_share_cpc_bid_ceiling_micros AS FLOAT64), 1000000), 0),
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_spend_cpc_bid_ceiling_micros AS FLOAT64), 1000000), 0)
    ) keyword_max_bid,
    SAFE_CAST( customer_account_type AS STRING ) account_type,
    SAFE_CAST( metrics_all_conversions AS FLOAT64) all_conversions,
    SAFE_CAST( metrics_all_conversions_value AS FLOAT64) all_conversions_value,
    SAFE_CAST( metrics_client_account_conversions AS FLOAT64 ) adwords_conversions,
    SAFE_CAST( campaign_status AS STRING ) campaign_status,
    SAFE_CAST( customer_engine_id AS STRING ) account_engine_id,
    SAFE_CAST( ad_group_criterion_final_urls AS STRING ) keyword_landing_page,
    SAFE_CAST( metrics_client_account_view_through_conversions AS INT64 ) adwords_view_through_conversions,
    SAFE_CAST( ad_group_criterion_status AS STRING ) status,
    SAFE_CAST( ad_group_criterion_engine_status AS STRING ) engine_status,
    SAFE_CAST( ad_group_name AS STRING ) adgroup,
    SAFE_CAST( metrics_cross_device_conversions AS FLOAT64) cross_device_conversions,
    (SAFE_CAST( metrics_all_conversions AS FLOAT64)
        - SAFE_CAST( metrics_cross_device_conversions AS FLOAT64)) dfa_transactions,
    SAFE_CAST( ad_group_status AS STRING ) adgroup_status,
    SAFE_DIVIDE( SAFE_CAST( metrics_cost_micros AS FLOAT64 ), 1000000) cost,
    SAFE_CAST( campaign_name AS STRING ) campaign,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_cpa_cpc_bid_floor_micros AS FLOAT64), 1000000) AS bidding_strategy_target_cpa_cpc_bid_floor,
    SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_roas_cpc_bid_floor_micros AS FLOAT64), 1000000) AS bidding_strategy_target_roas_cpc_bid_floor,
    LEAST(
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_cpa_cpc_bid_floor_micros AS FLOAT64), 1000000), 0),
        IFNULL( SAFE_DIVIDE( SAFE_CAST( bidding_strategy_target_roas_cpc_bid_floor_micros AS FLOAT64), 1000000), 0)
    ) keyword_min_bid,
    SAFE_CAST( ad_group_id AS STRING ) adgroup_id,
    SAFE_CAST( Ad_Group_Criterion_Keyword_Text AS STRING ) keyword_text,
    SAFE_CAST( metrics_client_account_conversions_value AS FLOAT64 ) adwords_conversion_value,
    SAFE_CAST( customer_id  AS STRING ) account_id,
    SAFE_CAST( metrics_cross_device_conversions_value AS FLOAT64) cross_device_conversions_value,
    (SAFE_CAST( metrics_all_conversions_value AS FLOAT64)
      - SAFE_CAST( metrics_cross_device_conversions_value AS FLOAT64)) dfa_revenue,
    SAFE_CAST( ad_group_criterion_engine_id	 AS STRING ) keyword_engine_id,
    SAFE_DIVIDE( SAFE_CAST( ad_group_criterion_cpc_bid_micros AS FLOAT64 ), 1000000) keyword_max_cpc,
    SAFE_CAST( customer_descriptive_name  AS STRING ) account_name,
    SAFE_CAST( ad_group_criterion_criterion_id AS STRING ) keyword_id,
    SAFE_CAST( metrics_impressions AS FLOAT64 ) impressions,
    SAFE_CAST( ad_group_criterion_final_url_suffix AS STRING ) keyword_url_params,
    SAFE_CAST( campaign_advertising_channel_type AS STRING) as campaign_advertising_channel_type,
    SAFE_CAST( campaign_advertising_channel_sub_type AS STRING) as campaign_advertising_channel_sub_type,
    SAFE_CAST( customer_manager as BOOL) as customer_manager,
    SAFE_CAST(TRIM(customer_currency_code)AS STRING ) statistics_currency_code,
    'sa360-keyword_performance-v1' AS raw_origin,
    SAFE_DIVIDE(SAFE_DIVIDE(SAFE_CAST(metrics_cost_micros AS FLOAT64), 1000000), exchange_source.ex_rate) _gbp_cost,
    ((SAFE_CAST( metrics_all_conversions_value AS FLOAT64)
      - SAFE_CAST( metrics_cross_device_conversions_value AS FLOAT64)) / exchange_source.ex_rate) _gbp_revenue,
/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
    {{ add_fields("Campaign_Name") }} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and
openexchange table is being referred as exchange_source */

    {{ source('sa360', 'keyword_performance_v1')}} source_a

LEFT JOIN exchange_source


ON

    SAFE_CAST(source_a.segments_date AS DATE) = exchange_source.day
/* Jinja var if default field has null value.Replace the default field based on the report */
    AND LOWER(IFNULL(TRIM(Customer_Currency_Code),'{{ var('account_currency') }}')) = exchange_source.currency_code
