
{{ config(enabled=var('sa360_campaign_performance_v2_enabled', True))}}
{{
  config(
    alias= var('sa360_campaign_performance_v2_alias','sa360-campaign_performance-v2')
  )
}}

SELECT
SAFE_CAST(segments_date as STRING) as date,
SAFE_CAST(customer_engine_id as STRING) as Customer_Engine_Id, 
SAFE_CAST(customer_descriptive_name as STRING) as Customer_Descriptive_Name,
SAFE_CAST(customer_id as STRING) as Customer_Id,
SAFE_CAST(campaign_network_settings_target_search_network as STRING)  as Campaign_Network_Settings_Target_Search_Network ,
SAFE_CAST(campaign_network_settings_target_content_network as STRING) as Campaign_Network_Settings_Target_Content_Network ,
SAFE_CAST(campaign_network_settings_target_google_search as STRING) as Campaign_Network_Settings_Target_Google_Search ,
SAFE_CAST(campaign_network_settings_target_partner_search_network as STRING) as Campaign_Network_Settings_Target_Partner_Search_Network ,
SAFE_CAST(segments_device as STRING) as Device,
SAFE_CAST(campaign_engine_id as STRING) as Campaign_Engine_Id ,
SAFE_CAST(metrics_client_account_conversions as STRING) as Client_Account_Conversions ,
SAFE_CAST(campaign_name as STRING) as Campaign_Name ,
SAFE_CAST(campaign_id as STRING) as Campaign_Id ,
SAFE_CAST(metrics_top_impression_percentage as STRING) as Top_Impression_Percentage ,
SAFE_CAST(NULL as STRING) as Customer_Sub_manager_ID,
SAFE_CAST(metrics_absolute_top_impression_percentage as STRING) as Absolute_Top_Impression_Percentage ,
SAFE_CAST(metrics_conversions as STRING) as Conversions ,
SAFE_CAST(metrics_conversions_value as STRING) as Conversions_Value ,
SAFE_CAST(metrics_all_conversions as STRING) as All_Conversions ,
SAFE_CAST(metrics_cross_device_conversions as STRING) as Cross_Device_Conversions ,
SAFE_CAST(campaign_advertising_channel_type as STRING) as Campaign_Advertising_Channel_Type ,
SAFE_CAST(campaign_bidding_strategy_type as STRING) as Campaign_Bidding_Strategy_Type ,
SAFE_CAST(NULL as STRING) as Customer_Sub_manager_descriptive_name, 
SAFE_CAST(metrics_client_account_conversions_value as STRING) as Client_Account_Conversions_Value ,
SAFE_CAST(customer_account_type as STRING) as Customer_Account_Type ,
SAFE_CAST(metrics_client_account_view_through_conversions as STRING) as Client_Account_View_Through_Conversions ,
SAFE_CAST(campaign_start_date as STRING) as Campaign_Start_Date ,
SAFE_CAST(campaign_end_date as STRING) as Campaign_End_Date ,
SAFE_CAST(metrics_search_absolute_top_impression_share as STRING) as Search_Absolute_Top_Impression_Share ,
SAFE_CAST(metrics_search_top_impression_share as STRING) as Search_Top_Impression_Share ,
SAFE_CAST(campaign_status as STRING) as Campaign_Status ,
SAFE_CAST(metrics_all_conversions_value as STRING) as All_Conversions_Value ,
SAFE_CAST(metrics_cross_device_conversions_value as STRING) as Cross_Device_Conversions_Value ,
SAFE_CAST(metrics_clicks as STRING) as Clicks ,
SAFE_CAST(metrics_cost_micros as STRING) as Cost_Micros ,
SAFE_CAST(metrics_impressions as STRING) as Impressions ,
SAFE_CAST(metrics_search_impression_share as STRING) as Search_Impression_Share ,
SAFE_CAST(customer_currency_code as STRING) as Customer_CurrencyCode,

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM {{ source('sa360', 'campaign_performance_v2')}}
