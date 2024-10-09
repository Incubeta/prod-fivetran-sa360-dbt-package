
{{ config(enabled=var('sa360_campaign_performance_v2_enabled', True))}}
{{
  config(
    alias= 'sa360-campaign_performance-v2'
  )
}}

SELECT
segments_date as date,
customer_engine_id as Customer_Engine_Id, 
customer_descriptive_name as Customer_Descriptive_Name,
customer_id as Customer_Id,
campaign_network_settings_target_search_network  as Campaign_Network_Settings_Target_Search_Network ,
campaign_network_settings_target_content_network as Campaign_Network_Settings_Target_Content_Network ,
campaign_network_settings_target_google_search as Campaign_Network_Settings_Target_Google_Search ,
campaign_network_settings_target_partner_search_network as Campaign_Network_Settings_Target_Partner_Search_Network ,
segments_device as Device,
campaign_engine_id as Campaign_Engine_Id ,
metrics_client_account_conversions as Client_Account_Conversions ,
campaign_name as Campaign_Name ,
campaign_id as Campaign_Id ,
metrics_top_impression_percentage as Top_Impression_Percentage ,
NULL as Customer_Sub_manager_ID,
metrics_absolute_top_impression_percentage as Absolute_Top_Impression_Percentage ,
metrics_conversions as Conversions ,
metrics_conversions_value as Conversions_Value ,
metrics_all_conversions as All_Conversions ,
metrics_cross_device_conversions as Cross_Device_Conversions ,
campaign_advertising_channel_type as Campaign_Advertising_Channel_Type ,
campaign_bidding_strategy_type as Campaign_Bidding_Strategy_Type ,
NULL as Customer_Sub_manager_descriptive_name, 
metrics_client_account_conversions_value as Client_Account_Conversions_Value ,
customer_account_type as Customer_Account_Type ,
metrics_client_account_view_through_conversions as Client_Account_View_Through_Conversions ,
campaign_start_date as Campaign_Start_Date ,
campaign_end_date as Campaign_End_Date ,
metrics_search_absolute_top_impression_share as Search_Absolute_Top_Impression_Share ,
metrics_search_top_impression_share as Search_Top_Impression_Share ,
campaign_status as Campaign_Status ,
metrics_all_conversions_value as All_Conversions_Value ,
metrics_cross_device_conversions_value as Cross_Device_Conversions_Value ,
metrics_clicks as Clicks ,
metrics_cost_micros as Cost_Micros ,
metrics_impressions as Impressions ,
metrics_search_impression_share as Search_Impression_Share ,
customer_currency_code as Customer_CurrencyCode,

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM {{ source('sa360', 'campaign_performance_v2')}}
