

{{ config(enabled=var('sa360_ad_performance_basic_v2_enabled', True))}}
{{
  config(
    alias= var('sa360_ad_performance_basic_v2_alias','sa360-ad_performance_basic-v2')
  )
}}




SELECT 
    segments_day as day,
customer_descriptive_name as  Customer_Descriptive_Name  ,
customer_id as  Customer_Id  ,
customer_account_type as  Customer_Account_Type  ,
ad_group_ad_name as  Ad_Group_Ad_Name  ,
ad_group_name as Ad_Group_Name ,
ad_group_id as Ad_Group_Id ,
ad_group_ad_id as  Ad_Group_Ad_Id  ,
metrics_client_account_conversions_value as Client_Account_Conversions_Value ,
metrics_client_account_conversions as Client_Account_Conversions ,
metrics_client_account_view_through_conversions as Client_Account_View_Through_Conversions ,
NULL as Customer_Sub_manager_descriptive_name,
NULL as Customer_Sub_manager_ID,
campaign_id as	Campaign_Id	,
campaign_name as Campaign_Name ,
campaign_status as Campaign_Status ,
metrics_clicks as Clicks ,
metrics_cost_micros as Cost_Micros ,
segments_device as Device,
metrics_all_conversions_value as All_Conversions_Value ,
metrics_cross_device_conversions_value as Cross_Device_Conversions_Value ,
metrics_all_conversions as All_Conversions ,
metrics_cross_device_conversions as Cross_Device_Conversions ,
metrics_impressions as Impressions ,
customer_currency_code as Customer_Currency_Code,

FROM {{source('sa360', 'ad_performance_basic_v2')}}
