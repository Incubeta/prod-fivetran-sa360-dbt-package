{{ config(enabled=var('sa360_adgroup_performance_v2_enabled', True))}}

{{
  config(
    alias=var('sa360_adgroup_performance_alias','sa360-adgroup_performance-v2')
  )
}}

SELECT
    segments_date as  day, 
NULL as Customer_Sub_manager_descriptive_name ,
NULL as Customer_Sub_manager_ID ,
metrics_clicks as Clicks ,
metrics_cost_micros as Cost_Micros ,
customer_descriptive_name as Customer_Descriptive_Name ,
customer_id as Customer_Id ,
campaign_name as Campaign_Name ,
campaign_id as Campaign_Id ,
campaign_status as Campaign_Status ,
metrics_client_account_conversions as Client_Account_Conversions ,
metrics_top_impression_percentage as Top_Impression_Percentage ,
metrics_client_account_view_through_conversions as Client_Account_View_Through_Conversions ,
metrics_absolute_top_impression_percentage as Absolute_Top_Impression_Percentage ,
metrics_client_account_conversions_value as Client_Account_Conversions_Value ,
metrics_search_absolute_top_impression_share as	Search_Absolute_Top_Impression_Share	,
metrics_search_impression_share as Search_Impression_Share ,
metrics_search_top_impression_share as Search_Top_Impression_Share ,
metrics_impressions as Impressions ,
ad_group_name as Ad_Group_Name ,
ad_group_name as Ad_Group_Name ,
ad_group_id as Ad_Group_Id ,
ad_group_status as Ad_Group_Status ,
ad_group_start_date as Ad_Group_Start_Date ,
ad_group_end_date as Ad_Group_End_Date ,
customer_engine_id as  Customer_Engine_Id  ,
ad_group_engine_id as  Ad_Group_Engine_Id  ,
metrics_all_conversions_value as All_Conversions_Value ,
metrics_cross_device_conversions_value as Cross_Device_Conversions_Value ,
ad_group_type as Ad_Group_Type ,
customer_account_type as Customer_Account_Type ,
segments_device as Device ,
metrics_all_conversions as All_Conversions ,
metrics_cross_device_conversions as Cross_Device_Conversions ,
customer_currency_code as Customer_Currency_Code,


FROM {{source('sa360', 'adgroup_performance_v2')}}
