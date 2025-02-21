
{{ config(enabled=var('sa360_campaign_performance_v2_enabled', True))}}
{{
  config(
    alias= var('sa360_campaign_performance_v2_alias','sa360-campaign_performance-v2'),
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

SELECT
segments_date as date,
{{dbt.safe_cast(customer_engine_id ,api.Column.translate_type('string'))}} as Customer_Engine_Id, 
{{dbt.safe_cast(customer_descriptive_name ,api.Column.translate_type('string'))}} as Customer_Descriptive_Name,
{{dbt.safe_cast(customer_id,api.Column.translate_type('string'))}} as Customer_Id,
{{dbt.safe_cast(campaign_network_settings_target_search_network,api.Column.translate_type('string'))}}  as Campaign_Network_Settings_Target_Search_Network ,
{{dbt.safe_cast(campaign_network_settings_target_content_network,api.Column.translate_type('string'))}} as Campaign_Network_Settings_Target_Content_Network ,
{{dbt.safe_cast(campaign_network_settings_target_google_search,api.Column.translate_type('string'))}} as Campaign_Network_Settings_Target_Google_Search ,
{{dbt.safe_cast(campaign_network_settings_target_partner_search_network,api.Column.translate_type('string'))}} as Campaign_Network_Settings_Target_Partner_Search_Network ,
{{dbt.safe_cast(segments_device,api.Column.translate_type('string'))}} as Device,
{{dbt.safe_cast(campaign_engine_id,api.Column.translate_type('string'))}} as Campaign_Engine_Id ,
{{dbt.safe_cast(metrics_client_account_conversions,api.Column.translate_type('string'))}} as Client_Account_Conversions ,
{{dbt.safe_cast(campaign_name,api.Column.translate_type('string'))}} as Campaign_Name ,
{{dbt.safe_cast(campaign_id,api.Column.translate_type('string'))}} as Campaign_Id ,
{{dbt.safe_cast(metrics_top_impression_percentage,api.Column.translate_type('string'))}} as Top_Impression_Percentage ,
{{dbt.safe_cast(NULL,api.Column.translate_type('string'))}} as Customer_Sub_manager_ID,
{{dbt.safe_cast(metrics_absolute_top_impression_percentage,api.Column.translate_type('string'))}} as Absolute_Top_Impression_Percentage ,
{{dbt.safe_cast(metrics_conversions,api.Column.translate_type('string'))}} as Conversions ,
{{dbt.safe_cast(metrics_conversions_value,api.Column.translate_type('string'))}} as Conversions_Value ,
{{dbt.safe_cast(metrics_all_conversions,api.Column.translate_type('string'))}} as All_Conversions ,
{{dbt.safe_cast(metrics_cross_device_conversions,api.Column.translate_type('string'))}} as Cross_Device_Conversions ,
{{dbt.safe_cast(campaign_advertising_channel_type,api.Column.translate_type('string'))}} as Campaign_Advertising_Channel_Type ,
{{dbt.safe_cast(campaign_bidding_strategy_type,api.Column.translate_type('string'))}} as Campaign_Bidding_Strategy_Type ,
{{dbt.safe_cast(NULL,api.Column.translate_type('string'))}} as Customer_Sub_manager_descriptive_name, 
{{dbt.safe_cast(metrics_client_account_conversions_value,api.Column.translate_type('string'))}} as Client_Account_Conversions_Value ,
{{dbt.safe_cast(customer_account_type,api.Column.translate_type('string'))}} as Customer_Account_Type ,
{{dbt.safe_cast(metrics_client_account_view_through_conversions,api.Column.translate_type('string'))}} as Client_Account_View_Through_Conversions ,
{{dbt.safe_cast(campaign_start_date,api.Column.translate_type('string'))}} as Campaign_Start_Date ,
{{dbt.safe_cast(campaign_end_date,api.Column.translate_type('string'))}} as Campaign_End_Date ,
{{dbt.safe_cast(metrics_search_absolute_top_impression_share,api.Column.translate_type('string'))}} as Search_Absolute_Top_Impression_Share ,
{{dbt.safe_cast(metrics_search_top_impression_share,api.Column.translate_type('string'))}} as Search_Top_Impression_Share ,
{{dbt.safe_cast(campaign_status,api.Column.translate_type('string'))}} as Campaign_Status ,
{{dbt.safe_cast(metrics_all_conversions_value,api.Column.translate_type('string'))}} as All_Conversions_Value ,
{{dbt.safe_cast(metrics_cross_device_conversions_value,api.Column.translate_type('string'))}} as Cross_Device_Conversions_Value ,
{{dbt.safe_cast(metrics_clicks,api.Column.translate_type('string'))}} as Clicks ,
{{dbt.safe_cast(metrics_cost_micros,api.Column.translate_type('string'))}} as Cost_Micros ,
{{dbt.safe_cast(metrics_impressions,api.Column.translate_type('string'))}} as Impressions ,
{{dbt.safe_cast(metrics_search_impression_share,api.Column.translate_type('string'))}} as Search_Impression_Share ,
{{dbt.safe_cast(customer_currency_code,api.Column.translate_type('string'))}} as Customer_CurrencyCode,

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM {{ source('sa360', 'campaign_performance_v2')}}
