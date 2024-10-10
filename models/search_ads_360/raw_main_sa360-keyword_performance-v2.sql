
{{ config(enabled=var('keyword_performance_v2_enabled', True))}}
{{
  config(
    alias= 'sa360-keyword_performance-v2'
  )
}}

SELECT
SAFE_CAST(campaign_id as STRING) as Campaign_Id ,
SAFE_CAST(NULL as STRING) as Customer_Sub_manager_descriptive_name,
SAFE_CAST(segments_date as STRING) as date,
SAFE_CAST(segments_device as STRING) as Device,
SAFE_CAST(metrics_clicks as STRING) as Clicks ,
SAFE_CAST(ad_group_criterion_keyword_match_type as STRING) as Ad_Group_Criterion_Keyword_Match_Type ,
SAFE_CAST(bidding_strategy_target_cpa_cpc_bid_ceiling_micros as STRING) as Bidding_Strategy_Target_Cpa_Cpc_Bid_Ceiling_Micros ,
SAFE_CAST(bidding_strategy_target_roas_cpc_bid_ceiling_micros as STRING) as Bidding_Strategy_Target_Roas_Cpc_Bid_Ceiling_Micros ,
SAFE_CAST(bidding_strategy_target_impression_share_cpc_bid_ceiling_micros as STRING) as Bidding_Strategy_Target_Impression_Share_Cpc_Bid_Ceiling_Micros ,
SAFE_CAST(bidding_strategy_target_outrank_share_cpc_bid_ceiling_micros as STRING) as Bidding_Strategy_Target_Outrank_Share_Cpc_Bid_Ceiling_Micros ,
SAFE_CAST(bidding_strategy_target_spend_cpc_bid_ceiling_micros as STRING) as Bidding_Strategy_Target_Spend_Cpc_Bid_Ceiling_Micros ,
SAFE_CAST(customer_account_type as STRING) as Customer_Account_Type ,
SAFE_CAST(metrics_all_conversions as STRING) as All_Conversions ,
SAFE_CAST(metrics_all_conversions_value as STRING) as All_Conversions_Value ,
SAFE_CAST(metrics_client_account_conversions as STRING) as Client_Account_Conversions ,
SAFE_CAST(campaign_status as STRING) as Campaign_Status ,
SAFE_CAST(customer_engine_id as STRING) as Customer_Engine_Id ,
SAFE_CAST(ad_group_criterion_final_urls as STRING) as Ad_Group_Criterion_Final_Urls ,
SAFE_CAST(metrics_client_account_view_through_conversions as STRING) as Client_Account_View_Through_Conversions ,
SAFE_CAST(ad_group_criterion_status as STRING) as Ad_Group_Criterion_Status ,
SAFE_CAST(ad_group_criterion_engine_status as STRING) as Ad_Group_Criterion_Engine_Status ,
SAFE_CAST(ad_group_name as STRING) as Ad_Group_Name ,
SAFE_CAST(metrics_cross_device_conversions as STRING) as Cross_Device_Conversions ,
SAFE_CAST(ad_group_status as STRING) as Ad_Group_Status ,
SAFE_CAST(metrics_cost_micros as STRING) as Cost_Micros ,
SAFE_CAST(campaign_name as STRING) as Campaign_Name ,
SAFE_CAST(bidding_strategy_target_cpa_cpc_bid_floor_micros as STRING) as Bidding_Strategy_Target_Cpa_Cpc_Bid_Floor_Micros ,
SAFE_CAST(bidding_strategy_target_roas_cpc_bid_floor_micros as STRING) as Bidding_Strategy_Target_Roas_Cpc_Bid_Floor_Micros ,
SAFE_CAST(ad_group_id as STRING) as Ad_Group_Id ,
SAFE_CAST(ad_group_criterion_keyword_text as STRING) as Ad_Group_Criterion_Keyword_Text ,
SAFE_CAST(metrics_client_account_conversions_value as STRING) as Client_Account_Conversions_Value ,
SAFE_CAST(customer_id as STRING) as  Customer_Id  ,
SAFE_CAST(metrics_cross_device_conversions_value as STRING) as Cross_Device_Conversions_Value ,
SAFE_CAST(ad_group_criterion_engine_id as STRING) as	 Ad_Group_Criterion_Engine_Id	 ,
SAFE_CAST(ad_group_criterion_cpc_bid_micros as STRING) as Ad_Group_Criterion_Cpc_Bid_Micros ,
SAFE_CAST(customer_descriptive_name as STRING) as  Customer_Descriptive_Name  ,
SAFE_CAST(ad_group_criterion_criterion_id as STRING) as Ad_Group_Criterion_Criterion_Id ,
SAFE_CAST(metrics_impressions as STRING) as Impressions ,
SAFE_CAST(ad_group_criterion_final_url_suffix as STRING) as Ad_Group_Criterion_Final_Url_Suffix ,
SAFE_CAST(campaign_advertising_channel_type as STRING) as Campaign_Advertising_Channel_Type ,
SAFE_CAST(campaign_advertising_channel_sub_type as STRING) as Campaign_Advertising_Channel_Sub_Type ,
SAFE_CAST(customer_manager as STRING) as Customer_Manager ,
SAFE_CAST(customer_currency_code as STRING) as Customer_Currency_Code,

FROM {{ source('sa360', 'keyword_performance_v2')}}

