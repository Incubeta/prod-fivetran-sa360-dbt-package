# Fivetran Search Ads 360 dbt package

## What does this dbt package do?
* Materializes the Search Ads 360 RAW_main tables using the data coming for the Search Ads 360 V2 API.

## How do I use the dbt package?
### Step 1: Prerequisites
To use this dby package, you must have the following:
- At least one Fivetran Search Ads 360 connector syncing data for at least one of the custom reports:
    - sa360-ad_performance_basic-v2
    - sa360-adgroup_performance-v2
    - sa360-campaign_performance-v2
    - sa360-keyword_performance-v2
- A BigQuery data destination

### Step 2: Install the package
Include the following sa_360 package version in your `packages.yml` file

### Step 3: Define input tables variables
This package reads the sa360 data from the different tables created by the sa360 connector. The names of the tables can be changed by setting the correct name in the root `dbt_project.yml` file.

The following table shows the configuration keys and the default table names:

|key|default|
|---|-------|
|sa360_campaign_performance_identifier|sa_360_campaign_performance_v_2|
|sa360_adgroup_performance_identifier|sa_360_adgroup_performance_v_2|
|sa360_keyword_performance_identifier|sa_360_keyword_performance_v_2|
|sa360_ad_performance_basic_identifier|sa_360_ad_performance_basic_v_2|

If the connector uses different table names (for example sa360_campaign_performance_v2) this can be set in the `dbt_project.yml` as follows.

```yaml
vars:
    sa360_campaign_performance_identifier: sa360_campaign_performance_v2 
```

### (Optional) Step 4: Additional configurations

#### Disable reports:
Individual reports can be disabled in the `dbt_project.yml` file.

```yaml
vars:
    sa360_campaign_performance_enabled: false # default true
    sa360_adgroup_performance_enabled: false # default true
    sa360_keyword_performance_enabled: false # default true
    sa360_ad_performance_basic_enabled: false # default true
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
