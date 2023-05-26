CREATE TABLE IF NOT EXISTS programmatic_stg(
adv_ssp  string
,adv_deal   string
,advertiser   string
,country   string
,device_category   string
,adv_agency   string
,adv_property   string
,marketplace   string
,integration_type_id     bigint
,monetization_channel_id bigint
,ad_unit_id              bigint
,total_impressions       bigint
,total_revenue           DOUBLE
,viewable_impressions    bigint
,measurable_impressions  bigint
,revenue_share_percent    DOUBLE
,load_time timestamp)
PARTITIONED BY (
filedate int
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS ssp_dim(
ssp_id bigint,
ssp_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;

CREATE TABLE IF NOT EXISTS deal_dim(
deal_id bigint,
deal_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS advertiser_dim(
advertiser_id bigint,
advertiser_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS country_dim(
country_id bigint,
country_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS device_category_dim(
device_category_id bigint,
device_category_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS agency_dim(
agency_id bigint,
agency_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS property_dim(
property_id bigint,
property_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;
CREATE TABLE IF NOT EXISTS marketplace_dim(
marketplace_id bigint,
marketplace_name  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET;