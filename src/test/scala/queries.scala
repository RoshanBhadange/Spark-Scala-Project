package test.scala

object queries {

  val stg_sql=  """CREATE TABLE IF NOT EXISTS programmatic_stg(
   ssp  string
  ,deal   string
  ,advertiser   string
  ,country   string
  ,device_category  string
  ,agency   string
  ,property   string
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
  stored AS PARQUET; """
  val ssp_dim = """CREATE TABLE IF NOT EXISTS ssp_dim(
 ssp_id bigint,
 ssp_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  // create other dim tables take reference of ssp_dim creation
  val deal_dim = """CREATE TABLE IF NOT EXISTS deal_dim(
 deal_id bigint,
 deal_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val advertiser_dim = """CREATE TABLE IF NOT EXISTS advertiser_dim(
 advertiser_id bigint,
 advertiser_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val country_dim = """CREATE TABLE IF NOT EXISTS country_dim(
 country_id bigint,
 country_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val device_category_dim = """CREATE TABLE IF NOT EXISTS device_category_dim(
 device_category_id bigint,
 device_category_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val agency_dim = """CREATE TABLE IF NOT EXISTS agency_dim(
 agency_id bigint,
 agency_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val property_dim = """CREATE TABLE IF NOT EXISTS property_dim(
 property_id bigint,
 property_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val marketplace_dim = """CREATE TABLE IF NOT EXISTS marketplace_dim(
 marketplace_id bigint,
 marketplace_desc  string)
PARTITIONED BY (
load_time timestamp
)
stored AS PARQUET; """
  val programmatic_fact = """CREATE TABLE IF NOT EXISTS programmatic_fact(
 ssp_id bigint,
 deal_id bigint,
 advertiser_id bigint,
 country_id bigint,
 device_category_id bigint,
 agency_id bigint,
 property_id bigint,
 marketplace_id bigint,
 integration_type_id     bigint
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
stored AS PARQUET; """
}
