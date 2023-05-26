package test.scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import queries._
import org.apache.spark.sql.{DataFrame, SparkSession}


object performanceTest {

  val spark = SparkSession.builder()
    .master("local[1]")
    .enableHiveSupport()
    .appName("PerformanceSpark")
    .config("hive.exec.dynamic.partition", true)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate()

  val tbl = Seq("ssp","deal","advertiser","country","agency","device_category","property","marketplace")

  def drop_tables() {
    spark.sql("create database if not exists programmatic_rv;")
    spark.sql("use programmatic_rv;")
    spark.sql("drop table programmatic_rv.programmatic_stg")
    spark.sql("drop table programmatic_rv.ssp_dim")
    spark.sql("drop table programmatic_rv.deal_dim")
    spark.sql("drop table programmatic_rv.advertiser_dim")
    spark.sql("drop table programmatic_rv.country_dim")
    spark.sql("drop table programmatic_rv.agency_dim")
    spark.sql("drop table programmatic_rv.property_dim")
    spark.sql("drop table programmatic_rv.device_category_dim")
    spark.sql("drop table programmatic_rv.marketplace_dim")
    spark.sql("drop table programmatic_rv.programmatic_fact")

  }

  def hive_utils() = {
    spark.sql("create database if not exists programmatic_rv;")
    spark.sql("use programmatic_rv;")
    // stage table creation
    spark.sql(stg_sql)

    // dim table creation

    spark.sql(ssp_dim)
    spark.sql(deal_dim)
    spark.sql(advertiser_dim)
    spark.sql(country_dim)
    spark.sql(agency_dim)
    spark.sql(device_category_dim)
    spark.sql(property_dim)
    spark.sql(marketplace_dim)

    // fact table creation

    spark.sql(programmatic_fact)
  }

  def stage_load(): DataFrame ={
    val df = spark.read.option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/roshanbhadange/Documents/FirstSpark/src/main/resources/data/SourceFiles_17-04-2021/*")

    val df_schema = df.select("*")
    .withColumn("filedate", regexp_replace(split(col("date")," ").getItem(0).cast("String"),"/","").cast("int"))
    .withColumn("load_time", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss SSS"))
    .withColumn("revenue_share_percent",col("revenue_share_percent").cast("Double"))
    .withColumn("total_revenue",col("total_revenue").cast("Double"))
    .withColumn("total_impressions",col("total_impressions").cast("bigint"))
    .withColumn("ad_unit_id",col("ad_unit_id").cast("bigint"))
    .withColumn("monetization_channel_id",col("monetization_channel_id").cast("bigint"))
    .withColumn("integration_type_id",col("integration_type_id").cast("long"))
    .withColumn("viewable_impressions",col("viewable_impressions").cast("bigint"))
    .withColumn("measurable_impressions",col("measurable_impressions").cast("bigint"))
    val df_stg = df_schema.select("ssp", "deal", "advertiser", "country", "device_category", "agency", "property", "marketplace","integration_type_id", "monetization_channel_id", "ad_unit_id", "total_impressions", "total_revenue", "viewable_impressions", "measurable_impressions", "revenue_share_percent", "load_time", "filedate" )
    df_stg
  }

  def read_stg(): DataFrame ={
    val df_stg = spark.sql("select * from programmatic_rv.programmatic_stg")
    df_stg.persist()
  }

  def write_hivetables(df: DataFrame, table_name: String)={
    df.write.mode("overwrite").insertInto(table_name)
  }

  def dim_load(tableName: String): DataFrame={
    val df_stg = read_stg()
    val tableNameDf = df_stg.select(tableName).distinct()
    val win = Window.orderBy(col(tableName))
    val tableName_id = tableName+"_id"
    val tableName_dim = tableNameDf.select(tableName).distinct()
    val tableNameDF = tableName_dim.withColumn(tableName_id, dense_rank().over(win))
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
      .withColumnRenamed(tableName,tableName+"_desc")

    val df_tableName = tableNameDF.select(tableName_id,tableName+"_desc","load_date")
    df_tableName
  }

  def fact_load(): DataFrame= {
    val stgtbl = read_stg()
    val ssptbl = spark.sql("select * from programmatic_rv.ssp_dim")
    val dealtbl = spark.sql("select * from programmatic_rv.deal_dim")
    val advtbl = spark.sql("select * from programmatic_rv.advertiser_dim")
    val ctytbl = spark.sql("select * from programmatic_rv.country_dim")
    val agencytbl = spark.sql("select * from programmatic_rv.agency_dim")
    val dctbl = spark.sql("select * from programmatic_rv.device_category_dim")
    val ptytbl = spark.sql("select * from programmatic_rv.property_dim")
    val mkttbl = spark.sql("select * from programmatic_rv.marketplace_dim")


    val final_df =stgtbl.join(ssptbl, stgtbl.col("ssp") === ssptbl.col("ssp_desc") , "left")
    .join(dealtbl, stgtbl.col("deal") === dealtbl.col("deal_desc") , "left")
    .join(advtbl, stgtbl.col("advertiser") === advtbl.col("advertiser_desc"), "left")
    .join(ctytbl, stgtbl.col("country") === ctytbl.col("country_desc"), "left")
    .join(dctbl, stgtbl.col("device_category") === dctbl.col("device_category_desc"), "left")
    .join(agencytbl, stgtbl.col("agency") === agencytbl.col("agency_desc"), "left")
    .join(ptytbl, stgtbl.col("property") === ptytbl.col("property_desc"), "left")
    .join(mkttbl, stgtbl.col("marketplace") === mkttbl.col("marketplace_desc"), "left")
      .withColumn("upd_dttm", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
      .select("ssp_id","deal_id","advertiser_id","country_id","device_category_id","agency_id","property_id","marketplace_id",
        "integration_type_id","monetization_channel_id","ad_unit_id","total_impressions","total_revenue",
        "measurable_impressions","revenue_share_percent","viewable_impressions", "upd_dttm","filedate")
      .distinct()
    final_df
  }

  def main(args: Array[String]): Unit = {
    drop_tables()
    hive_utils()
    write_hivetables(stage_load(), "programmatic_rv.programmatic_stg")
    write_hivetables(dim_load("ssp"), "programmatic_rv.ssp_dim")
    write_hivetables(dim_load("deal"), "programmatic_rv.deal_dim")
    write_hivetables(dim_load("advertiser"), "programmatic_rv.advertiser_dim")
    write_hivetables(dim_load("country"), "programmatic_rv.country_dim")
    write_hivetables(dim_load("agency"), "programmatic_rv.agency_dim")
    write_hivetables(dim_load("device_category"), "programmatic_rv.device_category_dim")
    write_hivetables(dim_load("property"), "programmatic_rv.property_dim")
    write_hivetables(dim_load("marketplace"), "programmatic_rv.marketplace_dim")

    write_hivetables(fact_load(), "programmatic_rv.programmatic_fact")

    spark.sql("select * from programmatic_rv.programmatic_fact").show(10)
  }
}