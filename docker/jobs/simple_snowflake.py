from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("simple-snowflake") \
    .config("spark.extraListeners", "org.opendatadiscovery.adapters.spark.ODDSparkListener") \
    .getOrCreate()

source_url = "jdbc:mysql://source-db:3306/mta_data"
source_properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

query = '(select vehicle_id from mta_reports r left join vehicle v on r.vehicle_id = v.id) mta_rep_view'

df_mysql = spark.read.jdbc(url=source_url, table=query, properties=source_properties)

sfReadOptions = {
    "sfURL": "mwlgidp-znb56211.snowflakecomputing.com",
    "sfUser": "ndementev",
    "sfPassword": "rTv83600xc!6052",
    "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
    "sfSchema": "TPCDS_SF100TCL",
    "sfWarehouse": "COMPUTE_WH"
}

sfWriteOptions = {
    "sfURL": "mwlgidp-znb56211.snowflakecomputing.com",
    "sfUser": "ndementev",
    "sfPassword": "rTv83600xc!6052",
    "sfDatabase": "TEST_DATABASE",
    "sfSchema": "TEST_SCHEMA",
    "sfWarehouse": "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfReadOptions) \
    .option("query", "select CC_CALL_CENTER_SK, CC_CLASS from CALL_CENTER") \
    .load() \
    .where("CC_CALL_CENTER_SK <= 10")

df \
    .join(df_mysql, df.CC_CALL_CENTER_SK == df_mysql.vehicle_id, "left") \
    .drop("vehicle_id") \
    .write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfWriteOptions) \
    .option("dbtable", "TEST_TABLE") \
    .mode('overwrite') \
    .save()
