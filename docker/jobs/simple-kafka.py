from pyspark.sql import SparkSession

appName = "simple-kafka"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .config("spark.extraListeners", "org.opendatadiscovery.adapters.spark.ODDSparkListener") \
    .appName(appName) \
    .getOrCreate()

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "users, more_users") \
    .load()

df = df \
    .withColumn('key_str', df['key'].cast('string').alias('key_str')) \
    .withColumn('value_str', df['value'].cast('string').alias('value_str'))

df.show(2)

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("topic", "total_users") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .save()
