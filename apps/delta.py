import pyspark

spark = pyspark.sql.SparkSession.builder.appName("Test Delta App") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .getOrCreate()

spark\
    .read.format('delta').load("s3a://odd-spark-delta-test/another_covid_cases__vaccination_delta") \
    .write.format("delta").mode("append").save("s3a://odd-spark-delta-test/another_covid_cases__vaccination_delta_2")
