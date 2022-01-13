from pyspark.sql import SparkSession
import os

spark = SparkSession \
    .builder \
    .appName("s3_job") \
    .config("spark.jars", "/opt/spark-apps/aws-java-sdk-bundle-1.11.874.jar") \
    .config("spark.jars", "/opt/spark-apps/hadoop-aws-3.2.0.jar") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xxxxx")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xxxxx")

spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")

#df = spark.read.text("s3a://spark-adapter-test-sw/data.txt")
#df.show()

rdd = spark.sparkContext.textFile("s3a://spark-adapter-test-sw/sub/data.txt")
result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
result.saveAsTextFile("/opt/spark-data/wc/result")

