from pyspark.sql import SparkSession
import os
import uuid

spark = SparkSession \
    .builder \
    .appName("s3_job") \
    .config("spark.jars", "/opt/spark-apps/aws-java-sdk-bundle-1.11.874.jar") \
    .config("spark.jars", "/opt/spark-apps/hadoop-aws-3.2.0.jar") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")

spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://192.168.80.2:9000")
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")

#df = spark.read.text("s3a://test/data.txt")
#df.show()

rdd = spark.sparkContext.textFile("s3a://test/data.txt")
result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
result.saveAsTextFile(f"/opt/spark-data/wc/result/{uuid.uuid4()}")

