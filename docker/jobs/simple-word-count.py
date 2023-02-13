from pyspark.sql import SparkSession
import uuid

spark = SparkSession \
    .builder \
    .appName("word-count") \
    .config("spark.extraListeners", "org.opendatadiscovery.adapters.spark.ODDSparkListener") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_user") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .getOrCreate()

rdd = spark.sparkContext.textFile("s3a://wordcount/words.txt")
result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
result.saveAsTextFile(f"/home/jovyan/test-spark-results/wordcount/result/{uuid.uuid4()}")