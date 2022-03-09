from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Word count").getOrCreate()
spark.sparkContext.setLogLevel('info')

hdfs_host = 'localhost:9010'

rdd = spark.sparkContext.textFile(f"hdfs://{hdfs_host}/user/root/data.txt")
result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
result.saveAsTextFile(f"hdfs://{hdfs_host}/user/root/result/1")

spark.stop()
