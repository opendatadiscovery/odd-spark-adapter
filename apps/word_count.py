from pyspark.sql import SparkSession
import uuid

spark = SparkSession.builder.appName("Word_count").getOrCreate()
spark.sparkContext.setLogLevel('info')

rdd = spark.sparkContext.textFile("hdfs://namenode:9000/user/root/data.txt")
result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
result.saveAsTextFile(f"hdfs://namenode:9000/user/root/wc/result/{uuid.uuid4()}")
# for word in result.collect():
#    print("%s: %s" %(word[0], word[1]))
