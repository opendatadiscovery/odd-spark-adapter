from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Word_count").getOrCreate()
spark.sparkContext.setLogLevel('info')

rdd = spark.sparkContext.textFile("/opt/spark-data/data.txt")
result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
result.saveAsTextFile("/opt/spark-data/wc/result")
# for word in result.collect():
#    print("%s: %s" %(word[0], word[1]))
