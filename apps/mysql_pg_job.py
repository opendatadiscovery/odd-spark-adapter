from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format

def init_spark():
  sql = SparkSession.builder\
    .appName("etl-app")\
    .config("spark.jars", "/opt/spark-apps/mysql-connector-java-8.0.26.jar,/opt/spark-apps/postgresql-42.2.22.jar")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql, sc

def main():

  source_url = "jdbc:mysql://source-db:3306/mta_data"
  source_properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
  }

  target_url = "jdbc:postgresql://target-db:5432/mta_data"
  target_properties = {
    "user": "postgres",
    "password": "casa1234",
    "driver": "org.postgresql.Driver"
  }

  sql, sc = init_spark()

  df = sql.read.jdbc(url=source_url, table='mta_reports', properties=source_properties)

  # df.show()
  df.count()
  
  # Filter invalid coordinates and transform
  df.where("latitude <= 90 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
    .where("latitude != 0.000000 OR longitude !=  0.000000 ") \
    .where("report_hour IS NOT NULL") \
    .drop("report_date") \
    .write.jdbc(url=target_url, table='mta_transform', mode='overwrite', properties=target_properties)
  
if __name__ == '__main__':
  main()