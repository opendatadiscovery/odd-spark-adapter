from pyspark.sql import SparkSession

appName = "simple-delta-lake"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.extraListeners", "org.opendatadiscovery.adapters.spark.ODDSparkListener") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_user") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .getOrCreate()

source_url = "jdbc:mysql://source-db:3306/mta_data"
source_properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

query = '(select * from mta_reports r left join vehicle v on r.vehicle_id = v.id) mta_rep_view'

df_delta_read = spark.read.format("delta") \
    .load('s3a://deltabucket/testdeltatable1') \
    .selectExpr("vehicle_id")

df = spark.read \
    .jdbc(url=source_url, table=query, properties=source_properties) \
    .where("latitude <= 80 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
    .where("latitude != 0.000000 OR longitude !=  0.000000 ") \
    .where("report_hour IS NOT NULL") \
    .drop("report_date") \
    .drop("id") \
    .drop("vehicle_id")

df \
    .join(df_delta_read, df.latitude == df_delta_read.vehicle_id, "left") \
    .write \
    .format("delta") \
    .mode('overwrite') \
    .save("s3a://deltabucket/testdeltatable1")
