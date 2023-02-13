from pyspark.sql import SparkSession


def init_spark():
    return SparkSession.builder \
        .appName("simple-jdbc-etl-app") \
        .config("spark.extraListeners", "org.opendatadiscovery.adapters.spark.ODDSparkListener") \
        .getOrCreate()


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

    spark = init_spark()

    query = '(select * from mta_reports r left join vehicle v on r.vehicle_id = v.id) mta_rep_view'

    spark.read \
        .jdbc(url=source_url, table=query, properties=source_properties) \
        .where("latitude <= 80 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
        .where("latitude != 0.000000 OR longitude !=  0.000000 ") \
        .where("report_hour IS NOT NULL") \
        .drop("report_date") \
        .drop("id") \
        .write \
        .jdbc(url=target_url, table='mta_transform', mode='overwrite', properties=target_properties)


if __name__ == '__main__':
    main()
