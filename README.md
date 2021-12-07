# odd-spark-adapter
ODD Spark Adapter

# Implementation notes

Implementation inspired by
[Marquez Spark integration](https://github.com/MarquezProject/marquez/tree/main/integrations/spark).

With listener only, it will not be able to track job outputs.

# Roadmap

Further development may include:
* Features
  * Support for capturing meaningful information for more inputs/outputs 
    types Spark has support for, e.g.:
    * Kafka
    * HBase
    * Hudi tables
    * etc.
  * Making sure different ways to start Spark jobs are supported 
    and examples documented:
    * spark-submit
    * with pyspark
    * with spark-shell (Scala)
    * from Scala, from Python, from Java
    * etc.
* Technical excellence improvement 
  * Unit tests

# Development

## Build
```sh
mvn clean package -dskipTests
```

`spark.odd.host.url` used to specify ODD platform host url.

## To run in docker

`docker build -t cluster-apache-spark:3.0.2 .`

`docker-compose up -d`

To start ETL job
```sh
./spark-submit \
--master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar,/opt/spark-apps/mysql-connector-java-8.0.26.jar,/opt/spark-apps/odd-spark-adapter-0.0.1-SNAPSHOT.jar \
--driver-memory 1G --executor-memory 1G \
--conf spark.extraListeners=org.opendatadiscovery.adapters.spark.OddAdapterSparkListener \
--conf spark.odd.host.url=http://host.docker.internal:8080  \
/opt/spark-apps/mysql_pg_job.py
```