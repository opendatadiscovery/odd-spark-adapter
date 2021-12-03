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
  * More unit tests
* Probably better design of `oddrn`s, ways to represent inputs/outputs etc.
  * Every time job is submitted to Spark, there is both JOB and JOB_RUN
  * Make sure data gathered is as meaningful in a long term as possible vs 
    only meaningful in a short time after when job is run  

# Development

## Build
```sh
mvn clean package -dskipTests
```
This creates `OddAdapterSparkListener.jar`.

`odd.endpoint` used to specify endpoint to 
report DataProcessor/DataProcessorRun to platform.

## To run in docker

`docker build -t cluster-apache-spark:3.0.2 .`

`docker-compose up -d`

To start ETL job
```sh
./spark-submit \
--master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar,/opt/spark-apps/mysql-connector-java-8.0.26.jar,/opt/spark-apps/odd-spark-adapter-0.0.1-SNAPSHOT.jar \
--driver-memory 1G --executor-memory 1G \
--conf spark.extraListeners=com.provectus.odd.adapters.spark.OddAdapterSparkListener \
--conf spark.odd.host.url=http://host.docker.internal:8080  \
/opt/spark-apps/mysql_pg_job.py
```