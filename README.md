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
gradle build
```

`spark.odd.host.url` used to specify ODD platform host url.

## To run in docker

`docker build -t cluster-apache-spark:3.0.2 .`

`docker-compose up -d`

To start ETL job using OddAdapterSparkListener
```sh
./spark-submit \
--master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar,/opt/spark-apps/mysql-connector-java-8.0.26.jar,/opt/spark-apps/odd-spark-adapter-0.0.1.jar \
--driver-memory 1G --executor-memory 1G \
--conf spark.extraListeners=org.opendatadiscovery.adapters.spark.OddAdapterSparkListener \
--conf spark.odd.host.url=http://host.docker.internal:8080  \
/opt/spark-apps/mysql_pg_job.py
```
To start ETL job using SparkAgent
```sh
./spark-submit \
--master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
--jars /opt/spark-apps/postgresql-42.2.22.jar,/opt/spark-apps/mysql-connector-java-8.0.26.jar \
--driver-java-options "-javaagent:/opt/spark-apps/odd-spark-adapter-0.0.1.jar=http://host.docker.internal:8080" \
/opt/spark-apps/mysql_pg_job.py
```

```sh
./spark-submit \
--master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
--driver-java-options "-javaagent:/opt/spark-apps/odd-spark-adapter-0.0.1.jar=http://host.docker.internal:8080" \
/opt/spark-apps/word_count.py
```

```sh
./spark-submit \
--master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
--driver-java-options "-javaagent:/opt/spark-apps/odd-spark-adapter-0.0.1.jar=http://host.docker.internal:8080" \
--jars /opt/spark-apps/aws-java-sdk-bundle-1.11.874.jar,/opt/spark-apps/hadoop-aws-3.2.0.jar \
/opt/spark-apps/s3-aws-word-count.py
```

To start with AWS Glue job
```sh
aws glue start-job-run \
--job-name odd-spark-adapter-test \
--profile odd \
--region eu-central-1 \
--arguments='--extra-jars="s3://spark-adapter-test/jars/odd-spark-adapter-0.0.1.jar",
  --conf="spark.extraListeners=org.opendatadiscovery.adapters.spark.OddAdapterSparkListener",
  --odd.host.url="http://host.docker.internal:8080"'
```