# ODD Spark Adapter

## Introduction
ODD Spark Adapter is a Spark Listener designed to send metadata and dependencies of Spark 3.3.1 jobs to platforms that are based on OpenDataDiscovery specification.

To learn more about OpenDataDiscovery and ODD Platform, please refer to project's [landing](https://opendatadiscovery.org/) and [documentation](https://docs.opendatadiscovery.org/) pages.

## Supported data sources
ODD Spark adapter v0.0.1 supports:
1. RDD low level jobs
2. Read/write from/to JDBC data sources
3. Read/write from/to Kafka topics (batch only)
4. Read/write from/to Snowflake tables
5. Read/write from/to S3 Delta tables

## Limitations
1. As of now ODD Spark adapter doesn't support Spark structured streaming (in roadmap)
2. As of now ODD Spark adapter doesn't support Spark structured streaming (in roadmap)
3. As of now ODD Spark adapter supports Spark 3.3.1 only (in roadmap)

## Setting up the ODD Spark adapter

ODD Spark Adapter is essentially a simple [Spark Listener](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/scheduler/SparkListener.html)
designed to gather metadata and inputs/outputs of Spark jobs and send it to the ODD Platform or any ODD based backend.

### Download listener JAR
Available JAR files can be found in [Releases](https://github.com/opendatadiscovery/odd-spark-adapter/releases)

### Configuration
1. `spark.odd.host.url` — URL of [ODD Platform](https://github.com/opendatadiscovery/odd-platform) deployment
2. `spark.odd.oddrn.key` — Unique identifier of Spark cluster. Can be any string that uniquely defines target Spark cluster in the scope of user's data infrastructure.

### Example of running Spark job with ODD Spark adapter
```bash
./spark-submit \
    --packages <needed packages for the Spark jobs> \
    --jars <path to the ODD Spark adapter JAR> \
    --conf "spark.odd.host.url=http://odd-platform:8080" \
    --conf "spark.odd.oddrn.key=unique_spark_cluster_key" \
    /jobs/simple-delta-lake.py
```