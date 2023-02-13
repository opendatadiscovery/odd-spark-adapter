# ODD Spark Adapter
## Introduction
ODD Spark Adapter is a Spark Listener designed to send metadata and dependencies of Spark jobs to platforms that are based on OpenDataDiscovery specification.

To learn more about OpenDataDiscovery and ODD Platform, please refer to project's [landing](https://opendatadiscovery.org/) and [documentation](https://docs.opendatadiscovery.org/) pages.

## Setting up the ODD Spark adapter
ODD Spark Adapter is essentially a simple [Spark Listener](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/scheduler/SparkListener.html)
designed to gather metadata and inputs/outputs of Spark jobs and send it to the ODD Platform or any ODD based backend.