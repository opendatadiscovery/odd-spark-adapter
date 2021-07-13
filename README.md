# odd-spark-adapter
ODD Spark Adapter

# Implementation notes

Implementation inspired by
[Marquez Spark integration](https://github.com/MarquezProject/marquez/tree/main/integrations/spark).

Both spark listener and java agent are currently needed to capture everything. 
With listener only, it will not be able to track job outputs.

# Roadmap

Further development may include:
* Features
  * Support Spark SQL
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
  * Use `CamelCase` for field names for JSON-mapped fields but map to `snake_case`
    when sending request (perhaps, having several different versions of Jackson
    in dependencies/transitive dependencies prevents it functioning now).
* Probably better design of `oddrn`s, ways to represent inputs/outputs etc.
  * Every time job is submitted to Spark, there is both JOB and JOB_RUN
  * Make sure data gathered is as meaningful in a long term as possible vs 
    only meaningful in a short time after when job is run  

# Development

## Build
```sh
mvn clean package -dskipTests
```
This creates `agent.jar` and `example.jar`. 

## Run examples

```sh
spark-submit --class com.provectus.odd.adapters.spark.examples.WordCount \
--master "local[2]" \
--driver-java-options "-javaagent:target/agent.jar" \
--jars target/agent.jar target/example.jar \
pom.xml target/words.txt
```

This example shows client configuration:
```sh
spark-submit \
--class com.provectus.odd.adapters.spark.examples.Pi \
--master "local[2]" \
--driver-java-options "-javaagent:target/agent.jar" \
--conf opendatadiscovery.endpoint=http://localhost:8080/ingestion/entities \
--jars target/agent.jar target/example.jar \
10 target/pi_out
```

`opendatadiscovery.endpoint` used to specify endpoint to 
report DataProcessor/DataProcessorRun to platform.
