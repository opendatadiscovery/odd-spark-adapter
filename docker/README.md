# ODD Spark Adapter local demo project

This project demonstrates how to run the ODD Spark Adapter within an Apache Spark cluster.

To get started, you can clone this repository and follow the instructions in the README file. The README provides a step-by-step guide on how to set up the project and run it within your Apache Spark cluster. Once you have everything set up, you can begin gathering the metadata using the ODD Spark Adapter.

## Prerequisites:
* Docker Engine 19.03.0+
* the latest Docker Compose
* Java 8+ [Optionally]

## Steps
1. Clone the ODD Spark Adapter repository:
```bash
git clone https://github.com/opendatadiscovery/odd-spark-adapter
```
2. Navigate to the cloned repository:
```bash
cd odd-spark-adapter
```
3. You can either download the already compiled JAR file or build it from scratch
   *  To download a JAR file, go to the Releases page and download the JAR file for the latest release. Rename it to `odd-spark-adapter-local.jar` and put it into the `build/libs` folder of the cloned repository. If the `build/libs` folder does not exist, create it first.
   * To build from scratch, install Java 8+ and execute the following command
    ```bash
    ./gradlew build -Pversion=local
    ```
4. Create a docker network
```bash
docker network create spark
```
5. Run the ODD Platform as a target OpenDataDiscovery backend for ODD Spark Adapter
```bash
docker-compose -f docker/odd-platform.yaml up -d
```
6. Create an ODDRN key and a data source with correspondent ODDRN:
   * The ODDRN key is a string that must uniquely define the Spark cluster. For the purpose of this tutorial, the string "local" is used.
   * To create a data source with a corresponding ODDRN, follow these steps:
      1. Navigate to `http://localhost:8080/management/datasources`.
      2. Click on "Add datasource".
      3. Fill out the name and ODDRN. Use `//spark/host/<oddrn_key>` as the format for the ODDRN, so that it will be `//spark/host/local`.
7. Run the Spark cluster using the following command:
```bash
docker-compose -f docker/spark.yaml up -d
```
8. Run local JDBC sources
```bash
docker-compose -f docker/jdbc-databases.yaml up -d
```
9. Run a Spark job
```bash
docker exec -it spark /usr/local/spark-3.3.1-bin-hadoop3/bin/spark-submit \
		--jars /odd-jar/odd-spark-adapter-local.jar \
		--conf "spark.odd.host.url=http://odd-platform:8080" \
		--conf "spark.odd.oddrn.key=local" \
		--packages org.postgresql:postgresql:42.2.22,mysql:mysql-connector-java:8.0.26 \
		/jobs/simple-jdbc.py
```

## Result
As a result, ODD Platform should collect and save Data Transformer with JDBC databases as inputs and outputs. 