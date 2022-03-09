echo "Copying spark.."
mkdir "spark"
docker cp spark_master:/opt/bitnami/spark/bin ./spark/bin/
docker cp spark_master:/opt/bitnami/spark/conf ./spark/
docker cp spark_master:/opt/bitnami/spark/jars ./spark/
docker cp spark_master:/opt/bitnami/spark/python ./spark/
docker cp spark_master:/opt/bitnami/spark/sbin ./spark/