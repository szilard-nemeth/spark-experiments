docker exec -it $(docker ps -qf "name=spark-master") \
spark-submit /app/spark_load.py