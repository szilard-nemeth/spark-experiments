docker exec -it $(docker ps -qf "name=spark-master") \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark/events \
  /app/spark_load.py