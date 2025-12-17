docker exec -it spark-experiments-spark-master-1 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark/events \
  /app/spark_load.py