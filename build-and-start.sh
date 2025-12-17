docker compose up --build -d
# verify
docker exec -it $(docker ps -qf "name=spark-master") python3 -c "import pyspark; print('Spark Ready:', pyspark.__version__)"