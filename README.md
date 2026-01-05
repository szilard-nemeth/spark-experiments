# spark-experiments

Access the SHS with: http://localhost:18080

## Build and start Spark services on local machine + Verification
```shell
./build-and-start.sh && python3 check-spark-ui.py
```

## Build and start Spark services on local machine + Submit jobs
```shell
./build-and-start.sh && python3 check-spark-ui.py && ./submit-job.sh
```

## Break down Spark event logs for individual files (formatted JSONs)
```shell
SRC=/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/sample_spark_event_files/app-20251217040542-0000-b76071b9
DEST=/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/sample_spark_events/breakdown/app-20251217040542-0000-b76071b9
python3 breakdown_spark_event_log.py $SRC $DEST
```

## Start integrated tools
1. Spark inventory
https://github.com/szilard-nemeth/Spark_Inventory_API.git

2. Spark profiler
https://github.infra.cloudera.com/snemeth/spark-profiler.git


```shell
python start-tools.py
```
