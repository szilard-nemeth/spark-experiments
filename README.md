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
- Spark inventory: https://github.com/szilard-nemeth/Spark_Inventory_API.git
- Spark profiler: https://github.infra.cloudera.com/snemeth/spark-profiler.git


Any of the tools can be requested to run, a single tool at once or `all` to execute all of them at once.
Available tools are: `inventory`, `profiler` or `all`.
```shell
python start-tools.py
usage: start-tools.py [-h] [--pull] {inventory,profiler,all}
start-tools.py: error: the following arguments are required: tool
```

Examples 
```shell
# Run all tools
python start-tools.py all

# Run all tools, pulling the repo first
python start-tools.py all --pull

# Run the tool 'inventory', pulling the repo first
python start-tools.py inventory --pull

# Run the tool 'profler'
python start-tools.py profiler
```
