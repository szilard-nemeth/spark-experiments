# spark-experiments

## Integrated tools

- Spark inventory: https://github.com/szilard-nemeth/Spark_Inventory_API.git
- Spark profiler: https://github.infra.cloudera.com/snemeth/spark-profiler.git
- PvC-DS-Sizing-v2: https://github.infra.cloudera.com/snemeth/PvC-DS-Sizing-v2

Any of the tools can be requested to run, a single tool at once or `all` to execute all of them at once.
Available tools are: `inventory`, `profiler` or `all`.
```shell
python start-tools.py
usage: start-tools.py [-h] [--pull] {inventory,profiler,cdp-monitor-pull,all}
```

Examples 
```shell
# Run all tools
python start-tools.py all

# Run all tools, pulling the repo first
python start-tools.py all --pull

# Run the tool 'inventory', pulling the repo first
python start-tools.py inventory --pull

# Run the tool 'profiler'
python start-tools.py profiler

# Run the tool 'profiler' with HDFS config for input files
python start-tools.py profiler --config config_hdfs.ini

# Run the tool 'cdp-monitor-pull'
python start-tools.py cdp-monitor-pull
```

### I/O HDFS compatibility of tools

#### Spark inventory 
Input: SHS API
Output: print to the console, no configuration for output file yet!


#### Spark profiler

Input: Spark event log dir with event JSON files, either local dir or HDFS dir with configurable location.

Output: 
- CSV file exported to specified local dir (local mode)
- CSV file exported to specified HDFS dir (yarn mode)

#### CDP Monitor (pull)
Input: YARN API, YARN pool data
Output: Local files for output data, with configurable output dir.



## Setup on local machine

## Build and start Spark services on local machine + Verification
```shell
./build-and-start.sh && python3 check-spark-ui.py
```

## Build and start Spark services on local machine + Submit jobs
```shell
./build-and-start.sh && python3 check-spark-ui.py && ./submit-job.sh
```

You can access the SHS here: http://localhost:18080


### Docker setup
The Dockerfile uses the official Apache Spark image.
The `docker-compose.yml` declares a Spark master, Spark worker and Spark SHS.

### Helper script: `breakdown_spark_event_log.py`
Breaks down Spark event logs into individual formatted JSON files.
Note: This is for demonstration purposes only, so we can see the individual event JSONs easier and separated from each other, by event type.

```shell
SRC=/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/sample_spark_event_files/app-20251217040542-0000-b76071b9
DEST=/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/sample_spark_events/breakdown/app-20251217040542-0000-b76071b9
python3 breakdown_spark_event_log.py $SRC $DEST
```

### Helper script: `check-spark-ui.py`
Checks if the Spark UI is accessible via http://localhost:8080


### Helper script: `spark_load.py`
A simple Spark job that generates some CPU load.