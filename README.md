# spark-experiments

## Prerequisites
- Python 3.6+
- Java for PySpark (with `JAVA_HOME` configured)

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
python start-tools.py profiler --config config_hdfs_ycloud.ini

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


## Setup on ycloud machine (for HDFS)
```shell
cd /home/systest
git clone https://github.com/szilard-nemeth/spark-experiments.git

cd spark-experiments
mkdir -p /home/systest/tool_output/

# Set up JAVA_HOME
# Launch spark3-shell and run: 
# sys.props("java.home")
# Example output: 
# scala> sys.props("java.home")
# res0: String = /usr/java/jdk1.8.0_232-cloudera/jre
export JAVA_HOME=/usr/java/jdk1.8.0_232-cloudera/jre
export PATH=$JAVA_HOME/bin:$PATH
# Test java by running 'java'


# Set up PyArrow / libhdfs.so
export CLASSPATH=$(hadoop classpath)
export ARROW_LIBHDFS_DIR=/opt/cloudera/parcels/CDH/lib64


python3.8 start-tools.py profiler --config config_hdfs_ycloud.ini --pull
```


## Troubleshooting
### Facing OSError: Unable to load libhdfs
The Spark profiler tool uses PyArrow to use HDFS paths.
The error `OSError: Unable to load libhdfs` indicates that PyArrow is trying to connect to HDFS using your `hdfs://...` URI type, but it cannot find the native C libraries (`libhdfs.dylib`) required to talk to the Hadoop file system.

The above error is coming from the PyArrow call e.g. `from_uri`: 

```python
import pyarrow.fs as fs


# Initialize FS
fs_impl = fs.FileSystem.from_uri("hdfs://path/to/dir")

# Opening a file
with fs_impl.open_input_file(filename") as f:
    ...
```



```shell
snemeth@22355 [21:51:25] <1> •100% --( ~/development/cloudera/cde/spark-scale-and-perf/spark-experiments ) @master [⇡+?] 🐳 v29.0.1 took 11.7s 
➜ python start-tools.py profiler --config config_hdfs_ycloud.ini
openjdk version "17.0.14" 2025-01-21
OpenJDK Runtime Environment Temurin-17.0.14+7 (build 17.0.14+7)
OpenJDK 64-Bit Server VM Temurin-17.0.14+7 (build 17.0.14+7, mixed mode, sharing)

>>> Launching Spark Profiler...
Requirement already satisfied: pip in ./venv/lib/python3.9/site-packages (25.3)
Requirement already satisfied: pyspark in ./venv/lib/python3.9/site-packages (from -r requirements.txt (line 1)) (4.0.1)
Requirement already satisfied: Cython in ./venv/lib/python3.9/site-packages (from -r requirements.txt (line 2)) (3.2.4)
Requirement already satisfied: pyarrow in ./venv/lib/python3.9/site-packages (from -r requirements.txt (line 3)) (21.0.0)
Requirement already satisfied: python-dateutil in ./venv/lib/python3.9/site-packages (from -r requirements.txt (line 4)) (2.9.0.post0)
Requirement already satisfied: py4j==0.10.9.9 in ./venv/lib/python3.9/site-packages (from pyspark->-r requirements.txt (line 1)) (0.10.9.9)
Requirement already satisfied: six>=1.5 in ./venv/lib/python3.9/site-packages (from python-dateutil->-r requirements.txt (line 4)) (1.17.0)
WARNING: Using incubator modules: jdk.incubator.vector
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/01/08 22:00:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Config: {'_config': <configparser.ConfigParser object at 0x106aab610>, 'logdir': 'hdfs://ccycloud-1.snemeth.root.comops.site:8020/user/spark/lngvtylog/vc-1-eventlog', 'output_dir': '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/tool_output/spark_profiler/execution-20260108-220013', 'summary_output_dir': '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/tool_output/spark_profiler', 'summary_file': '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/tool_output/spark_profiler/summary.csv', 'output_file': '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/tool_output/spark_profiler/execution-20260108-220013/applications-20260108_2200179496.csv', 'since': 1735707600.0}
Traceback (most recent call last):
  File "/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/spark_profiler.py", line 173, in <module>
    profiler.run()
  File "/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/spark_profiler.py", line 140, in run
    hdfs, base_path = fs.FileSystem.from_uri(self._config.logdir)
  File "pyarrow/_fs.pyx", line 502, in pyarrow._fs.FileSystem.from_uri
  File "pyarrow/_fs.pyx", line 457, in pyarrow._fs.FileSystem._native_from_uri
  File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
  File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
OSError: Unable to load libhdfs
dlopen(libhdfs.dylib) failed: dlopen(libhdfs.dylib, 0x0006): tried: 'libhdfs.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OSlibhdfs.dylib' (no such file), '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/venv/lib/python3.9/site-packages/pyarrow/libhdfs.dylib' (no such file), '/usr/lib/libhdfs.dylib' (no such file, not in dyld cache), 'libhdfs.dylib' (no such file)
dlopen(./libhdfs.dylib) failed: dlopen(./libhdfs.dylib, 0x0006): tried: './libhdfs.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS./libhdfs.dylib' (no such file), '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/venv/lib/python3.9/site-packages/pyarrow/./libhdfs.dylib' (no such file), '/usr/lib/./libhdfs.dylib' (no such file, not in dyld cache), './libhdfs.dylib' (no such file), '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/libhdfs.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/libhdfs.dylib' (no such file), '/Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/libhdfs.dylib' (no such file)

[ERROR] Command failed: /Users/snemeth/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler/local/venv/bin/python spark_profiler.py local
```

#### Solution - Only for ycloud machines
Set up PyArrow / libhdfs.so with:
```shell
export CLASSPATH=$(hadoop classpath)
export ARROW_LIBHDFS_DIR=/opt/cloudera/parcels/CDH/lib64
```