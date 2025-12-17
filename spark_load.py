from pyspark.sql import SparkSession
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadGeneratorAndParser") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "512m") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/opt/spark/events") \
    .config("spark.eventLog.logBlockUpdates.enabled", "true") \
    .config("spark.executor.processTreeMetrics.enabled", "true") \
    .config("spark.executor.metrics.pollingInterval", "1s") \
    .getOrCreate()  # <--- Corrected this line

# Reduce noise in the console
spark.sparkContext.setLogLevel("WARN")

def generate_load():
    print("--- Generating Heavy CPU Load ---")
    # Increase n to 100 million or more to cross the 1s polling threshold
    n = 100000000
    count = spark.sparkContext.parallelize(range(0, n), 10) \
        .map(lambda x: 1 if x % 2 == 0 else 0).count()
    print(f"Load task complete. Processed {count} elements.")

def parse_event_logs():
    log_dir = "/opt/spark/events"
    print(f"--- Parsing Event Logs in {log_dir} ---")

    if not os.path.exists(log_dir) or not os.listdir(log_dir):
        print("No logs found yet.")
        return

    try:
        # Read the raw JSON event logs
        df = spark.read.json(log_dir)
        # if "Event" in df.columns:
        #     df.groupBy("Event").count().show()
        # else:
        #     print("Logs found, but no 'Event' column detected yet.")

        # Check TaskEnd events for peak values recorded at the end of every task
        if "Task Executor Metrics" in df.columns:
            print("--- Executor Memory Performance ---")
            df.filter(df.Event == "SparkListenerTaskEnd") \
                .select(
                "Task Info.Executor ID",
                "Task Executor Metrics.JVMHeapMemory",
                "Task Executor Metrics.OffHeapExecutionMemory"
            ).show()
    except Exception as e:
        print(f"Could not parse logs: {e}")

if __name__ == "__main__":
    generate_load()
    parse_event_logs()
    spark.stop()