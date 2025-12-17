from pyspark.sql import SparkSession
import os

# 1. Initialize Spark Session
# We enable event logging so the script has files to parse later
spark = SparkSession.builder \
    .appName("LoadGeneratorAndParser") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/opt/bitnami/spark/events") \
    .get_name_or_create()

def generate_load():
    print("--- Generating CPU Load (Calculating Pi) ---")
    n = 10000000
    count = spark.sparkContext.parallelize(range(0, n), 4) \
        .map(lambda x: 1 if x % 2 == 0 else 0).count()
    print(f"Load task complete. Processed {count} elements.")

def parse_event_logs():
    log_dir = "/opt/bitnami/spark/events"
    print(f"--- Parsing Event Logs in {log_dir} ---")

    if not os.path.exists(log_dir) or not os.listdir(log_dir):
        print("No logs found yet.")
        return

    # Read the raw JSON event logs
    # Spark event logs are technically newline-delimited JSON
    try:
        df = spark.read.json(log_dir)
        if "Event" in df.columns:
            df.groupBy("Event").count().show()
        else:
            print("Logs found, but no 'Event' column detected yet.")
    except Exception as e:
        print(f"Could not parse logs: {e}")

if __name__ == "__main__":
    generate_load()
    parse_event_logs()
    spark.stop()