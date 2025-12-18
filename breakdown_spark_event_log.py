import json
import argparse
from pathlib import Path

def split_spark_logs(input_file, target_dir):
    # Ensure the target directory exists
    output_path = Path(target_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Dictionary to keep track of event counts for naming duplicates
    event_counts = {}

    try:
        with open(input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    event_name = data.get("Event", "UnknownEvent")

                    # Update count for this specific event name
                    event_counts[event_name] = event_counts.get(event_name, 0) + 1
                    current_count = event_counts[event_name]

                    # Handle filename formatting
                    if current_count == 1:
                        file_name = f"{event_name}.json"
                    else:
                        file_name = f"{event_name}_{current_count}.json"

                    file_dest = output_path / file_name

                    # Save formatted JSON
                    with open(file_dest, 'w') as out_file:
                        json.dump(data, out_file, indent=4)

                except json.JSONDecodeError:
                    print(f"Warning: Skipping a line that is not valid JSON.")

        print(f"Success: Files have been generated in {target_dir}")

    except FileNotFoundError:
        print(f"Error: The input file '{input_file}' was not found.")

if __name__ == "__main__":
    # Example call
    # python3 breakdown_spark_event_log.py $HOME/development/cloudera/cde/spark-scale-and-perf/spark-experiments/sample_spark_event_files/breakdown_app-20251217040542-0000-b76071b9 $HOME/development/cloudera/cde/spark-scale-and-perf/spark-experiments/sample_spark_events/breakdown_app-20251217040542-0000-b76071b9
    parser = argparse.ArgumentParser(description="Split Spark event logs into individual JSON files.")
    parser.add_argument("input_file", help="Path to the Spark event log file (JSONL format)")
    parser.add_argument("target_dir", help="Directory where the individual JSON files will be saved")

    args = parser.parse_args()

    split_spark_logs(args.input_file, args.target_dir)