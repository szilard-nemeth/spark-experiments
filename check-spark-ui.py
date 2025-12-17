import requests
import sys
import time

def verify_spark_ui(url="http://localhost:8080", timeout=10):
    print(f"🔍 Checking Spark UI at {url}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Check for the word 'Alive' or 'Workers: 1' in the UI output
                content = response.text
                if "Alive" in content or "Workers" in content:
                    print("✅ Success: Spark Master UI is UP.")

                    # Optional: Check if at least one worker is registered
                    if "Workers: 0" in content:
                        print("⚠️  Warning: Master is up, but 0 Workers are registered.")
                    else:
                        print("🚀 Cluster is healthy with active workers.")
                    return True
        except requests.exceptions.ConnectionError:
            pass

        print("...waiting for UI to initialize...")
        time.sleep(2)

    print("❌ Error: Spark UI could not be reached. Check if port 8080 is mapped.")
    return False

if __name__ == "__main__":
    if not verify_spark_ui():
        sys.exit(1)