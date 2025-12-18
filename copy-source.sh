# Copy python files from Spark_Inventory_API
mkdir -p ./external_scripts/spark_inventory_api/
cp -R $HOME/development/cloudera/cde/spark-scale-and-perf/Spark_Inventory_API/*.py ./external_scripts/spark_inventory_api/
ls -latr ./external_scripts/spark_inventory_api/

# mkdir -p ./external_scripts/spark_profiler/
# cd $HOME/development/cloudera/cde/spark-scale-and-perf/spark-profiler/
# find . -name "*.py" -exec cp --parents {} "$(pwd)/external_scripts/spark_profiler/" \;
# ls -latr ./external_scripts/spark_profiler/

# Copy python files from spark-profiler
SRC_DIR="$HOME/development/cloudera/cde/spark-scale-and-perf/spark-profiler/"
TARGET_DIR="$HOME/development/cloudera/cde/spark-scale-and-perf/spark-experiments/external_scripts/spark_profiler"

# 1. Create target directory if it doesn't exist
mkdir -p "$TARGET_DIR"

# 2. Copy preserving structure
cd "$SRC_DIR"
find . -name "*.py" | cpio -pdm "$TARGET_DIR"

# 3. Verify
echo "Files copied to $TARGET_DIR:"
ls -R "$TARGET_DIR"