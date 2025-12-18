REPO_ROOT="$(cd "$(dirname -- "$(readlink -f "${BASH_SOURCE[0]}" || echo "${BASH_SOURCE[0]}")")" >/dev/null 2>&1 && pwd)"


function spark_inventory_pull_and_launch {
  # 1. Mandatory Parameters & Branch Logic
  local BASE_URL=$1
  local KNOX_TOKEN=$2
  local BRANCH=${3:-learn} # Defaults to 'learn' if 3rd argument is missing
  local REPO_DIR="Spark_Inventory_API"

  # Check if mandatory arguments are provided
  if [[ -z "$BASE_URL" || -z "$KNOX_TOKEN" ]]; then
    echo "Usage: spark-inventory <BASE_URL> <KNOX_TOKEN> [BRANCH]"
    echo "Example: spark-inventory https://your-cluster-url.com your-secret-token development"
    return 1
  fi

  cd external_scripts || exit

  # 2. Clone or Update Repo
  if [ ! -d "$REPO_DIR" ]; then
    git clone https://github.com/szilard-nemeth/Spark_Inventory_API.git
    cd "$REPO_DIR" || exit
  else
    cd "$REPO_DIR" || exit
    git fetch --all
  fi

  # 3. Checkout and Sync
  git checkout "$BRANCH"
  git pull origin "$BRANCH"
  spark_inventory_just_launch

}

function spark_inventory_just_launch {
  local REPO_DIR="Spark_Inventory_API"

  cd external_scripts || exit
  cd "$REPO_DIR" || exit

  # 4. Virtual Environment Setup
  echo "Setting up virtual environment..."
  # Create the venv if it doesn't exist
  if [ ! -d "venv" ]; then
    python3 -m venv venv
  fi

  # Activate the virtual environment
  source venv/bin/activate

  # Upgrade pip and install requirements
  pip install --upgrade pip
  if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
  else
    # Fallback if requirements.txt is missing but you know specific deps
    echo "requirements.txt not found, installing base dependencies..."
    pip install requests
  fi

  # 5. Generate config_test.ini
  cat <<EOF > config_test.ini
[DEFAULT]
; Obtain from DataHub Endpoints UI
BASE_URL=$BASE_URL
; Obtain from Knox Token Management UI
KNOX_TOKEN=$KNOX_TOKEN
PASS_TOKEN=False
EOF

  echo "Configuration file 'config_test.ini' created with provided credentials."

  # 5. Execute script
  # Note: The script uses the Config class to parse the ini file
  set -x
  if [ -f "datahubExample.py" ]; then
    mkdir -p $REPO_ROOT/sample_Spark_Inventory_API
    python3 datahubExample.py --config config_test.ini --print printall | tee $REPO_ROOT/sample_Spark_Inventory_API/output-printall.txt
    python3 datahubExample.py --config config_test.ini --print printmeta --print printenv | tee $REPO_ROOT/sample_Spark_Inventory_API/output-printmeta-printenv.txt
  else
    echo "Error: datahubExample.py not found."
  fi

  set +x
  # Optional: Deactivate venv after script finishes
  deactivate
}



#####################################################
BASE_URL="http://localhost:18080"
KNOX_TOKEN="dummy"
# spark_inventory_pull_and_launch "$BASE_URL" "$KNOX_TOKEN" "learn"
spark_inventory_just_launch



