import os
import subprocess
import sys
import argparse
import configparser
from datetime import datetime
from pathlib import Path

# Paths
# Equivalent to REPO_ROOT
REPO_ROOT = Path(__file__).resolve().parent
EXTERNAL_SCRIPTS = REPO_ROOT / "external_scripts"
CONFIG_FILE = REPO_ROOT / "config.ini"

# Constants
SPARK_INVENTORY_REPO_URL = "https://github.com/szilard-nemeth/Spark_Inventory_API.git"
SPARK_INVENTORY_REPO_NAME = "spark_inventory_api"
SPARK_PROFILER_REPO_URL = "https://github.infra.cloudera.com/snemeth/spark-profiler.git"
SPARK_PROFILER_REPO_NAME = "spark_profiler"

def get_or_create_config():
    """Reads the ini file or creates a default one if missing."""
    config = configparser.ConfigParser()

    if CONFIG_FILE.exists():
        config.read(CONFIG_FILE)
    else:
        # TODO Adjust to master branch later?
        print(f"--- Generating default config: {CONFIG_FILE} ---")
        config["inventory"] = {
            "base_url": "http://localhost:18080",
            # TODO Use real-world knox setup
            "knox_token": "dummy",
            "branch": "learn"
        }
        config["profiler"] = {
            "event_log_dir": str(REPO_ROOT / "spark_events"),
            "output_dir": str(REPO_ROOT / "output_spark_profiler"),
            "branch": "learn"
        }
        with open(CONFIG_FILE, "w") as f:
            config.write(f)
    return config


def run_command(command, cwd=None, env=None, capture_output=True):
    """Executes shell commands and streams output to console and log."""
    try:
        # Use shell=True to support multi-part commands if necessary
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        output_lines = []
        for line in process.stdout:
            print(line, end="")
            output_lines.append(line)

        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)

        return "".join(output_lines)
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Command failed: {command}")
        sys.exit(e.returncode)

def setup_venv(repo_path: Path):
    """Creates/Updates venv and returns the path to the python executable."""
    venv_path = repo_path / "venv"
    # Handling Windows vs Unix path differences for the venv binary
    bin_dir = "Scripts" if os.name == "nt" else "bin"
    python_bin = venv_path / bin_dir / "python"
    pip_bin = venv_path / bin_dir / "pip"

    if not venv_path.exists():
        print(f"--- Creating venv in {repo_path} ---")
        run_command(f"{sys.executable} -m venv venv", cwd=repo_path)

    run_command(f"{pip_bin} install --upgrade pip", cwd=repo_path)
    req_file = repo_path / "requirements.txt"
    if req_file.exists():
        run_command(f"{pip_bin} install -r requirements.txt", cwd=repo_path)
    else:
        run_command(f"{pip_bin} install requests", cwd=repo_path)

    return python_bin

def sync_repo(target_dir, url, branch, pull):
    """Handles git cloning and pulling."""
    if not (target_dir / ".git").exists():
        print(f"Cloning {url}...")
        run_command(f"git clone {url} .", cwd=target_dir)
        pull = True # Force sync if we just cloned

    if pull:
        run_command("git fetch --all", cwd=target_dir)
        run_command(f"git checkout {branch}", cwd=target_dir)
        run_command(f"git pull origin {branch}", cwd=target_dir)

def task_spark_inventory(conf, pull: bool):
    target_dir = EXTERNAL_SCRIPTS / SPARK_INVENTORY_REPO_NAME
    target_dir.mkdir(parents=True, exist_ok=True)

    # TODO Force pull if directory does not exists
    sync_repo(target_dir, SPARK_INVENTORY_REPO_URL, conf['branch'], pull)
    python_bin = setup_venv(target_dir)

    # Write local tool config
    local_cfg = f"[DEFAULT]\nBASE_URL={conf['base_url']}\nKNOX_TOKEN={conf['knox_token']}\nPASS_TOKEN=False\n"
    (target_dir / "config_test.ini").write_text(local_cfg)

    # Execution
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = REPO_ROOT / "output_spark_inventory_api"
    out_dir.mkdir(exist_ok=True)

    execution_tasks = [
        ("--print printall", "printall"),
        ("--print printall --format-json", "printall-formatted"),
        ("--print printmeta --print printenv", "printmeta-printenv")
    ]

    for args, suffix in execution_tasks:
        cmd = f"{python_bin} datahubExample.py --config config_test.ini {args}"
        output = run_command(cmd, cwd=target_dir)
        (out_dir / f"output-{suffix}-{ts}.txt").write_text(output)

def task_spark_profiler(conf, pull: bool):
    target_dir = EXTERNAL_SCRIPTS / SPARK_PROFILER_REPO_NAME
    # TODO Get rid of 'local' dir later
    local_dir = target_dir / "local"
    target_dir.mkdir(parents=True, exist_ok=True)

    sync_repo(target_dir, SPARK_PROFILER_REPO_URL, conf['branch'], pull)

    local_dir.mkdir(exist_ok=True)
    python_bin = setup_venv(local_dir)

    # Write local tool config
    local_cfg = f"[DEFAULT]\nEVENTLOGDIR={conf['event_log_dir']}\nOUTPUTDIR={conf['output_dir']}\nSINCE=2025-01-01 00:00\n"
    (local_dir / "config.ini").write_text(local_cfg)

    # Execution
    Path(conf['output_dir']).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")

    env = os.environ.copy()
    env["PYTHONPATH"] = ".."

    output = run_command(f"{python_bin} spark-profiler.py", cwd=local_dir, env=env)
    (Path(conf['output_dir']) / f"logs-{ts}.txt").write_text(output)

def main():
    parser = argparse.ArgumentParser(description="Spark Tool Launcher")
    parser.add_argument("tool", choices=["inventory", "profiler", "all"], help="Which tool to run")
    parser.add_argument("--pull", action="store_true", help="Sync git repos")
    args = parser.parse_args()

    config = get_or_create_config()

    # Pre-flight check
    run_command("java -version")

    if args.tool in ["inventory", "all"]:
        print("\n>>> Launching Spark Inventory...")
        task_spark_inventory(config["inventory"], args.pull)

    if args.tool in ["profiler", "all"]:
        print("\n>>> Launching Spark Profiler...")
        task_spark_profiler(config["profiler"], args.pull)

if __name__ == "__main__":
    main()