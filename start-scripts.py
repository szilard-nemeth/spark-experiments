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

def read_config():
    """Reads the ini file or creates a default one if missing."""
    config = configparser.ConfigParser()

    if not CONFIG_FILE.exists():
        raise ValueError("Cannot find config file: " + str(CONFIG_FILE))

    config.read(CONFIG_FILE)
    return config

def write_ini_file(file_path: Path, data: dict, section: str):
    """Writes a dictionary to an INI file elegantly."""
    writer = configparser.ConfigParser()

    # This prevents the default lowercase transformation
    writer.optionxform = str

    writer[section] = {k.upper(): str(v) for k, v in data.items()}
    with file_path.open("w") as f:
        writer.write(f)


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
    # TODO Force pull if directory does not exists
    """Handles git cloning and pulling."""
    if not (target_dir / ".git").exists():
        print(f"Cloning {url}...")
        run_command(f"git clone {url} .", cwd=target_dir)
        pull = True # Force sync if we just cloned

    if pull:
        run_command("git fetch --all", cwd=target_dir)
        run_command(f"git checkout {branch}", cwd=target_dir)

        # List of potential config files
        configs = ["local/config.ini", "config_test.ini"]

        # Discard local changes to config files so pull doesn't fail
        # This handles 'config.ini' and 'config_test.ini'
        ## run_command("git checkout -- local/config.ini config_test.ini || true", cwd=target_dir)
        for cfg in configs:
            cfg_path = target_dir / cfg
            if not cfg_path.exists():
                continue

            # Check if Git is actually tracking this file
            # 'git ls-files --error-unmatch' returns 0 if tracked, non-zero if not
            is_tracked = subprocess.run(
                ["git", "ls-files", "--error-unmatch", cfg],
                cwd=target_dir, capture_output=True
            ).returncode == 0

            if is_tracked:
                # File is tracked: Reset it to HEAD so pull succeeds
                print(f"Resetting tracked file: {cfg}")
                run_command(f"git checkout -- {cfg}", cwd=target_dir)
            else:
                # File is untracked: Delete it so it doesn't collide with pull
                print(f"Removing untracked file: {cfg}")
                cfg_path.unlink()

        run_command(f"git pull origin {branch}", cwd=target_dir)

def task_spark_inventory(conf, pull: bool):
    target_dir = EXTERNAL_SCRIPTS / SPARK_INVENTORY_REPO_NAME
    target_dir.mkdir(parents=True, exist_ok=True)

    sync_repo(target_dir, SPARK_INVENTORY_REPO_URL, conf['branch'], pull)
    python_bin = setup_venv(target_dir)

    # Write local tool config
    config_dict = {**conf, "pass_token": False}
    write_ini_file(target_dir / "config_test.ini", config_dict, "DEFAULT")

    # Execution
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(conf['output_dir'])
    output_dir.mkdir(exist_ok=True, parents=True)

    execution_tasks = [
        ("--print printall", "printall"),
        ("--print printall --format-json", "printall-formatted"),
        ("--print printmeta --print printenv", "printmeta-printenv")
    ]

    for args, suffix in execution_tasks:
        cmd = f"{python_bin} datahubExample.py --config config_test.ini {args}"
        output = run_command(cmd, cwd=target_dir)
        (output_dir / f"output-{suffix}-{ts}.txt").write_text(output)

def task_spark_profiler(conf, pull: bool):
    target_dir = EXTERNAL_SCRIPTS / SPARK_PROFILER_REPO_NAME
    # TODO Get rid of 'local' dir later
    local_dir = target_dir / "local"
    target_dir.mkdir(parents=True, exist_ok=True)

    sync_repo(target_dir, SPARK_PROFILER_REPO_URL, conf['branch'], pull)

    local_dir.mkdir(exist_ok=True)
    python_bin = setup_venv(local_dir)

    # Write local tool config
    config_dict = {**conf, "since": "2025-01-01 00:00"}
    write_ini_file(local_dir / "config.ini", config_dict, "DEFAULT")

    # Execution
    output_dir = Path(conf['output_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")

    env = os.environ.copy()
    env["PYTHONPATH"] = ".."

    output = run_command(f"{python_bin} spark_profiler.py", cwd=local_dir, env=env)
    (output_dir / f"logs-{ts}.txt").write_text(output)

def main():
    parser = argparse.ArgumentParser(description="Spark Tool Launcher")
    parser.add_argument("tool", choices=["inventory", "profiler", "all"], help="Which tool to run")
    parser.add_argument("--pull", action="store_true", help="Sync git repos")
    args = parser.parse_args()

    config = read_config()

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