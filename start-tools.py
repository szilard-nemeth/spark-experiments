import os
import shutil
import subprocess
import sys
import argparse
import configparser
from datetime import datetime
from pathlib import Path
from typing import Any

# Paths
# Equivalent to REPO_ROOT
REPO_ROOT = Path(__file__).resolve().parent
EXTERNAL_SCRIPTS = REPO_ROOT / "external_scripts"
DEFAULT_CONFIG_FILE_NAME = "config.ini"

TOOL_INVENTORY = "inventory"
TOOL_PROFILER = "profiler"
TOOL_CDP_MONITOR_PULL = "cdp-monitor-pull"


class ConfigWriter:
    def __init__(self):
        self.writer = configparser.ConfigParser()
        # Preserves the casing of keys instead of defaulting to lowercase
        self.writer.optionxform = str

    def add_section_data(self, section: str, data: dict):
        """Adds or updates a dictionary of data to a specific section."""
        if section not in self.writer:
            self.writer.add_section(section)

        # Apply your specific logic: keys converted to UPPERCASE
        for k, v in data.items():
            self.writer[section][k.upper()] = str(v)

    def add_key(self, section: str, key: str, value: Any):
        """Adds a single key-value pair to a section."""
        if section not in self.writer:
            self.writer.add_section(section)

        self.writer[section][key.upper()] = str(value)

    def save(self, file_path: Path):
        """Writes the current state to the specified file path."""
        with file_path.open("w") as f:
            self.writer.write(f)


def read_config(config_file_name: str):
    """Reads the ini file or creates a default one if missing."""
    config = configparser.ConfigParser()

    config_path = REPO_ROOT / config_file_name
    if not config_path.exists():
        raise ValueError("Cannot find config file: " + str(config_path))

    config.read(config_path)
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

def _create_execution_dir(conf) -> Path:
    # Execution dir
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(conf['output_dir'])
    output_dir.mkdir(exist_ok=True, parents=True)

    # Create the execution-specific subdirectory
    execution_dir = output_dir / f"execution-{ts}"
    execution_dir.mkdir(exist_ok=True, parents=True)
    return execution_dir

def sync_repo(target_dir, url, branch, pull):
    """Handles git cloning and pulling."""
    if not (target_dir / ".git").exists():
        print(f"Cloning {url}...")
        run_command(f"git clone {url} .", cwd=target_dir)
        pull = True # Force sync if we just cloned

    if pull:
        run_command("git fetch --all", cwd=target_dir)
        run_command(f"git checkout {branch}", cwd=target_dir)

        # List of potential config files for tools
        configs = ["config.ini", "config_test.ini"]

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
    target_dir = EXTERNAL_SCRIPTS / conf['repo_target_dir_name']
    target_dir.mkdir(parents=True, exist_ok=True)

    sync_repo(target_dir, conf['repo_url'], conf['branch'], pull)
    python_bin = setup_venv(target_dir)

    execution_dir = _create_execution_dir(conf)

    # Write local tool config
    # Spark Inventory does not use 'output_dir' so no need to add it to its config
    write_ini_file(target_dir / "config_test.ini", conf, "DEFAULT")

    # Execution
    execution_tasks = [
        ("--print printall", "printall"),
        ("--print printall --format-json", "printall-formatted"),
        ("--print printmeta --print printenv", "printmeta-printenv")
    ]

    for args, suffix in execution_tasks:
        cmd = f"{python_bin} datahubExample.py --config config_test.ini {args}"
        output = run_command(cmd, cwd=target_dir)
        (execution_dir / f"stdout-{suffix}.txt").write_text(output)


def task_spark_profiler(conf, pull: bool):
    target_dir = EXTERNAL_SCRIPTS / conf['repo_target_dir_name']
    # TODO Get rid of 'local' dir later
    local_dir = target_dir / "local"
    target_dir.mkdir(parents=True, exist_ok=True)

    sync_repo(target_dir, conf['repo_url'], conf['branch'], pull)

    local_dir.mkdir(exist_ok=True)
    python_bin = setup_venv(local_dir)

    execution_dir = _create_execution_dir(conf)

    # Write local tool config
    # Spark profiler uses the 'output_dir' so we need to override it in the config with the execution dir
    config_dict = {**conf, "output_dir": str(execution_dir)}
    write_ini_file(local_dir / "config.ini", config_dict, "DEFAULT")

    # Execution
    env = os.environ.copy()
    env["PYTHONPATH"] = ".."

    output = run_command(f"{python_bin} spark_profiler.py {conf['mode']}", cwd=local_dir, env=env)
    (execution_dir / f"stdout.txt").write_text(output)


def task_cdp_monitor_pull(conf, pull: bool):
    target_dir = EXTERNAL_SCRIPTS / conf['repo_target_dir_name']
    target_dir.mkdir(parents=True, exist_ok=True)

    sync_repo(target_dir, conf['repo_url'], conf['branch'], pull)
    python_bin = setup_venv(target_dir)

    execution_dir = _create_execution_dir(conf)

    # Write local tool config
    config_template: Path = target_dir / "config_template_pull_spark.ini"
    config_ini = target_dir / "config.ini"
    shutil.copy(config_template, config_ini)

    conf_writer = ConfigWriter()
    conf_writer.add_section_data("CM", {
        "cm_url": conf["cm_url"],
        "cluster": conf["cm_cluster_name"],
        "cm_user": conf["cm_user"],
        "cm_pass": conf["cm_user"],
    })
    conf_writer.add_section_data("Dates", {
        "from_date": conf["from_date"],
        "to_date": conf["to_date"],
        "limit": conf["limit"],
        "scan_time_window": conf["scan_time_window"],

    })
    conf_writer.add_section_data("Yarn", {
        "service": conf["service"],
        "pool": conf["pool"],
        "application_types": conf["application_types"],
    })

    # CDP Monitor uses the 'output_dir' so we need to override it in the config with the execution dir
    conf_writer.add_section_data("Output", {
        "output_dir": str(execution_dir),
    })
    conf_writer.add_section_data("Threads", {
        "max_threads": conf["max_threads"],
    })
    conf_writer.save(config_ini)

    output = run_command(f"{python_bin} cdp_monitor_pull.py --service spark --config config.ini --verify", cwd=target_dir)
    (execution_dir / "stdout.txt").write_text(output)

def main():
    parser = argparse.ArgumentParser(description="Spark Tool Launcher")
    parser.add_argument("tool", choices=[TOOL_INVENTORY, TOOL_PROFILER, TOOL_CDP_MONITOR_PULL, "all"], help="Which tool to run")
    parser.add_argument("--pull", action="store_true", help="Sync git repos")
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_FILE_NAME,
        help="Name of the configuration file (e.g., my_config.ini)"
    )


    args = parser.parse_args()
    config = read_config(args.config)

    # Pre-flight check
    # TODO uncomment this later?
    # run_command("java -version")

    if args.tool in [TOOL_INVENTORY, "all"]:
        print("\n>>> Launching Spark Inventory...")
        task_spark_inventory(config[TOOL_INVENTORY], args.pull)
    if args.tool in [TOOL_PROFILER, "all"]:
        print("\n>>> Launching Spark Profiler...")
        task_spark_profiler(config[TOOL_PROFILER], args.pull)
    if args.tool in [TOOL_CDP_MONITOR_PULL, "all"]:
        print("\n>>> Launching CDP Monitor pull...")
        task_cdp_monitor_pull(config[TOOL_CDP_MONITOR_PULL], args.pull)


if __name__ == "__main__":
    main()