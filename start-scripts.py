import os
import subprocess
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Equivalent to REPO_ROOT
REPO_ROOT = Path(__file__).resolve().parent
EXTERNAL_SCRIPTS = REPO_ROOT / "external_scripts"
SPARK_INVENTORY_REPO_URL = "https://github.com/szilard-nemeth/Spark_Inventory_API.git"
SPARK_PROFILER_REPO_URL = "https://github.infra.cloudera.com/snemeth/spark-profiler.git"

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

def task_spark_inventory(base_url, knox_token, branch, pull: bool):
    repo_name = "spark_inventory_api"
    target_dir = EXTERNAL_SCRIPTS / repo_name
    target_dir.mkdir(parents=True, exist_ok=True)

    # TODO Force pull if directory does not exists
    if pull:
        # Git Logic
        if not (target_dir / ".git").exists():
            run_command(f"git clone {SPARK_INVENTORY_REPO_URL} .", cwd=target_dir)
        else:
            run_command("git fetch --all", cwd=target_dir)
        run_command(f"git checkout {branch}", cwd=target_dir)
        run_command(f"git pull origin {branch}", cwd=target_dir)

    python_bin = setup_venv(target_dir)

    # Config Generation
    config_content = f"[DEFAULT]\nBASE_URL={base_url}\nKNOX_TOKEN={knox_token}\nPASS_TOKEN=False\n"
    (target_dir / "config_test.ini").write_text(config_content)

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

def task_spark_profiler(event_log_dir, output_dir, branch, pull: bool):
    repo_name = "spark_profiler"
    target_dir = EXTERNAL_SCRIPTS / repo_name
    # TODO Get rid of 'local' dir later
    local_dir = target_dir / "local"
    target_dir.mkdir(parents=True, exist_ok=True)

    if pull:
        if not (target_dir / ".git").exists():
            run_command(f"git clone {SPARK_PROFILER_REPO_URL} .", cwd=target_dir)
        else:
            run_command("git fetch --all", cwd=target_dir)

        run_command(f"git checkout {branch}", cwd=target_dir)
        run_command(f"git pull origin {branch}", cwd=target_dir)

    local_dir.mkdir(exist_ok=True)
    python_bin = setup_venv(local_dir)

    # Config Generation
    config_content = f"[DEFAULT]\nEVENTLOGDIR={event_log_dir}\nOUTPUTDIR={output_dir}\nSINCE=2025-01-01 00:00\n"
    (local_dir / "config.ini").write_text(config_content)

    # Execution
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")

    env = os.environ.copy()
    env["PYTHONPATH"] = ".."

    output = run_command(f"{python_bin} spark-profiler.py", cwd=local_dir, env=env)
    (Path(output_dir) / f"logs-{ts}.txt").write_text(output)

def main():
    parser = argparse.ArgumentParser(description="Spark Tool Launcher")
    parser.add_argument("tool", choices=["inventory", "profiler", "all"], help="Which tool to run")
    # TODO Adjust to master branch later?
    parser.add_argument("--branch", default="learn", help="Git branch to use")
    parser.add_argument("--url", default="http://localhost:18080", help="Base URL (Inventory)")
    parser.add_argument("--token", default="dummy", help="Knox Token (Inventory)")
    parser.add_argument("--logs", default=str(REPO_ROOT / "spark_events"), help="Event logs dir (Profiler)")
    parser.add_argument("--out", help="Custom output directory")
    parser.add_argument(
        "--pull",
        action="store_true",
        default=False,
        help="Pull git repository"
    )

    args = parser.parse_args()

    # Pre-flight check
    run_command("java -version")

    if args.tool in ["inventory", "all"]:
        print("\n>>> Launching Spark Inventory API...")
        task_spark_inventory(args.url, args.token, args.branch, args.pull)

    if args.tool in ["profiler", "all"]:
        print("\n>>> Launching Spark Profiler...")
        out_path = args.out if args.out else str(REPO_ROOT / "output_spark_profiler")
        task_spark_profiler(args.logs, out_path, args.branch, args.pull)

if __name__ == "__main__":
    main()