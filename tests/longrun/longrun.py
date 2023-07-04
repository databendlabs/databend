import os
import subprocess
from multiprocessing import Pool
import time
import argparse

warehouse_dsn = "databend://root:@localhost:8000/?sslmode=disable"
log_table = "events"


def run_script(args):
    script, max_iter_limit = args
    start_time = time.time()
    run_times = 0
    work_id = script
    env = os.environ.copy()
    env["WORK_ID"] = str(script)
    env["SEED"] = str(run_times)
    env["DSN"] = str(warehouse_dsn)
    output_file = os.path.basename(work_id)
    output_file = os.path.splitext(output_file)[0]
    output_file = f"_{output_file}_{int(start_time)}_events.csv"
    while run_times < max_iter_limit:
        with open(output_file, "a") as f:
            p_start = time.time()
            p = subprocess.Popen(["bash", script], stdout=f, env=env)
            p.communicate()
            print(
                "job {}, run {}. duration: {}".format(
                    script, run_times, time.time() - p_start
                )
            )
            run_times += 1
            env["SEED"] = str(run_times)
    print(
        "finish job {}, run {}, state file: {}".format(script, run_times, output_file)
    )
    if max_iter_limit > 0:
        upload_log_file(output_file)


def upload_log_file(file):
    file = os.path.abspath(file)
    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        try:
            p = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'bendsql --dsn "{}" -q "insert into {} values" -d @{}'.format(
                        warehouse_dsn, log_table, file
                    ),
                ]
            )
            p.wait()

            error_code = p.returncode
            if error_code == 0:
                os.remove(file)
                break
            else:
                raise ValueError("failed to upload {}", format(file))
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            attempt += 1
            time.sleep(1)  # Wait for a short period before retrying


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script runner")
    parser.add_argument(
        "-d", "--dir", help="Directory path containing scripts", required=True
    )
    parser.add_argument(
        "-i",
        "--iter",
        type=int,
        help="Maximum number time a job needs to run",
        required=True,
    )
    parser.add_argument(
        "--logger", help="event logging table", required=False, default="events"
    )
    args = parser.parse_args()
    warehouse_dsn = os.getenv(
        "WAREHOUSE_DSN", "databend://root:@localhost:8000/?sslmode=disable"
    )
    directory_path = args.dir
    maximum_time_limit = args.iter
    log_table = args.logger

    file_list = os.listdir(directory_path)

    before_scripts = sorted(
        [
            script
            for script in file_list
            if script.endswith(".sh")
            if script.startswith("_before")
        ]
    )
    if len(before_scripts) > 0:
        print("execute before scripts, num: {}".format(len(before_scripts)))
        for script in before_scripts:
            run_script((os.path.join(directory_path, script), 1))

    scripts_list = [
        script
        for script in file_list
        if script.endswith(".sh")
        if script.startswith("_") is False
    ]
    if len(scripts_list) > 0:
        print("execute scripts concurrently, num: {}".format(len(scripts_list)))
        # create a separate process for each script
        with Pool(processes=len(scripts_list)) as pool:
            pool.map(
                run_script,
                [
                    (os.path.join(directory_path, script), maximum_time_limit)
                    for script in scripts_list
                ],
            )

    after_scripts = sorted(
        [
            script
            for script in file_list
            if script.endswith(".sh")
            if script.startswith("_after")
        ]
    )
    if len(after_scripts) > 0:
        print("execute after scripts, num: {}".format(len(after_scripts)))
        for script in after_scripts:
            run_script((os.path.join(directory_path, script), 1))
