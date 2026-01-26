import hashlib
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

import grpc
import json
from concurrent import futures
from grpc_reflection.v1alpha import reflection
from google.protobuf import json_format
from datetime import datetime, timezone
import calendar
import task_pb2
import task_pb2_grpc
import notification_pb2
import notification_pb2_grpc
import timestamp_pb2
import udf_pb2
import udf_pb2_grpc

# Simple in-memory database
TASK_DB = {}
TASK_RUN_DB = {}

NOTIFICATION_DB = {}
NOTIFICATION_HISTORY_DB = {}

UDF_HEADERS = {"x-authorization": os.getenv("UDF_MOCK_TOKEN", "123")}
UDF_DOCKER_KEEP_CONTAINER = True
UDF_DOCKER_LOG_COMMANDS = True
UDF_DOCKER_IMAGE_PREFIX = os.getenv("UDF_DOCKER_IMAGE_PREFIX", "databend-udf-cloud")
UDF_DOCKER_HOST = os.getenv("UDF_DOCKER_HOST", "127.0.0.1")
UDF_DOCKER_CONTAINER_PORT = int(os.getenv("UDF_DOCKER_CONTAINER_PORT", "8815"))
UDF_DOCKER_BASE_IMAGE = os.getenv("UDF_DOCKER_BASE_IMAGE", "python:3.12-slim")
UDF_DOCKER_STARTUP_TIMEOUT_SECS = float(
    os.getenv("UDF_DOCKER_STARTUP_TIMEOUT_SECS", "15")
)
UDF_DOCKER_CACHE = {}
UDF_DOCKER_LOCK = threading.Lock()
RESOURCE_STATUS_CONTAINER_PORT = 8080
RESOURCE_DOCKER_CACHE = {}
RESOURCE_DOCKER_LOCK = threading.Lock()
RESOURCE_IMAGE_BY_TYPE = {
    "udf": UDF_DOCKER_BASE_IMAGE,
}
RESOURCE_SERVICE_PORT_BY_TYPE = {
    "udf": UDF_DOCKER_CONTAINER_PORT,
}


def _log(*args):
    print(*args, flush=True)


def _run_command(args, input_text=None):
    if UDF_DOCKER_LOG_COMMANDS:
        _log("Sandbox docker exec:", " ".join(args))
    try:
        return subprocess.run(
            args,
            input=input_text,
            text=True,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        if UDF_DOCKER_LOG_COMMANDS:
            _log("Sandbox docker failed:", exc)
            if exc.stdout:
                _log("Sandbox docker stdout:", exc.stdout.strip())
            if exc.stderr:
                _log("Sandbox docker stderr:", exc.stderr.strip())
        raise


def _get_resource_image(resource_type):
    image = RESOURCE_IMAGE_BY_TYPE.get(resource_type)
    if not image:
        raise RuntimeError(f"unknown sandbox type '{resource_type}'")
    return image


def _get_resource_service_port(resource_type):
    port = RESOURCE_SERVICE_PORT_BY_TYPE.get(resource_type)
    if not port:
        raise RuntimeError(f"missing service port for sandbox type '{resource_type}'")
    return int(port)


def _docker_available():
    return shutil.which("docker") is not None


def _docker_image_exists(image_tag):
    try:
        _run_command(["docker", "image", "inspect", image_tag])
        return True
    except subprocess.CalledProcessError:
        return False


def _docker_container_running(container_name):
    try:
        result = _run_command(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_name]
        )
        return result.stdout.strip() == "true"
    except subprocess.CalledProcessError:
        return False


def _reserve_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((UDF_DOCKER_HOST, 0))
    _, port = sock.getsockname()
    sock.close()
    return port


def _wait_for_port(host, port, timeout_secs):
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def _import_basename(location, index):
    name = location.strip().rstrip("/").split("/")[-1]
    if not name:
        return f"import_{index}"
    return name


def _download_udf_imports(imports, target_dir):
    if not imports:
        return
    os.makedirs(target_dir, exist_ok=True)
    for index, item in enumerate(imports):
        name = _import_basename(item.location, index)
        dest = os.path.join(target_dir, name)
        if UDF_DOCKER_LOG_COMMANDS:
            _log("UDF import download:", item.location, "->", dest)
        try:
            headers = dict(item.headers)
            req = urllib.request.Request(item.url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp, open(dest, "wb") as f:
                shutil.copyfileobj(resp, f)
        except Exception as exc:
            raise RuntimeError(f"failed to download UDF import '{item.location}': {exc}") from exc


def _build_udf_image(dockerfile, image_tag, imports):
    if UDF_DOCKER_LOG_COMMANDS:
        _log(f"UDF docker build image={image_tag}")
    with tempfile.TemporaryDirectory(prefix="udf-cloud-") as temp_dir:
        dockerfile_path = os.path.join(temp_dir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile)
        _download_udf_imports(imports, os.path.join(temp_dir, "imports"))
        _run_command(["docker", "build", "-t", image_tag, "-f", dockerfile_path, temp_dir])


def _start_resource_container(
    image_tag,
    host_port,
    service_port,
    status_port,
    container_name,
    script,
):
    if UDF_DOCKER_LOG_COMMANDS:
        _log(
            "Sandbox docker run:",
            f"image={image_tag}",
            f"container={container_name}",
            f"port={host_port}->{service_port}",
            f"status={status_port}->{RESOURCE_STATUS_CONTAINER_PORT}",
        )
    cmd = [
        "docker",
        "run",
        "-d",
        "--network",
        "host",
        "-e",
        f"RESOURCE_STATUS_ADDR=0.0.0.0:{status_port}",
        "-e",
        f"UDF_SERVER_ADDR=0.0.0.0:{host_port}",
    ]
    if not UDF_DOCKER_KEEP_CONTAINER:
        cmd.append("--rm")
    cmd.extend(
        [
            "--name",
            container_name,
            image_tag,
            "bash",
            "-lc",
            script,
        ]
    )
    result = _run_command(cmd)
    if UDF_DOCKER_LOG_COMMANDS:
        container_id = result.stdout.strip()
        if container_id:
            _log(f"Sandbox docker container_id={container_id}")


def _ensure_resource_endpoint(resource_type, script):
    image_tag = _get_resource_image(resource_type)
    service_port = _get_resource_service_port(resource_type)
    key_payload = json.dumps(
        {"type": resource_type, "image": image_tag, "script": script},
        sort_keys=True,
    )
    key_hash = hashlib.sha256(key_payload.encode("utf-8")).hexdigest()[:12]

    with RESOURCE_DOCKER_LOCK:
        cached = RESOURCE_DOCKER_CACHE.get(key_hash)
        if cached and _docker_container_running(cached["container"]):
            if UDF_DOCKER_LOG_COMMANDS:
                _log(
                    "Sandbox docker reuse:",
                    f"container={cached['container']}",
                    f"endpoint={cached['endpoint']}",
                    f"status={cached['status_endpoint']}",
                )
            return cached["endpoint"], cached["status_endpoint"]

        host_port = _reserve_port()
        status_port = _reserve_port()
        container_name = (
            f"{UDF_DOCKER_IMAGE_PREFIX}-{resource_type}-{key_hash}-{host_port}"
        )
        _start_resource_container(
            image_tag,
            host_port,
            service_port,
            status_port,
            container_name,
            script,
        )

        endpoint = f"http://{UDF_DOCKER_HOST}:{host_port}"
        status_endpoint = f"http://{UDF_DOCKER_HOST}:{status_port}/health"
        RESOURCE_DOCKER_CACHE[key_hash] = {
            "container": container_name,
            "endpoint": endpoint,
            "status_endpoint": status_endpoint,
        }

    if UDF_DOCKER_LOG_COMMANDS:
        _log(f"Sandbox docker endpoint={endpoint}")
        _log(f"Sandbox docker status_endpoint={status_endpoint}")
    if not _wait_for_port(UDF_DOCKER_HOST, host_port, UDF_DOCKER_STARTUP_TIMEOUT_SECS):
        raise RuntimeError(
            f"Sandbox container {container_name} did not start on {endpoint}"
        )
    if not _wait_for_port(
        UDF_DOCKER_HOST, status_port, UDF_DOCKER_STARTUP_TIMEOUT_SECS
    ):
        raise RuntimeError(
            f"Sandbox container {container_name} did not start on {status_endpoint}"
        )

    return endpoint, status_endpoint


def _spec_to_dockerfile(spec):
    if not spec.code:
        raise ValueError("spec missing code")
    if not spec.handler:
        raise ValueError("spec missing handler")
    if not spec.result_type:
        raise ValueError("spec missing result_type")
    return _build_udf_cloud_dockerfile(
        spec.code,
        spec.handler,
        list(spec.input_types),
        spec.result_type,
        list(spec.imports),
        list(spec.packages),
    )


def _build_udf_cloud_dockerfile(
    code, handler, input_types, result_type, imports, packages
):

    server_stub = _build_udf_cloud_server_stub(handler, input_types, result_type)
    code_marker = _unique_heredoc_marker("UDF_CODE", [code, server_stub])
    server_marker = _unique_heredoc_marker("UDF_SERVER", [code, server_stub])
    pyproject = _build_udf_cloud_pyproject(packages)
    pyproject_marker = _unique_heredoc_marker(
        "UDF_PYPROJECT", [code, server_stub, pyproject]
    )

    lines = [
        f"FROM {UDF_DOCKER_BASE_IMAGE}",
        "WORKDIR /app",
        "RUN python -m pip install --no-cache-dir uv",
        f"RUN cat <<'{pyproject_marker}' > /app/pyproject.toml",
        pyproject.rstrip("\n"),
        pyproject_marker,
        "RUN uv sync",
    ]
    if imports:
        lines.extend(
            [
                "RUN mkdir -p /app/imports",
                "COPY imports/ /app/imports/",
            ]
        )
    lines.extend(
        [
            f"RUN cat <<'{code_marker}' > /app/udf.py",
            code.rstrip("\n"),
            code_marker,
            f"RUN cat <<'{server_marker}' > /app/server.py",
            server_stub.rstrip("\n"),
            server_marker,
            "EXPOSE 8815",
            'CMD ["uv","run","--project","/app","python","/app/server.py"]',
        ]
    )
    return "\n".join(lines) + "\n"


def _build_udf_cloud_server_stub(handler, input_types, result_type):
    handler_literal = _escape_python_double_quoted(handler)
    input_types_literal = _python_string_list(input_types)
    result_type_literal = _escape_python_double_quoted(result_type)
    lines = [
        "import importlib",
        "import os",
        "import sys",
        "from databend_udf import UDFServer, udf as udf_decorator",
        "",
        'IMPORTS_DIR = "/app/imports"',
        "",
        "def _add_imports():",
        "    if not os.path.isdir(IMPORTS_DIR):",
        "        return",
        "    sys.path.insert(0, IMPORTS_DIR)",
        "    for name in os.listdir(IMPORTS_DIR):",
        "        path = os.path.join(IMPORTS_DIR, name)",
        "        if os.path.isfile(path):",
        "            ext = os.path.splitext(name)[1].lower()",
        '            if ext in (".zip", ".whl", ".egg"):',
        "                sys.path.insert(0, path)",
        "",
        "def _load_udf():",
        '    return importlib.import_module("udf")',
        "",
        "def _wrap_udf(func):",
        (
            f'    return udf_decorator(name="{handler_literal}", '
            f"input_types={input_types_literal}, result_type=\"{result_type_literal}\")(func)"
        ),
        "",
        "def main():",
        "    _add_imports()",
        "    udf = _load_udf()",
        '    address = os.getenv("UDF_SERVER_ADDR", "0.0.0.0:8815")',
        "    server = UDFServer(location=address)",
        f'    func = getattr(udf, "{handler_literal}")',
        '    if not hasattr(func, "_name"):',
        "        func = _wrap_udf(func)",
        "    server.add_function(func)",
        "    server.serve()",
        "",
        'if __name__ == "__main__":',
        "    main()",
        "",
    ]
    return "\n".join(lines)


def _build_udf_cloud_pyproject(packages):
    dependencies = ["databend-udf"]
    for pkg in packages:
        trimmed = pkg.strip()
        if trimmed:
            dependencies.append(trimmed)
    deps_literal = ", ".join(
        f'"{_escape_toml_double_quoted(dep)}"' for dep in dependencies
    )
    return "\n".join(
        [
            "[project]",
            'name = "databend-udf-app"',
            'version = "0.1.0"',
            f"dependencies = [{deps_literal}]",
            "",
        ]
    )


def _python_string_list(values):
    items = ", ".join(f'"{_escape_python_double_quoted(v)}"' for v in values)
    return f"[{items}]"


def _escape_python_double_quoted(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _escape_toml_double_quoted(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _unique_heredoc_marker(base, contents):
    suffix = 0
    marker = base
    while any(marker in content for content in contents):
        suffix += 1
        marker = f"{base}_{suffix}"
    return marker




def load_data_from_json():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    task_directory_path = os.path.join(script_directory, "testdata", "tasks")

    for file_name in os.listdir(task_directory_path):
        if file_name.endswith(".json"):
            with open(os.path.join(task_directory_path, file_name), "r") as f:
                task_run_data = json.load(f)
                task = task_pb2.Task()
                json_format.ParseDict(task_run_data["Task"], task)
                TASK_DB[task.task_name] = task
    TASK_RUN_DB["MockTask"] = create_mock_task_runs_from_task(TASK_DB["SampleTask"], 10)
    notification_history_directory_path = os.path.join(
        script_directory, "testdata", "notification_history"
    )
    for file_name in os.listdir(notification_history_directory_path):
        if file_name.endswith(".json"):
            with open(
                os.path.join(notification_history_directory_path, file_name), "r"
            ) as f:
                notification_history_data = json.load(f)
                notification_history = notification_pb2.NotificationHistory()
                json_format.ParseDict(notification_history_data, notification_history)
                NOTIFICATION_HISTORY_DB[notification_history.name] = (
                    notification_history
                )


def create_task_request_to_task(id, create_task_request):
    # Convert CreateTaskRequest to dictionary
    task = task_pb2.Task()

    # Copy fields from CreateTaskRequest to Task
    task.task_name = create_task_request.task_name
    task.task_id = id
    task.query_text = create_task_request.query_text
    task.owner = create_task_request.owner
    task.comment = (
        create_task_request.comment if create_task_request.HasField("comment") else ""
    )
    task.schedule_options.CopyFrom(create_task_request.schedule_options)
    task.warehouse_options.CopyFrom(create_task_request.warehouse_options)
    task.status = task_pb2.Task.Suspended
    task.suspend_task_after_num_failures = (
        create_task_request.suspend_task_after_num_failures
    )
    task.error_integration = create_task_request.error_integration

    task.when_condition = (
        create_task_request.when_condition
        if create_task_request.HasField("when_condition")
        else ""
    )
    task.after.extend(create_task_request.after)
    task.created_at = datetime.now(timezone.utc).isoformat()
    task.updated_at = datetime.now(timezone.utc).isoformat()
    # add session parameters
    task.session_parameters.update(create_task_request.session_parameters)
    return task


def create_notification_request_to_notification(id, create_notification_request):
    # Convert CreateNotification to dictionary
    notification = notification_pb2.Notification()

    notification.name = create_notification_request.name
    notification.notification_id = id
    notification.notification_type = create_notification_request.notification_type
    notification.enabled = create_notification_request.enabled
    notification.webhook_url = create_notification_request.webhook_url
    notification.webhook_method = create_notification_request.webhook_method
    notification.webhook_authorization_header = (
        create_notification_request.webhook_authorization_header
    )
    notification.comments = create_notification_request.comments
    t = timestamp_pb2.Timestamp()
    dt = datetime.utcnow()
    seconds = calendar.timegm(dt.utctimetuple())
    nanos = dt.microsecond * 1000
    t.seconds = seconds
    t.nanos = nanos
    notification.created_time.CopyFrom(t)
    notification.updated_time.CopyFrom(t)
    return notification


def get_root_task_id(task):
    if len(task.after) == 0:
        return str(task.task_id)
    else:
        root_ids = []
        for prev_task in task.after:
            root_ids.append(get_root_task_id(TASK_DB[prev_task]))

        dedup = list(set(root_ids))
        return ",".join(dedup)


def create_task_run_from_task(task):
    task_run = task_pb2.TaskRun()
    task_run.task_id = task.task_id
    task_run.task_name = task.task_name
    task_run.owner = task.owner
    task_run.query_text = task.query_text
    task_run.schedule_options.CopyFrom(task.schedule_options)
    task_run.warehouse_options.CopyFrom(task.warehouse_options)
    task_run.condition_text = task.when_condition
    task_run.root_task_id = str(get_root_task_id(task))

    task_run.state = task_pb2.TaskRun.SUCCEEDED
    task_run.attempt_number = 0
    task_run.comment = task.comment
    task_run.error_code = 0
    task_run.error_message = ""
    task_run.run_id = "1ftx"
    task_run.query_id = "qwert"
    task_run.scheduled_time = datetime.now(timezone.utc).isoformat()
    task_run.completed_time = datetime.now(timezone.utc).isoformat()
    task_run.session_parameters.update(task.session_parameters)
    return task_run


def create_mock_task_runs_from_task(task, num):
    task_runs = []
    for i in range(0, num):
        task_run = create_task_run_from_task(task)
        task_run.task_name = "MockTask"
        task_run.run_id = "1ftx" + str(i)
        task_runs.append(task_run)
    return task_runs


class TaskService(task_pb2_grpc.TaskServiceServicer):
    def CreateTask(self, request, context):
        print("CreateTask", request)
        task_name = request.task_name
        if task_name in TASK_DB and request.if_not_exist is False:
            return task_pb2.CreateTaskResponse(
                error=task_pb2.TaskError(
                    kind="ALREADY_EXISTS", message="Task already exists", code=6
                )
            )
        task_id = len(TASK_DB) + 1
        TASK_DB[task_name] = create_task_request_to_task(task_id, request)

        return task_pb2.CreateTaskResponse(task_id=task_id)

    def DescribeTask(self, request, context):
        print("DescribeTask", request)
        task_name = request.task_name
        if task_name not in TASK_DB:
            return task_pb2.DescribeTaskResponse(
                error=task_pb2.TaskError(
                    kind="NOT_FOUND", message="Task not found", code=5
                )
            )
        task = TASK_DB[task_name]
        return task_pb2.DescribeTaskResponse(task=task)

    def DropTask(self, request, context):
        print("DropTask", request)
        task_name = request.task_name
        if task_name not in TASK_DB:
            return task_pb2.DropTaskResponse()
        del TASK_DB[task_name]
        return task_pb2.DropTaskResponse()

    def AlterTask(self, request, context):
        print("AlterTask", request)
        task_name = request.task_name
        if task_name not in TASK_DB:
            return task_pb2.AlterTaskResponse(
                error=task_pb2.TaskError(
                    kind="NOT_FOUND", message="Task not found", code=5
                )
            )
        task = TASK_DB[task_name]
        if request.alter_task_type == task_pb2.AlterTaskRequest.Suspend:
            task.status = task_pb2.Task.Suspended
        elif request.alter_task_type == task_pb2.AlterTaskRequest.Resume:
            task.status = task_pb2.Task.Started
        elif request.alter_task_type == task_pb2.AlterTaskRequest.ModifyAs:
            if request.HasField("query_text"):
                task.query_text = request.query_text
            else:
                return task_pb2.AlterTaskResponse(
                    error=task_pb2.TaskError(
                        kind="INVALID_ARGUMENT",
                        message="query_text not provided for MODIFY_AS",
                        code=7,
                    )
                )
        elif request.alter_task_type == task_pb2.AlterTaskRequest.ModifyWhen:
            if request.HasField("when_condition"):
                task.when_condition = request.when_condition
            else:
                return task_pb2.AlterTaskResponse(
                    error=task_pb2.TaskError(
                        kind="INVALID_ARGUMENT",
                        message="when_condition not provided for MODIFY_WHEN",
                        code=7,
                    )
                )
        elif request.alter_task_type == task_pb2.AlterTaskRequest.AddAfter:
            if len(request.add_after) > 0:
                task.after.extend(request.add_after)
            else:
                return task_pb2.AlterTaskResponse(
                    error=task_pb2.TaskError(
                        kind="INVALID_ARGUMENT",
                        message="add_after not provided for ADD_AFTER",
                        code=7,
                    )
                )
        elif request.alter_task_type == task_pb2.AlterTaskRequest.RemoveAfter:
            after = task.after
            print(request)
            if len(request.remove_after) > 0:
                filtered_array = [
                    elem for elem in after if elem not in request.remove_after
                ]
                task.after[:] = []
                task.after.extend(filtered_array)
            else:
                return task_pb2.AlterTaskResponse(
                    error=task_pb2.TaskError(
                        kind="INVALID_ARGUMENT",
                        message="remove_after not provided for REMOVE_AFTER",
                        code=7,
                    )
                )
        elif request.alter_task_type == task_pb2.AlterTaskRequest.Set:
            has_options = False
            if request.HasField("schedule_options"):
                task.schedule_options.CopyFrom(request.schedule_options)
                has_options = True
            if request.HasField("warehouse_options"):
                task.warehouse_options.CopyFrom(request.warehouse_options)
                has_options = True
            if request.HasField("comment"):
                task.comment = request.comment
                has_options = True
            if request.HasField("suspend_task_after_num_failures"):
                task.suspend_task_after_num_failures = (
                    request.suspend_task_after_num_failures
                )
                has_options = True
            if request.HasField("error_integration"):
                task.error_integration = request.error_integration
                has_options = True
            if request.set_session_parameters:
                task.session_parameters.update(request.session_parameters)
                has_options = True
            if has_options is False:
                return task_pb2.AlterTaskResponse(
                    error=task_pb2.TaskError(
                        kind="INVALID_ARGUMENT",
                        message="No options provided for SET",
                        code=8,
                    )
                )
        else:
            # not supported
            return task_pb2.AlterTaskResponse(
                error=task_pb2.TaskError(
                    kind="INVALID_ARGUMENT",
                    message="AlterTaskType not supported",
                    code=3,
                )
            )
        current_time = datetime.now(timezone.utc)
        current_time = current_time.isoformat()
        task.updated_at = current_time
        task_name = task.task_name
        TASK_DB[task_name] = task
        return task_pb2.AlterTaskResponse(task=task)

    def ExecuteTask(self, request, context):
        print("ExecuteTask", request)
        for task_name, task in TASK_DB.items():
            TASK_RUN_DB[task_name] = [create_task_run_from_task(task)]
        return task_pb2.ExecuteTaskResponse(error=None)

    def ShowTasks(self, request, context):
        print("ShowTasks", request)
        tasks = list(TASK_DB.values())
        return task_pb2.ShowTasksResponse(tasks=tasks)

    def ShowTaskRuns(self, request, context):
        print("ShowTaskRuns", request)
        task_runs = [item for sublist in TASK_RUN_DB.values() for item in sublist]
        task_runs = sorted(task_runs, key=lambda x: x.run_id)
        num_results = len(task_runs)

        if len(request.task_name) > 0:
            print("Limiting task_name to", request.task_name)
            task_runs = list(
                filter(lambda x: x.task_name == request.task_name, task_runs)
            )
        # limit and sort by run_id
        if request.result_limit > 0:
            print("Limiting result to", request.result_limit)
            task_runs = task_runs[: request.result_limit]
            if request.result_limit < num_results:
                num_results = request.result_limit
        # pagination
        start_index = 0
        page_size = 2
        if request.HasField("next_page_token"):
            print("Next page token", request.next_page_token)
            start_index = request.next_page_token

        end_index = start_index + page_size
        next_page_token = end_index
        if end_index > num_results:
            next_page_token = None
        task_runs = task_runs[start_index:end_index]
        return task_pb2.ShowTaskRunsResponse(
            task_runs=task_runs, next_page_token=next_page_token
        )

    def GetTaskDependents(self, request, context):
        print("GetTaskDependents", request)
        task_name = request.task_name
        if task_name not in TASK_DB:
            return task_pb2.GetTaskDependentsResponse(task=[])
        task = TASK_DB[task_name]
        root = task
        l = [root]
        if request.recursive is False:
            return task_pb2.GetTaskDependentsResponse(task=l)

        while len(root.after) > 0:
            root = TASK_DB[root.after[0]]
            l.insert(0, root)
        return task_pb2.GetTaskDependentsResponse(task=l)

    def EnableTaskDependents(self, request, context):
        print("EnableTaskDependents", request)
        task_name = request.task_name
        if task_name not in TASK_DB:
            return task_pb2.EnableTaskDependentsResponse()
        task = TASK_DB[task_name]
        task.status = task_pb2.Task.Started
        return task_pb2.EnableTaskDependentsResponse()


class NotificationService(notification_pb2_grpc.NotificationServiceServicer):
    def CreateNotification(self, request, context):
        print("CreateTask", request)
        name = request.name
        if name in NOTIFICATION_DB and request.if_not_exists is False:
            return notification_pb2.CreateNotificationResponse(
                error=notification_pb2.NotificationError(
                    kind="ALREADY_EXISTS", message="Notification already exists", code=6
                )
            )
        notification_id = len(NOTIFICATION_DB) + 1
        NOTIFICATION_DB[name] = create_notification_request_to_notification(
            notification_id, request
        )
        return notification_pb2.CreateNotificationResponse(
            notification_id=notification_id
        )

    def GetNotification(self, request, context):
        print("GetNotification", request)
        name = request.name
        if name not in NOTIFICATION_DB:
            return notification_pb2.GetNotificationResponse(
                error=notification_pb2.NotificationError(
                    kind="NOT_FOUND", message="Notification not found", code=5
                )
            )
        notification = NOTIFICATION_DB[name]
        return notification_pb2.GetNotificationResponse(notification=notification)

    def ListNotification(self, request, context):
        print("ListNotification", request)
        notifications = list(NOTIFICATION_DB.values())
        return notification_pb2.ListNotificationResponse(notifications=notifications)

    def AlterNotification(self, request, context):
        print("AlterNotification", request)
        name = request.name
        if name not in NOTIFICATION_DB:
            return notification_pb2.AlterNotificationResponse(
                error=notification_pb2.NotificationError(
                    kind="NOT_FOUND", message="Notification not found", code=5
                )
            )
        notification = NOTIFICATION_DB[name]
        if request.operation_type == "SET":
            if request.HasField("enabled"):
                notification.enabled = request.enabled
            if request.HasField("webhook_url"):
                notification.webhook_url = request.webhook_url
            if request.HasField("webhook_method"):
                notification.webhook_method = request.webhook_method
            if request.HasField("webhook_authorization_header"):
                notification.webhook_authorization_header = (
                    request.webhook_authorization_header
                )
            if request.HasField("comments"):
                notification.comments = request.comments
        return notification_pb2.AlterNotificationResponse(
            notification_id=notification.notification_id
        )

    def DropNotification(self, request, context):
        print("DropNotification", request)
        name = request.name
        if name not in NOTIFICATION_DB:
            return notification_pb2.DropNotificationResponse()
        del NOTIFICATION_DB[name]
        return notification_pb2.DropNotificationResponse()

    def ListNotificationHistory(self, request, context):
        print("ListNotificationHistory", request)
        notification_histories = list(NOTIFICATION_HISTORY_DB.values())

        if (
            request.HasField("result_limit")
            and len(notification_histories) > request.result_limit
        ):
            print("Limiting result to", request.result_limit)
            notification_histories = notification_histories[: request.result_limit]
        return notification_pb2.ListNotificationHistoryResponse(
            notification_histories=notification_histories
        )


class UdfService(udf_pb2_grpc.UdfServiceServicer):
    def ApplyUdfResource(self, request, context):
        _log("ApplyUdfResource", request)
        if not _docker_available():
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "docker not found; a working docker CLI is required",
            )

        resource_type = request.type or "udf"
        script = request.script
        if not script:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "missing script")
        _log("Sandbox script:\n", script)

        try:
            endpoint, _status_endpoint = _ensure_resource_endpoint(resource_type, script)
        except Exception as exc:
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"failed to provision sandbox container: {exc}",
            )

        return udf_pb2.ApplyUdfResourceResponse(
            endpoint=endpoint, headers=UDF_HEADERS
        )


def timestamp_to_datetime(timestamp):
    # Convert google.protobuf.Timestamp to Python datetime
    return datetime.fromtimestamp(timestamp.seconds + timestamp.nanos / 1e9)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    task_pb2_grpc.add_TaskServiceServicer_to_server(TaskService(), server)
    notification_pb2_grpc.add_NotificationServiceServicer_to_server(
        NotificationService(), server
    )
    udf_pb2_grpc.add_UdfServiceServicer_to_server(UdfService(), server)
    # Add reflection service
    SERVICE_NAMES = (
        task_pb2.DESCRIPTOR.services_by_name["TaskService"].full_name,
        notification_pb2.DESCRIPTOR.services_by_name["NotificationService"].full_name,
        udf_pb2.DESCRIPTOR.services_by_name["UdfService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:50051")
    server.start()
    _log("Server Started at port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    load_data_from_json()
    serve()
