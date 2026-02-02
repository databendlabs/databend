import hashlib
import os
import re
import shutil
import socket
import subprocess
import threading
import time

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
import resource_pb2
import resource_pb2_grpc

try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        tomllib = None

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
RESOURCE_STATUS_CONTAINER_PORT = 8080
RESOURCE_DOCKER_CACHE = {}
RESOURCE_DOCKER_LOCK = threading.Lock()
RESOURCE_IMAGE_BY_TYPE = {
    "udf": UDF_DOCKER_BASE_IMAGE,
}
RESOURCE_SERVICE_PORT_BY_TYPE = {
    "udf": UDF_DOCKER_CONTAINER_PORT,
}
UDF_ENV_CONFIG_PATH = os.getenv(
    "UDF_ENV_CONFIG",
    os.path.join(os.path.dirname(__file__), "udf_env.toml"),
)
UDF_ENV_CONFIG_LOCK = threading.Lock()
UDF_ENV_CONFIG_CACHE = {"mtime": None, "data": {}}
ENV_KEY_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
RESERVED_ENV_KEYS = {"RESOURCE_STATUS_ADDR", "UDF_SERVER_ADDR"}


def _log(*args):
    print(*args, flush=True)


def _get_metadata_value(context, key):
    target = key.lower()
    for meta_key, meta_value in context.invocation_metadata():
        if meta_key.lower() == target:
            return meta_value
    return ""


def _stringify_env_value(value):
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, sort_keys=True)


def _sanitize_env_vars(env_vars):
    cleaned = {}
    for key, value in (env_vars or {}).items():
        if not isinstance(key, str) or not ENV_KEY_RE.match(key):
            _log(f"Skip invalid env key: {key!r}")
            continue
        if key in RESERVED_ENV_KEYS:
            _log(f"Skip reserved env key: {key}")
            continue
        cleaned[key] = _stringify_env_value(value)
    return cleaned


def _load_udf_env_config():
    if tomllib is None:
        raise RuntimeError("tomllib is required to parse udf_env.toml")
    path = UDF_ENV_CONFIG_PATH
    if not os.path.isfile(path):
        with UDF_ENV_CONFIG_LOCK:
            UDF_ENV_CONFIG_CACHE["mtime"] = None
            UDF_ENV_CONFIG_CACHE["data"] = {}
        return {}
    mtime = os.path.getmtime(path)
    with UDF_ENV_CONFIG_LOCK:
        if UDF_ENV_CONFIG_CACHE["mtime"] != mtime:
            with open(path, "rb") as handle:
                data = tomllib.load(handle)
            UDF_ENV_CONFIG_CACHE["mtime"] = mtime
            UDF_ENV_CONFIG_CACHE["data"] = data
        return UDF_ENV_CONFIG_CACHE["data"]


def _get_udf_env_vars(tenant, resource_name):
    if not tenant or not resource_name:
        return {}
    data = _load_udf_env_config()
    udf_env = data.get("udf_env") or {}
    if not isinstance(udf_env, dict):
        return {}
    key = f"{tenant}.{resource_name}"
    udf_cfg = udf_env.get(key) or {}
    if not isinstance(udf_cfg, dict):
        return {}
    return _sanitize_env_vars(udf_cfg)


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


def _start_resource_container(
    image_tag,
    host_port,
    service_port,
    status_port,
    container_name,
    script,
    env_vars,
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
    if env_vars:
        for key, value in env_vars.items():
            cmd.extend(["-e", f"{key}={value}"])
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


def _ensure_resource_endpoint(resource_type, script, env_vars):
    image_tag = _get_resource_image(resource_type)
    service_port = _get_resource_service_port(resource_type)
    key_payload = json.dumps(
        {
            "type": resource_type,
            "image": image_tag,
            "script": script,
            "env": env_vars or {},
        },
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
            env_vars,
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


class ResourceService(resource_pb2_grpc.ResourceServiceServicer):
    def ApplyResource(self, request, context):
        _log("ApplyResource", request)
        if not _docker_available():
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "docker not found; a working docker CLI is required",
            )

        resource_type = request.type or "udf"
        resource_name = request.resource_name
        script = request.script
        if not script:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "missing script")
        _log("Sandbox script:\n", script)

        env_vars = {}
        if resource_type == "udf":
            tenant = _get_metadata_value(context, "x-databend-tenant")
            try:
                env_vars = _get_udf_env_vars(tenant, resource_name)
            except Exception as exc:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"failed to load udf env config: {exc}",
                )
            if env_vars:
                _log(
                    "Sandbox env keys:",
                    ",".join(sorted(env_vars.keys())),
                )

        try:
            endpoint, _status_endpoint = _ensure_resource_endpoint(
                resource_type, script, env_vars
            )
        except Exception as exc:
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"failed to provision sandbox container: {exc}",
            )

        return resource_pb2.ApplyResourceResponse(
            endpoint=endpoint, headers=UDF_HEADERS
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    task_pb2_grpc.add_TaskServiceServicer_to_server(TaskService(), server)
    notification_pb2_grpc.add_NotificationServiceServicer_to_server(
        NotificationService(), server
    )
    resource_pb2_grpc.add_ResourceServiceServicer_to_server(
        ResourceService(), server
    )
    # Add reflection service
    SERVICE_NAMES = (
        task_pb2.DESCRIPTOR.services_by_name["TaskService"].full_name,
        notification_pb2.DESCRIPTOR.services_by_name["NotificationService"].full_name,
        resource_pb2.DESCRIPTOR.services_by_name["ResourceService"].full_name,
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
