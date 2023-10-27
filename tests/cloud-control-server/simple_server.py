import os

import grpc
import json
from concurrent import futures
from grpc_reflection.v1alpha import reflection
from google.protobuf import json_format
from datetime import datetime, timezone

import task_pb2
import task_pb2_grpc

# Simple in-memory database
TASK_DB = {}
TASK_RUN_DB = {}


def load_data_from_json():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.join(script_directory, "testdata", "tasks")

    for file_name in os.listdir(directory_path):
        if file_name.endswith(".json"):
            with open(os.path.join(directory_path, file_name), "r") as f:
                task_data = json.load(f)
                task = task_pb2.Task()
                json_format.ParseDict(task_data["Task"], task)
                TASK_DB[task.task_name] = task


def create_task_request_to_task(id, create_task_request):
    # Convert CreateTaskRequest to dictionary
    request_dict = vars(create_task_request)
    current_time = datetime.now(timezone.utc)
    current_time = current_time.isoformat()
    request_dict["created_at"] = current_time
    request_dict["updated_at"] = current_time
    request_dict["next_scheduled_at"] = current_time
    request_dict["task_id"] = id
    # Create a new Task object from the dictionary
    task = task_pb2.Task(**request_dict)

    return task


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
        elif request.alter_task_type == task_pb2.AlterTaskRequest.Set:
            has_options = False
            if request.HasField("schedule_options"):
                task.schedule_options = request.schedule_options
                has_options = True

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
        return task_pb2.AlterTaskResponse(task=task)

    def ShowTasks(self, request, context):
        print("ShowTasks", request)
        tasks = list(TASK_DB.values())
        return task_pb2.ShowTasksResponse(tasks=tasks)

    def ShowTaskRuns(self, request, context):
        print("ShowTaskRuns", request)
        task_runs = list(TASK_RUN_DB.values())
        return task_pb2.ShowTaskRunsResponse(task_runs=task_runs)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_pb2_grpc.add_TaskServiceServicer_to_server(TaskService(), server)
    # Add reflection service
    SERVICE_NAMES = (
        task_pb2.DESCRIPTOR.services_by_name["TaskService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:50051")
    server.start()
    print("Server Started at port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    load_data_from_json()
    serve()
