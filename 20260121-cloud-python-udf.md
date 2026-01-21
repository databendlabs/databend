---
title: Cloud Python UDF
description: RFC for cloud python udf
---

# Motivation
The current Python UDF implementation has the following issues:
- Global Interpreter Lock (GIL) prevents Python UDFs from fully utilizing multi-core CPUs.
- Shared execution environment leads to dependency conflicts across different Python UDFs on the same node.
- Process-level instability, where Python-level segmentation faults can bring down the entire query process.
- Uncontrolled resource consumption, as Python memory usage may interfere with the SQL engine’s memory management.

This RFC proposes addressing these issues by moving the execution and resource management of Python UDFs to a cloud-based environment.

# Guide-level explanation
From the user’s perspective, the UDF syntax remains unchanged. When CloudScriptUDF is not enabled, the existing ScriptUDF behavior is unaffected. 
Once CloudScriptUDF is enabled, users no longer need to deploy and manage their own Python UDF servers, as execution and resource management are handled by the cloud.

# Reference-level explanation
The architecture is divided into three layers:

- Control Plane (Cloud Control Plane)
  - Responsible for resource scheduling, permission validation, and sandbox lifecycle management.

- Execution Plane (Databend Query)
  - Acts as the client, issuing computation requests via the Arrow Flight protocol.

- Compute Plane (Sandbox Workers)
  - Provides lightweight Python environments isolated by gVisor, running the databend-udf service.

```
------------------------+   ApplyResource   +------------------------+
|   Databend Query       | ----------------> |   Cloud Controller      |
|  (Execution Plane)     | <---------------- |  Resource Manager       |
|  - UDF Planner         |    Endpoint+Token |  Image Cache/Warm Pool  |
+-----------+------------+                   +-----------+------------+
            |   Arrow Flight (DoExchange)                |
            |                                            | Provision
            v                                            v
+------------------------+                   +------------------------+
|   Sandbox Worker Pod   | <---------------  |   K8s + runsc (gVisor)  |
|  (Compute Plane)       |                   +------------------------+
|  databend-udf service  |
+------------------------+
```
### UDF Resource Provisioning Interface
A remote RPC service is provided to submit an ApplyUdfResourceRequest
for UDF resource allocation.

The RPC call returns immediately after the request is accepted.
However, resource provisioning is performed asynchronously, and the
returned endpoint is not guaranteed to be ready for use at call
completion.

The allocated resource is later accessed by the Query module through
the existing Server UDF mechanism.
```proto
syntax = "proto3";
option go_package = "databend.com/cloudcontrol/proto";

package udfproto;

message UdfImport {
  string location = 1;
  string url = 2;
  // Content tag for caching (e.g. etag or content length), independent of presigned URL.
  string tag = 3;
}

message ApplyUdfResourceRequest {
  // JSON runtime spec (code/handler/types/packages). The control plane builds Dockerfile.
  string spec = 1;
  repeated UdfImport imports = 2;
}

message ApplyUdfResourceResponse {
  string endpoint = 1;
  map<string, string> headers = 2;
}

service UdfService {
  rpc ApplyUdfResource(ApplyUdfResourceRequest) returns (ApplyUdfResourceResponse);
}
```
### Query Execution Flow
On the Query side, when the `cloud_script_udf` configuration is enabled,
executing a Script UDF follows the workflow below:

1. The Python code and metadata are packaged into a JSON runtime spec.
2. All imported modules are inspected. For each import located in a
   stage, a presigned URL and its corresponding content tag (e.g. etag)
   are generated, enabling spec reuse across executions.
3. The Query module invokes the `ApplyUdfResource` RPC, submitting the
   runtime spec together with the resolved imports to the Cloud Control
   Plane.
4. The Query module blocks until the Cloud Control Plane completes
   resource provisioning and returns an execution endpoint.
5. The UDF is then invoked using the existing Server UDF execution path.

### Cloud-Side Resource Manager (The Broker)
The Resource Manager, acting as the broker between Query and Sandbox
workers, is responsible for the following:

- Tenant Isolation
  - Ensures that UDFs from different tenants are executed in isolated
  environments, such as separate VPCs or Kubernetes namespaces.

- Runtime Image Management
  - Docker image builds are reused when the submitted Dockerfile remains
  unchanged, reducing build overhead and improving provisioning
  latency.

- Security Enforcement
  - Enforces the use of RuntimeClass: runsc to provide gVisor-based
  isolation, restricts system calls, and disables access to sensitive
  network resources.

## Config Maintenance
Two new configuration options are introduced under the [query] section:
```toml
[query]
enable_udf_cloud_script = true
cloud_control_grpc_server_address = "http://0.0.0.0:50051"
```
- enable_udf_cloud_script
  - Enables execution of Script UDFs through the Cloud Control Plane. When
  enabled, Script UDFs are packaged and executed in isolated sandbox
  environments instead of the local runtime.

- cloud_control_grpc_server_address
  - Specifies the gRPC endpoint of the Cloud Control Plane used for UDF
  resource provisioning.

# Drawbacks
### Cold start latency is significant
Docker image builds and dependency
installation via `uv sync` are slow, resulting in high latency on first
execution or when the cache is invalidated.

# Rationale and alternatives
Currently, there are no existing engineering solutions that use an RPC-based approach for dynamic resource allocation; 
however, a related research paper explores this direction.
- https://dl.acm.org/doi/10.14778/3551793.3551860


# Prior art
The logic of the Server UDF is currently reused to execute after the Cloud returns the endpoint.

# Unresolved questions

The first build process may take a long time, causing the UDF execution to time out.

# Future possibilities
