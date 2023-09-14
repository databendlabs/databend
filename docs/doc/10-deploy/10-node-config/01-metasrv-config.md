---
title: Meta Configurations
---

This page describes the Meta node configurations available in the [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml) configuration file.

```toml title='databend-meta.toml'
# Usage:
# databend-meta -c databend-meta.toml

admin_api_address       = "0.0.0.0:28101"
grpc_api_address        = "0.0.0.0:9191"
# databend-query fetch this address to update its databend-meta endpoints list,
# in case databend-meta cluster changes.
grpc_api_advertise_host = "127.0.0.1"

[raft_config]
id            = 1
raft_dir      = "/var/lib/databend/raft"
raft_api_port = 28103

# Assign raft_{listen|advertise}_host in test config.
# This allows you to catch a bug in unit tests when something goes wrong in raft meta nodes communication.
raft_listen_host = "127.0.0.1"
raft_advertise_host = "localhost"

# Start up mode: single node cluster
single        = true
```

- Some parameters listed in the table below may not be present in [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml). If you require these parameters, you can manually add them to the file.

- You can find [sample configuration files](https://github.com/datafuselabs/databend/tree/main/scripts/ci/deploy/config) on GitHub that set up Databend for various deployment environments. These files were created for internal testing ONLY. Please do NOT modify them for your own purposes. But if you have a similar deployment, it is a good idea to reference them when editing your own configuration files.

## General Parameters

The following is a list of general parameters available in the [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml) configuration file. These parameters should not be included under any specific section.

| Parameter                         | Description                                                                                                             |
|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| admin_api_address                 | IP address and port for the admin API of Databend.                                                                      |
| admin_tls_server_cert             | Path to the admin TLS server certificate file.                                                                          |
| admin_tls_server_key              | Path to the admin TLS server key file.                                                                                  |
| grpc_api_address                  | IP address and port for the gRPC API of Databend.                                                                       |
| grpc_api_advertise_host           | IP address used for advertising the gRPC API (used for updating Databend-meta endpoints).                               |
| grpc_tls_server_cert              | Path to the gRPC TLS server certificate file.                                                                           |
| grpc_tls_server_key               | Path to the gRPC TLS server key file.                                                                                   | 

## [log] Section

This section can include two subsections: [log.file] and [log.stderr].

### [log.file] Section

The following is a list of the parameters available within the [log.file] section:

| Parameter | Description                                                                |
|-----------|----------------------------------------------------------------------------|
| on        | Enable file-based logging (true or false). Default: true                   |
| level     | Log level for file-based logging (e.g., "DEBUG", "INFO"). Default: "DEBUG" |
| dir       | Directory where log files will be stored. Default: "./.databend/logs"      |
| format    | Log format for file-based logging (e.g., "json", "text"). Default: "json"  |

### [log.stderr] Section

The following is a list of the parameters available within the [log.stderr] section:

| Parameter | Description                                                            |
|-----------|------------------------------------------------------------------------|
| on        | Enable stderr logging (true or false). Default: true                   |
| level     | Log level for stderr logging (e.g., "DEBUG", "INFO"). Default: "DEBUG" |
| format    | Log format for stderr logging (e.g., "text", "json"). Default: "text"  |

## [raft_config] Section

The following is a list of the parameters available within the [raft_config] section:

| Parameter                | Description                                                                                                             |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------|
| id                       | Unique identifier for the Raft configuration.                                                                           |
| raft_dir                 | Directory where Raft data is stored.                                                                                    |
| raft_api_port            | Port for the Raft API of Databend.                                                                                      |
| raft_listen_host         | IP address for Raft to listen on.                                                                                       |
| raft_advertise_host      | IP address used for advertising the Raft API.                                                                           |
| single                   | Boolean indicating whether Databend should run in single-node cluster mode (true or false).                             |
| join                     | List of addresses (<raft_advertise_host>:<raft_api_port>) of nodes in an existing cluster that a new node is joined to. |
| heartbeat_interval       | Heartbeat interval in milliseconds. Default: 1000                                                                       |
| install_snapshot_timeout | Install snapshot timeout in milliseconds. Default: 4000                                                                 |
| max_applied_log_to_keep  | Maximum number of applied Raft logs to keep. Default: 1000                                                              |
| snapshot_logs_since_last | Number of Raft logs since the last snapshot. Default: 1024                                                              |
| wait_leader_timeout      | Wait leader timeout in milliseconds. Default: 70000                                                                     |