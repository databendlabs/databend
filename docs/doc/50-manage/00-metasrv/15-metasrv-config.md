---
title: Databend Meta Service Configuration
sidebar_label: Meta Service Configuration
description: 
  Databend Meta Service Configuration
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

A `databend-meta` server is configured with a config file in `toml`: `databend-meta --config-file databend-meta.toml`.

```toml title="databend-meta.toml"

#
# Logging
#
log_dir                  = "metadata/_logs1"
log_level                = "DEBUG"
#
# Metric API endpoint
#
metric_api_address       = "0.0.0.0:28100"
#
# Admin API endpoint
#
admin_api_address        = "0.0.0.0:28101"
admin_tls_server_cert    = "admin.cert" 
admin_tls_server_key     = "admin.key" 
#
# GRPC client API endpoint
#
grpc_api_address         = "0.0.0.0:9191"
grpc_tls_server_cert     = "grpc.cert" 
grpc_tls_server_key      = "grpc.key"
#
# Internal raft communication
#
[raft_config]
id                       = 1
raft_dir                 = "metadata/datas1"
raft_api_port            = 28103
raft_listen_host         = "127.0.0.1"
raft_advertise_host      = "localhost"
#
# Raft internal config
#
heartbeat_interval       = 1000 # milli second 
install_snapshot_timeout = 4000 # milli second
max_applied_log_to_keep  = 1000 # N.O. raft logs
snapshot_logs_since_last = 1024 # N.O. raft logs
#
# Startup config
#
single                   = false
join                     = ["127.0.0.1:28103", "127.0.0.1:28203"]
```
## 1. Logging config

- `log_id` is the path to a directory for storing hourly-rolling debug log.
- `log_level` is the log level. by default it is `DEBUG`.

## 2. Metric service config

Metric API provides service metrics data for other component such as prometheus.

- `metric_api_address` is the HTTP service for metrics collection.

## 3. Admin config

Admin API provides cluster information such as node list, the currently active leader.

- `admin_api_address` is the HTTP service for retrieving cluster status.
- `admin_tls_server_cert` specifies the path to load tls certificate for admin service 
- `admin_tls_server_key` specifies the path to load tls key for admin service

## 4. GRPC config

GRPC API is application API a `databend-meta` client connects to, for reading and writing metadata.

- `grpc_api_address` is the HTTP server address for reading and writing metadata.
- `grpc_tls_server_cert` specifies the path to load tls certificate.
- `grpc_tls_server_key` specifies the path to load tls key.

## 5. Raft config

- `raft_config.id` is the globally unique id for this node; it is a `u64`.

- `raft_config.raft_dir` is the local dir to store metadata, including raft log
  and state machine etc.

- `raft_config.raft_api_port`,`raft_config.raft_listen_host` and `raft_config.raft_advertise_host`
  defines the service for internal raft communication.  Application should never touch this port.

  `raft_listen_host` is the host the internal raft server listens on.
  `raft_advertise_host` is the host the internal raft client to connect to.

## 6. Raft internal config

Defines raft behaviors on raft-storage and the state machine.

- `heartbeat_interval` specifies the interval between two heartbeat in milliseconds.

- `install_snapshot_timeout` specifies the max time for installing a snapshot in milliseconds.

- `max_applied_log_to_keep` specifies the max number of applied raft-log to keep.

- `snapshot_logs_since_last` specifies the number of raft-logs since the last snapshot beyond which a snapshot will be generated.

### 7. Startup config

- `single` tells the node to initialize a single node cluster if it is not
  initialized. Otherwise, this arg is just ignored.

- `join` specifies a list of address(`<raft_advertise_host>:<raft_api_port>`) of nodes in an existing cluster a new node is joint to.

  `join` is only used for an uninitialized node.
  `join` will be ignored if the node is already initialized.
