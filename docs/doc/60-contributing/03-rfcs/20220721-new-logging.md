---
title: The New Logging
description: Make Databend Logging Great Again!
---

- RFC PR: [datafuselabs/databend#0000](https://github.com/datafuselabs/databend/pull/0000)
- Tracking Issue: [datafuselabs/databend#0000](https://github.com/datafuselabs/databend/issues/0000)

# Summary

Introduce a new configuration to make the databend's logging more user-friendly, and create more space for further improvement.

# Motivation

Databend's logging is verbose.

Every log will be written to both files and the console. Users can't control the logging behavior.

For examples:

- Users can't specify the `JSON` / `TEXT` format for stderr or files.
- Users can't disable the file's logging

Even worse, we can't leave meaningful messages to users. Our users will be overwhelmed by a large number of logs. We should use `stdout` to communicate with the user.

# Guide-level explanation

With this RFC, users will have new config options:

```toml
[log.file]
on = true
level = "debug"
dir = "./databend/logs"
format = "json"

[log.stderr]
on = true
level = "debug"
format = "text"
```

- Users can disable any of the output
- Users can control the log level and formats

By default, we will enable the `file` log only. And start `databend-query` will not print records to `stderr` anymore. We will print the following messages to `stdout` instead:

```shell
Databend Server starting at xxxxxxx (took x.xs)

Information

version: v0.7.128-xxxxx
logs:    
  file:   enabled dir=./databend/logs level=DEBUG
  stderr: disabled (set LOG_STDERR_ON=true to enable)
storage: s3://endpoint=127.0.0.1:1090,bucket=test,root=/path/to/data
metasrv: embed

Connection

MySQL:             mysql://root@localhost:3307/xxxx
clickhouse:        clickhouse://root@locahost:9000/xxxx
clickhouse (HTTP): http://root:@locahost:9001

Useful Links

Documentation:    https://databend.rs
Looking for help: https://github.com/datafuselabs/databend/discussions
```

To enable `stderr` logs, we can set `LOG_STDERR_ON=true` or `RUST_LOG=info`.

# Reference-level explanation

Internally, we will add new config structs in `Config`. The old config will be compatible.

# Drawbacks

None

# Rationale and alternatives

## Minio

[Minio](https://github.com/minio/minio) doesn't print logs to `stdout` nor `stderr`. Instead, they only print welcome messages:

```shell
:) minio server . --address ":9900"
MinIO Object Storage Server
Copyright: 2015-0000 MinIO, Inc.
License: GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>
Version: RELEASE.2022-06-30T20-58-09Z (go1.18.3 Linux/amd64)

API: http://192.168.1.104:9900  http://172.16.195.1:9900  http://192.168.97.1:9900  http://127.0.0.1:9900
root user: minioadmin
RootPass: minioadmin

WARNING: Console endpoint is listening on a dynamic port (34219), please use --console-address ":PORT" to choose a static port.
Console: http://192.168.1.104:34219 http://172.16.195.1:34219 http://192.168.97.1:34219 http://127.0.0.1:34219
root user: minioadmin
RootPass: minioadmin

Command-line: https://docs.min.io/docs/minio-client-quickstart-guide
   $ mc alias set myminio http://192.168.1.104:9900 minioadmin minioadmin

Documentation: https://docs.min.io

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ You are running an older version of MinIO released 2 weeks ago ┃
┃ Update: Run `mc admin update`                                  ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
```

## CockroachDB

[CockroachDB](https://www.cockroachlabs.com/) doesn't print logs to `stderr` by default:

They allow users to use `--log=<yaml-config>` to specify the logging behavior.

```shell
:) ./cockroach start-single-node
CockroachDB node starting at 2022-07-21 06:56:04.36859988 +0000 UTC (took 0.7s)
build:               CCL v22.1.4 @ 2022/07/19 17:09:48 (go1.17.11)
WebUI:               http://xuanwo-work:8080
sql:                 postgresql://root@xuanwo-work:26257/defaultdb?sslmode=disable
sql (JDBC):          JDBC:postgresql://xuanwo-work:26257/defaultdb?sslmode=disable&user=root
RPC client flags:    ./cockroach <client cmd> --host=xuanwo-work:26257 --insecure
logs:                /tmp/cockroach-v22.1.4.linux-amd64/cockroach-data/logs
temp dir:            /tmp/cockroach-v22.1.4.linux-amd64/cockroach-data/cockroach-temp3237741659
external I/O path:   /tmp/cockroach-v22.1.4.linux-amd64/cockroach-data/extern
store[0]:            path=/tmp/cockroach-v22.1.4.linux-amd64/cockroach-data
storage engine:      pebble
clusterID:           e1ab003d-7eba-48cd-b635-7a51f40269c2
status:              restarted pre-existing node
nodeID:              1
```

# Prior art

None

# Unresolved questions

None

# Future possibilities

## Add HTTP log support

Allow sending logs to the HTTP endpoint

## Support read SQL from stdin

Based on this RFC, we can implement reading SQL from stdin.
