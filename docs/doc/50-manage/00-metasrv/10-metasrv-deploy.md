---
title: Deploy a Databend Meta Service Cluster
sidebar_label: Deploy a Meta Service Cluster
description: 
  How to deploy Databend Meta Service Cluster
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

`databend-meta` is a global service for the metadata(such as user, table schema etc.).
It can be a single process service for testing,
or a cluster of 3 or 5 nodes for production use.

`databend-meta` has to be deployed before booting up a `databend-query` cluster.

```text
                               .~~~~~~~~~~~~~~~~.
                               !   MySQL        !
                               !   ClickHouse   !
                               !   REST API     !
 .------.                      '~~~~~~~~~~~~~~~~'
 | meta |                               ^
 '------'                               |
    ^                                   v
    |                          .----------------.
 .------.                      |                |
 | meta |<-------------------->| databend-query |
 '------'                      |                |
    |                          '----------------'
    v                                   ^
 .------.                               !
 | meta |                               v
 '------'                      .~~~~~~~~~~~~~~~~.
                               !                !
                               !     AWS S3     !
                               !                !
                               '~~~~~~~~~~~~~~~~'
```


## 1. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page or [build from source](../../60-contributing/00-building-from-source.md).

```shell
mkdir databend && cd databend
```
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.7.32-nightly/databend-v0.7.32-nightly-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
tar xzvf databend-v0.7.32-nightly-x86_64-unknown-linux-musl.tar.gz
```

You can find two executable files:
```shell
ls
# databend-meta databend-query
```

</TabItem>
</Tabs>


## 2. Deploy databend-meta

:::tip

Standalone mode should **NEVER** be used in production.
But do not worry. Anytime when needed, a standalone `databand-meta` can be extended to a cluster of 3 or 5 nodes with [Cluster management API](./20-metasrv-add-remove-node.md).

:::

### 2.1 Standalone mode

#### 2.1.1 Create databend-meta.toml

```shell title="databend-meta.toml"
log_dir = "metadata/_logs"
admin_api_address = "127.0.0.1:28101"
grpc_api_address = "127.0.0.1:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"
```

#### 2.1.2 Start the databend-meta

```shell
./databend-meta -c ./databend-meta.toml > meta.log 2>&1 &
```

#### 2.1.3 Check databend-meta

```shell
curl -I  http://127.0.0.1:28101/v1/health
```

Check the response is `HTTP/1.1 200 OK`.


### 2.2 Cluster mode

In this chapter we will deploy a `databend-meta` cluster with 3 nodes.

First we are going to start up the first node in **single** mode, to form a
single node cluster.
Then join the other two nodes to the cluster to finally form a 3-nodes cluster.

:::tip

One of the 3 nodes will be elected to be a **leader** which serves write and read
API.
Other nodes are **follower**, and will redirect requests to the leader.
Thus a `databend-query` can be configured to connect to any of the nodes in a
cluster.

:::

#### 2.2.1 Create databend-meta-1.toml

```shell title="databend-meta-1.toml"
log_dir            = "metadata/_logs1"
metric_api_address = "0.0.0.0:28100"
admin_api_address  = "0.0.0.0:28101"
grpc_api_address   = "0.0.0.0:9191"

[raft_config]
id                  = 1
raft_dir            = "metadata/datas1"
raft_api_port       = 28103
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "localhost"
```

- `metric_api_address` is the service for metrics collection.
- `admin_api_address` is the service for retrieving cluster status.
- `grpc_api_address` is the service for applications to write or read metadata.

- `raft_config.id` is the globally unique id for this node; it is a `u64`.
- `raft_config.raft_dir` is the local dir to store metadata, including raft log
    and state machine etc.

- `raft_config.raft_api_port`,`raft_config.raft_listen_host` and `raft_config.raft_advertise_host`
  defines the service for internal raft communication.  Application should never touch this port.

  `raft_listen_host` is the host the internal raft server listens on.
  `raft_advertise_host` is the host the internal raft client to connect to.

- `single` tells the node to initialize a single node cluster if it is not
    initialized. Otherwise, this arg is just ignored.

For more information about config, see [Configuration](15-metasrv-config.md)

#### 2.2.2 Start the databend-meta node-1

```shell
./databend-meta -c ./databend-meta-1.toml > meta1.log 2>&1 &
```

#### 2.2.3 Check databend-meta node-1

```shell
curl -I  http://127.0.0.1:28101/v1/health
```

Check the response is `HTTP/1.1 200 OK`.


### 2.2.4 Create config files for other 2 nodes:

The config for other nodes are similar, except the `single` should be replaced
with `join`, and the `id` has to be different.

```shell title="databend-meta-2.toml"
log_dir            = "metadata/_logs2"
metric_api_address = "0.0.0.0:28200"
admin_api_address  = "0.0.0.0:28201"
grpc_api_address   = "0.0.0.0:28202"

[raft_config]
id                  = 2
raft_dir            = "metadata/datas2"
raft_api_port       = 28203
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "localhost"
join                = ["localhost:28103"]
```

```shell title="databend-meta-3.toml"
log_dir            = "metadata/_logs3"
metric_api_address = "0.0.0.0:28300"
admin_api_address  = "0.0.0.0:28301"
grpc_api_address   = "0.0.0.0:28302"

[raft_config]
id                  = 3
raft_dir            = "metadata/datas3"
raft_api_port       = 28303
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "localhost"
join                = ["localhost:28103"]
```

The arg `join` specifies a list of raft addresses(`<raft_advertise_host>:<raft_api_port>`) of nodes in the existing cluster it wants to
be joined to.


### Start other nodes.

```shell
./databend-meta -c ./databend-meta-2.toml > meta2.log 2>&1 &
./databend-meta -c ./databend-meta-3.toml > meta3.log 2>&1 &
```
