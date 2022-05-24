---
title: Manage a Databend Meta Service Cluster
sidebar_label: Manage a Meta Service Cluster
description: How to add/remove nodes from the Databend Meta Service cluster ---
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

At any time a `databend-meta` node can be added or removed without service downtime.

## 1. Add Node

### 1.1 Create databend-meta-n.toml for the new node

The new node has to have a unique `id` and unique listening addresses. E.g., to add a new node with id `7`, the config toml would look like:

```shell title="databend-meta-7.toml"
log_dir            = "metadata/_logs7"
metric_api_address = "0.0.0.0:28700"
admin_api_address  = "0.0.0.0:28701"
grpc_api_address   = "0.0.0.0:28702"

[raft_config]
id                  = 7
raft_dir            = "metadata/datas7"
raft_api_port       = 28703
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "localhost"
join                = ["localhost:28103"]
```

The arg `join` specifies a list of raft addresses(`<raft_advertise_host>:<raft_api_port>`) of nodes in the existing cluster it wants to be joined to.

### 1.2 Start the new node

```shell
./databend-meta -c ./databend-meta-7.toml > meta7.log 2>&1 &
```

## 2. Remove Node
