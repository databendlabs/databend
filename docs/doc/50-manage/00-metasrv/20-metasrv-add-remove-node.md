---
title: Manage a Databend Meta Service Cluster
sidebar_label: Manage a Meta Service Cluster
description: 
  How to add/remove nodes from the Databend Meta Service cluster
---

:::tip

Expected deployment time: **5 minutes ⏱**

:::

At any time a `databend-meta` node can be added or removed without service downtime.

## 1. Add Node

### 1.1 Create databend-meta-n.toml for the new node

The new node has to have a unique `id` and unique listening addresses.
E.g., to add a new node with id `7`, the config toml would look like:

```shell title="databend-meta-7.toml"
log_dir            = "metadata/_logs7"
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

The arg `join` specifies a list of raft addresses(`<raft_advertise_host>:<raft_api_port>`) of nodes in the existing cluster it wants to
be joined to.

### 1.2 Start the new node

```shell
./databend-meta -c ./databend-meta-7.toml > meta7.log 2>&1 &
```

## 2. Remove Node

Remove a node with:
`databend-meta --leave-id <node_id_to_remove> --leave-via <node_addr_1> <node_addr_2>...`

This command can be used anywhere there is a `databend-meta` installed.
It will send a `leave` request to the first `<node_addr_i>` it could connect to.
And it will block until the `leave` request is done or an error occur.

`databend-meta --leave-via` will quit at once when the `leave` RPC is done.

- `--leave-via` specifies a list of the node `advertise` addresses to send the `leave` request to.
  See: `--raft-advertise-host`

- `--leave-id` specifies the node id to leave. It can be any id in a cluster.

## 3. Examine cluster members

At every step of adding or removing a node, the cluster state should be checked to ensure everything goes well.

The `admin-api-address` defined in the config provides a administration HTTP service to examine cluster state:
E.g., `curl -s localhost:28101/v1/cluster/nodes` will display the members in a cluster:

```json
[
  {
    "name": "1",
    "endpoint": {
      "addr": "localhost",
      "port": 28103
    }
  },
  {
    "name": "2",
    "endpoint": {
      "addr": "localhost",
      "port": 28203
    }
  }
]
```

