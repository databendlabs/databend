---
title: Deploying a Databend Cluster
sidebar_label: Deploying a Databend Cluster
description: 
  Deploying a Databend Cluster
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Databend recommends deploying a cluster with a minimum of three meta nodes and one query node for production environments. To gain a better understanding of Databend cluster deployment, see [Understanding Databend Deployments](../00-understanding-deployment-modes.md), which will familiarize you with the concept. This topic aims to provide a practical guide for deploying a Databend cluster.

## Before You Begin

Before you start, make sure you have completed the following preparations:

- Plan your deployment. This document is based on the following cluster deployment plan, which involves setting up a meta cluster comprising three meta nodes and a query cluster consisting of two query nodes:

| Node #  	| IP Address    	| Leader Meta Node? 	| Tenant ID 	| Query Cluster ID 	|
|---------	|---------------	|-------------------	|-----------	|------------------	|
| Meta-1  	| 192.168.1.100 	| Yes               	| -         	| -                	|
| Meta-2  	| 192.168.1.101 	| No                	| -         	| -                	|
| Meta-3  	| 192.168.1.102 	| No                	| -         	| -                	|
| Query-1 	| 192.168.1.10  	| -                 	| default   	| default          	|
| Query-2 	| 192.168.1.20  	| -                 	| default   	| default          	|

- [Download](https://databend.rs/download) and extract the Databend package onto each of your prepared servers according to your deployment plan.

## Step 1: Deploy Meta Cluster

1. Configure the file **databend-meta.toml** in each meta node. Please note the following when configuring each node:

    - Ensure that the **id** parameter in each node is set to a unique value.

    - Set the **single** parameter to *true* for the leader meta node.

    - For follower meta nodes, comment out the **single** parameter using the # symbol, then add a **join** setting and provide an array of the IP addresses of the other meta nodes as its value.

<Tabs>
  <TabItem value="Meta-1" label="Meta-1" default>

```toml title="databend-meta.toml"
log_dir                 = "/var/log/databend"
admin_api_address       = "0.0.0.0:28101"
grpc_api_address        = "0.0.0.0:9191"
# databend-query fetch this address to update its databend-meta endpoints list,
# in case databend-meta cluster changes.
grpc_api_advertise_host = "192.168.1.100"

[raft_config]
id            = 1
raft_dir      = "/var/lib/databend/raft"
raft_api_port = 28103

# Assign raft_{listen|advertise}_host in test config.
# This allows you to catch a bug in unit tests when something goes wrong in raft meta nodes communication.
raft_listen_host = "192.168.1.100"
raft_advertise_host = "192.168.1.100"

# Start up mode: single node cluster
single        = true
```
  </TabItem>
  <TabItem value="Meta-2" label="Meta-2">

```toml title="databend-meta.toml"
log_dir                 = "/var/log/databend"
admin_api_address       = "0.0.0.0:28101"
grpc_api_address        = "0.0.0.0:9191"
# databend-query fetch this address to update its databend-meta endpoints list,
# in case databend-meta cluster changes.
grpc_api_advertise_host = "192.168.1.101"

[raft_config]
id            = 2
raft_dir      = "/var/lib/databend/raft"
raft_api_port = 28103

# Assign raft_{listen|advertise}_host in test config.
# This allows you to catch a bug in unit tests when something goes wrong in raft meta nodes communication.
raft_listen_host = "192.168.1.101"
raft_advertise_host = "192.168.1.101"

# Start up mode: single node cluster
# single        = true
join            =["192.168.1.100:28103","192.168.1.102:28103"]
```
  </TabItem>
  <TabItem value="Meta-3" label="Meta-3">

```toml title="databend-meta.toml"
log_dir                 = "/var/log/databend"
admin_api_address       = "0.0.0.0:28101"
grpc_api_address        = "0.0.0.0:9191"
# databend-query fetch this address to update its databend-meta endpoints list,
# in case databend-meta cluster changes.
grpc_api_advertise_host = "192.168.1.102"

[raft_config]
id            = 3
raft_dir      = "/var/lib/databend/raft"
raft_api_port = 28103

# Assign raft_{listen|advertise}_host in test config.
# This allows you to catch a bug in unit tests when something goes wrong in raft meta nodes communication.
raft_listen_host = "192.168.1.102"
raft_advertise_host = "192.168.1.102"

# Start up mode: single node cluster
# single        = true
join            =["192.168.1.100:28103","192.168.1.101:28103"]
```
  </TabItem>
</Tabs>

2. To start the meta nodes, run the following script on each node: Start the leader node (Meta-1) first, followed by the follower nodes in sequence.

```shell
./databend-meta -c ./databend-meta.toml > meta.log 2>&1 &
```

3. Once all the meta nodes have started, you can use the following curl command to check the nodes in the cluster:

```shell
curl 192.168.1.100:28102/v1/cluster/nodes
```

## Step 2: Deploy Query Cluster

1. Configure the file **databend-query.toml** in each query node. The following list only includes the parameters you need to set in each query node to reflect the deployment plan outlined in this document.

    - Set the tenant ID and cluster ID according to the deployment plan.

    - Set the **endpoints** parameter to an array of the IP addresses of the meta nodes.

```toml title="databend-query.toml"
...

tenant_id = "default"
cluster_id = "default"

...

[meta]
# It is a list of `grpc_api_advertise_host:<grpc-api-port>` of databend-meta config
endpoints = ["192.168.1.100:9191","192.168.1.101:9191","192.168.1.102:9191"]
...
```

2. For each query node, you also need to configure the object storage in the file **databend-query.toml**. For detailed instructions, see [Deploying a Query Node](../02-deploying-databend.md#deploying-a-query-node).

3. Run the following script on each query node to start them:

```shell
./databend-query -c ../configs/databend-query.toml > query.log 2>&1 &
```

## Next Steps

After deploying Databend, you might need to learn about the following topics:

- [SQL Clients](/doc/sql-clients): Learn to connect to Databend using SQL clients.
- [Manage Settings](../../13-sql-reference/42-manage-settings.md): Optimize Databend for your needs. 
- [Load & Unload Data](/doc/load-data): Manage data import/export in Databend.
- [Visualize](/doc/visualize): Integrate Databend with visualization tools for insights.