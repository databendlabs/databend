---
title: Expanding a Standalone Databend
sidebar_label: Expanding a Standalone Databend
description:
  How to expand a standalone Databend
---

## Expanding a Standalone Databend

If you already have a standalone Databend, you can expand it by adding one or more query nodes to obtain more powerful computing capability.

In this topic, we will add a new Query node to an existing standalone Databend.

### Deploying a Standalone Databend
Follow [Deploying with Self-Hosted Object Storage](./10-deploying-with-self-hosted-object-storage.md) to deploy a local standalone Databend with MinIO.

### Deploying a New Query Node
1. Make a copy of the file *databend-meta-node.toml* in the folder */usr/local/databend/etc*, paste it to the same folder with a name "databend-query-node2.toml".

2. Open the file *databend-query-node2.toml*, modify the values for the parameters as below:

```toml
# For admin RESET API.
admin_api_address = "127.0.0.1:8082"

# Cluster flight RPC.
flight_api_address = "127.0.0.1:9092"
```

:::tip

**Keep the same cluster ID**. When you set up a new Query node for a cluster, make sure all the cluster IDs are the same across the nodes. A Query node uses this ID to decide which cluster to join and registers to the corresponding Meta node.

:::

3. Open a terminal window and navigate to the folder */usr/local/databend/bin*.

4. Run the following command to start the Query node:

```curl
./databend-query -c ./databend-query-node2.toml 2>&1 > query.node2.log&
```

5. Run the following command to check if the new node was started successfully:

```curl
curl -I  http://127.0.0.1:8082/v1/health
```

6. Check the cluster information:
```sql
mysql -h127.0.0.1 -uroot -P3308
SELECT * FROM system.clusters
+------------------------+-----------+------+
| name                   | host      | port |
+------------------------+-----------+------+
| QXyxUbieMYMV6OGrjoDKL6 | 127.0.0.1 | 9092 |
| Y1lJiseTjCLwpVRYItQ2f3 | 127.0.0.1 | 9091 |
+------------------------+-----------+------+
```