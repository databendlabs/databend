---
title: Understanding Deployment Modes
sidebar_label: Understanding Deployment Modes
description:
  Describes Databend deployment modes
---

## Understanding Deployment Modes

A Databend deployment includes two types of node, Meta and Query. The Meta node stores various types of metadata (such as database, table, cluster, and transaction) and manages user information (including authorization and authentication). The Query node takes care of queries. 

When deploying Databend, you specify a deployment mode, standalone, or cluster. A standalone Databend allows for one Meta node and one Query node, and a Databend cluster can include multiple Meta and Query nodes to enhance the computing capability.

### Supported Object Storage Solutions
Databend works with self-hosted or cloud object storage solutions. You will need to prepare your own object storage before deploying Databend. The following is a list of compatible object storage solutions:
- Self-hosted object storage solutions:
  - MinIO
  - SeaweedFS
- Cloud storage solutions:
  - Amazon S3
  - Tencent COS
  - Alibaba OSS
  - Wasabi
  - QingCloud QingStore
- Azure Blob Storage

## Standalone Deployment
This topic describes the standalone deployment architecture and environments.

### Deployment Architecture
When you deploy Databend in standalone mode, you host a Meta node and a Query node on the same machine or separately. For more information about how to deploy Databend in standalone mode with various object storage solutions, see [Deploying with Self-Hosted Object Storage](./10-deploying-with-self-hosted-object-storage.md).

<img src="/img/deploy/deploy-standalone-arch.png"/>

### Supported Environments
You can deploy both the Meta and Query nodes on-premises server or in the cloud. Databend can be deployed on most public cloud platforms. This includes:
- Amazon EC2
- Tencent Cloud
- Alibaba Cloud

The following list provides recommended hardware specifications for the server running a Databend node in standalone mode:
- CPU: 16-core or above
- Memory: 32 GB or above
- Hard Disk: 200 to 600 GB, SSD
- Network Interface Card: 10 Gbps or above

## Cluster Deployment
This topic describes the cluster deployment architecture and environments.

### Deployment Architecture
When you deploy Databend in cluster mode, you set up multiple Meta and Query nodes, and host each node on separate machine. Please note that you must have a minimum of three Meta nodes in a cluster.

<img src="/img/deploy/deploy-cluster-arch.png"/>

When you deploy Databend in cluster mode, you launch up a Meta node first, and then set up and start the other Meta nodes to join the first one. After all the Meta nodes are started successfully, start the Query nodes one by one. Each Query node automatically registers to the Meta nodes after startup to form a cluster.

<img src="/img/deploy/deploy-clustering.png"/>

### Supported Environments
You can deploy the Databend nodes to your on-premises servers or in the cloud. Databend can be deployed on most public cloud platforms. This includes:
- Amazon EC2
- Tencent Cloud
- Alibaba Cloud

The following list provides recommended hardware specifications for the server running a Databend node in cluster mode:
- CPU: 16-core or above
- Memory: 32 GB or above
- Hard Disk
  - Meta node: 200 to 600 GB, SSD
  - Query node: 100 to 200 GB, SSD
- Network Interface Card: 10 Gbps or above