---
title: Deploying a Query Cluster on Kubernetes
sidebar_label: Deploying a Query Cluster on Kubernetes
description:
    How to deploy a Databend query cluster on Kubernetes.
---

This topic explains how to install and configure the Databend query cluster on Kubernetes with MinIO as the storage backend.

## Before You Begin

* Make sure your cluster has enough resource for installation (at least 4 CPUs, 4GB RAM, 50GB disk).
* Make sure you have a Kubernetes cluster up and running. For more information, see [k3d](https://k3d.io/v5.3.0/) or [minikube](https://minikube.sigs.k8s.io/docs/start/).
* Please note that Databend Cluster mode only works on a shared storage (AWS S3 or MinIO s3-like storage).

## Deploy a Sample Databend Cluster with MinIO

### Step 1. Install MinIO

:::caution
This configuration is for demonstration ONLY. Never use it for production. Refer to https://docs.min.io/docs/deploy-minio-on-kubernetes.html
for more information on TLS and High Availability configurations for production.
:::

We will bootstrap a MinIO server on Kubernetes with the following configurations:

```shell title="minio-server-config"
STORAGE_TYPE=s3
S3_STORAGE_BUCKET=sample-storage
S3_STORAGE_REGION=us-east-1
S3_STORAGE_ENDPOINT_URL=http://minio.minio.svc.cluster.local:9000
S3_STORAGE_ACCESS_KEY_ID=minio
S3_STORAGE_SECRET_ACCESS_KEY=minio123
```

The following configuration applies to the target Kubernetes cluster. It will create a bucket named `sample-storage` with `10Gi` storage space:

```shell title="minio-server-deployment"
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f https://raw.githubusercontent.com/datafuselabs/databend/main/scripts/kubernetes/minio-sample.yaml -n minio
```

### Step 2. Deploy a Standalone Databend Meta-Service Layer

The following configuration creates a standalone Databend meta-service on `databend-system` namespace:

```shell title="databend-meta-service-deployment"
kubectl create namespace databend-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f https://raw.githubusercontent.com/datafuselabs/databend/main/scripts/kubernetes/meta-standalone.yaml -n databend-system
```
### Step 3. Deploy a Databend Query Cluster

The following configuration creates a Databend query cluster on `tenant1` namespace. Each pod under the deployment has a `900m` vCPU with `900Mi` memory:

```shell title="databend-query-service-deployment"
kubectl create namespace tenant1 --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f https://raw.githubusercontent.com/datafuselabs/databend/main/scripts/kubernetes/query-cluster.yaml -n tenant1
```

To scale up or down the query cluster, please use the following command:

```shell
 # scale query cluster number to 0
 kubectl scale -n tenant1 deployment query --replicas=0
 # scale query cluster number to 3
 kubectl scale -n tenant1 deployment query --replicas=3
 ```

#### Check the Cluster Information

:::tip
Make sure that the localhost port 3308 is available.
:::

```shell
nohup kubectl port-forward -n tenant1 svc/query-service 3308:3307 &
mysql -h127.0.0.1 -uroot -P3308
```

```sql
SELECT * FROM system.clusters
```
```
+----------------------+------------+------+
| name                 | host       | port |
+----------------------+------------+------+
| dIUkzbOaqJEPudb0A7j4 | 172.17.0.6 | 9191 |
| NzfBm4KIQGEHe0sxAWa3 | 172.17.0.7 | 9191 |
| w3MuQR8aTHKHC1OLj5a6 | 172.17.0.5 | 9191 |
+----------------------+------------+------+
```

### Step 4. Distributed Query

```sql
EXPLAIN SELECT max(number), sum(number) FROM numbers_mt(10000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10;
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                           |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Limit: 10                                                                                                                                                                                                         |
|   RedistributeStage[expr: 0]                                                                                                                                                                                      |
|     Projection: max(number):UInt64, sum(number):UInt64                                                                                                                                                            |
|       AggregatorFinal: groupBy=[[(number % 3), (number % 4), (number % 5)]], aggr=[[max(number), sum(number)]]                                                                                                    |
|         RedistributeStage[expr: sipHash(_group_by_key)]                                                                                                                                                           |
|           AggregatorPartial: groupBy=[[(number % 3), (number % 4), (number % 5)]], aggr=[[max(number), sum(number)]]                                                                                              |
|             Expression: (number % 3):UInt8, (number % 4):UInt8, (number % 5):UInt8, number:UInt64 (Before GroupBy)                                                                                                |
|               ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000000000, read_bytes: 80000000000, partitions_scanned: 1000001, partitions_total: 1000001], push_downs: [projections: [0]] |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

The distributed query works now, and the cluster will efficiently transfer data through `flight_api_address`.

#### Upload Data to the Cluster

```sql
CREATE TABLE t1(i INT, j INT);
```

```sql
INSERT INTO t1 SELECT number, number + 300 from numbers(10000000);
```
```sql
SELECT count(*) FROM t1;
```
```
+----------+
| count()  |
+----------+
| 10000000 |
+----------+
```

## Install Databend Cluster with Helm Chart

We support installing Databend cluster with our official [Helm Charts](https://github.com/datafuselabs/helm-charts).

### Install Meta Service

Install a standalone Databend meta service. Please follow the [documentation](https://github.com/datafuselabs/helm-charts/blob/main/charts/databend-meta/values.yaml) for further configuration options (for example, high availability).

```bash
helm repo add databend https://charts.databend.rs
helm install my-release databend/databend-meta --namespace databend --create-namespace
```

### Install Query Service

The following command registers the Databend query service to the meta service with 3 nodes:

```bash
helm repo add databend https://charts.databend.rs
helm install query databend/databend-query --namespace databend --create-namespace \
          --set config.meta.address=my-release-databend-meta.databend.svc.cluster.local:9191 \
          --set replicaCount=3
```

Please follow the [documentation](https://github.com/datafuselabs/helm-charts/blob/main/charts/databend-query/values.yaml) for further configuration options (for example, object storage secrets).