# Proposal: DatafuseCli Design Doc

## Background

`fusectl` is a command-line tool for creating, listing, logging,
deleting datafuse running instances on local or
on cloud.
It also supports to port forward webUI, monitoring dashboard on local
and run SQL query from command line

### Goals

1. Centralized way to manage running datafuse cluster on local or on k8s(Cloud) (start, delete, update, log)
2. Manage and install release Instances on local machine
3. Show different dashboards on local (prometheus, jaeger, query web UI (like querybook or superset))
4. Support to run query or load data through command line
5. TLS Authentication support: support client side authentication, and also support to configure mTLS for managed datafuse instances

### Non Goals for now

1. More detailed managements like manage schema, table etc
2. Query Task visualization (list and show all query tasks on current cluster)
3. RBAC tenant management(add a subcommand tenant is helpful,
   and is compatible with this design)

## Installation

Use single line install script to install `fusecli` on local machine

```bash
curl -fsS https://raw.githubusercontent.com/datafuselabs/datafuse/master/scripts/installer/install.sh | bash
```

## SubCommands

### Cluster Create

Create, Configure and switch to a datafuse cluster using the following command:

```bash
fusecli cluster create --profile=<datafuse profile>
```

Support three kinds of profile in alpha stage,

1. local: local profile will run standalone `datafuse` cluster on local(one running fuse-query instance and one running fuse-store instance)
2. demo: install a standalone `datafuse` instance on cloud(k8s or maybe fargate in later stage)
3. cluster: install `datafuse` cluster on cloud( through `datafuse operator`)

Support to use flags or yaml or toml files for deployment configuration setup
For example:

Run datafuse instance on local and setup the api address, version, tls for it

```bash
fusecli cluster create --profile=local --set local.mysql_port=3307 --set local.http_address=127.0.0.1:7070 --set local.version=v0.4.88-nightly --set local.tls_key=<key file location>
--set local.tls_cert=<cert file location --set local.ca_cert=<ca cert location>
```

Create and configure datafuse cluster through toml file or yaml file

```bash
fusecli cluster create -f cluster_configuration.toml
```

### Cluster List

List all clusters managed by the command line (Name with * in the cluster used in current session)

```bash
fusecli cluster list
```

```bash
| NAME | PROFILE | CLUSTER | STATUS | ADDRESS | TLS |
| default(*)| local | local | RUNNING| localhost:3307 | disabled|
| demo | demo| minikube | RUNNING | 172.0.0.11:3307 | disabled|
| production| cluster | GKE | RUNNING | 192.12.1.1:3307 | enabled|
```

### Cluster View

View datafuse components in current cluster
For example:
In cluster profile:

```bash
fusecli cluster view
| NAME | CLUSTER | COMPONENT | STATUS | TLS |
| query-1 | GKE | datafuse-query | running | enabled |
| query-2 | GKE | datafuse-query | running | enabled |
| query-3 | GKE | datafuse-query | pending | enabled |
| store-1 | GKE | datafuse-store | running | enabled |
| store-2 | GKE | datafuse-store | running | enabled |
```

Check on disk utilization

```bash
fusecli cluster df
| NAME | COMPONENT | USED | ALWAYABLE | LOCATION |
| local-disk-1 | Block| 10Gi | 90Gi | /mnt/fuse-store |
| s3-disk | Object| 100 Gi | 1000Gi | s3://bucket-1/mnt/fuse-store |
```

### Cluster delete

For local profile, pids in running instances shall be killed, and for cluster profile,  computing pods would be deleted, can add some flags to delete disk resources as well( RBAC needed)

```bash
fusecli cluster delete
```

### Cluster log

Show logs in current running instance

show all fuse-query logs

```bash
fusecli cluster log --component=query --all
```

The command above would show all fuse-store logs

```bash
fusecli cluster log --component=store --all
```

The command above would show datafuse operator logs

```bash
fusecli cluster log --component=operator
```

### Cluster add

Add component to current storage

The following command add two query nodes, each need 2 cpu resource

```bash
fusecli cluster add --component query --num 2 --cpu 2 --namespace=<operator namespace>
```

### Cluster use

Switch to another cluster
The Command will switch to another cluster run on GKE cloud

```bash
fusecli cluster use --name cloud-instance-1 --cluster GKE --kubeconfig ~/.kube/config --kubecontext gke-cloud-1
```

### Cluster Check

Check whether current configuration can be deployed on given cluster

It will check on port availability, and storage resource availability for deployment

```bash
fusecli cluster check --profile=local
```

It will check on cloud resources, whether compute nodes could be scheduled on given cloud platform, whether TLS configured etc

```bash
fusecli cluster check -f deploy.yaml
```

### Cluster Analyze

Analyze and troubleshooting on given configuration, difference between analyze and check is that analyze is troubleshooting on a running cluster, and check mainly used for pre-fight check

```bash
fusecli cluster analyze --profile=local
```

### Cluster Update

Update cluster to a newer version

```bash
fusecli cluster update v0.5.1-nightly
```

### Query

Run query using selected client

```bash
fusecli query 'SELECT * FROM TABLE1' --client=mysql
```


