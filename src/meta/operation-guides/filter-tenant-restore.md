# Filter One Tenant and Restore MetaService

This guide describes how to export Databend Meta Service data, keep only one tenant's metadata, import the filtered data into a new three-node MetaService cluster, and start the cluster.

The examples assume locally built `databend-metactl` and `databend-meta` binaries:

```bash
export META_CTL=/path/to/databend-metactl
export META_SRV=/path/to/databend-meta
export WORK_DIR=/tmp/databend-meta-restore
export TENANT_NAME=tenant1
mkdir -p "$WORK_DIR"
```

## 1. Trigger Snapshot and Export Data

Stop writes before exporting so that the state machine snapshot contains all business metadata.

The tenant filter only processes the exported `state_machine/*` section. In a MetaService export, the `state_machine/*` records come from the current snapshot. Therefore, trigger a snapshot before exporting so that data that has only been applied in memory is flushed into the snapshot.

Trigger a snapshot on the target MetaService node:

```bash
"$META_CTL" trigger-snapshot \
  --admin-api-address 127.0.0.1:28101
```

The command should print:

```text
triggered snapshot successfully.
```

Optionally check the snapshot status:

```bash
"$META_CTL" status \
  --grpc-api-address 127.0.0.1:9191
```

Export from a running MetaService node:

```bash
"$META_CTL" export \
  --grpc-api-address 127.0.0.1:9191 \
  --db "$WORK_DIR/backup.data"
```

Or export from a local raft-dir:

```bash
"$META_CTL" export \
  --raft-dir /data/databend-meta/source \
  --db "$WORK_DIR/backup.data"
```

## 2. Filter Data

Use `filter-tenant` to keep the target tenant and every record reachable from that tenant's roots. The command skips raft logs and filters state machine records only.

```bash
"$META_CTL" filter-tenant \
  --tenant "$TENANT_NAME" \
  --input "$WORK_DIR/backup.data" \
  --output "$WORK_DIR/tenant-filtered.data" \
  2>"$WORK_DIR/filter-tenant.log"
```

`filter-tenant.log` records snapshot orphan keys that were detected and dropped. Check the output sizes after filtering:

```bash
wc -l "$WORK_DIR/tenant-filtered.data" "$WORK_DIR/filter-tenant.log"
```

## 3. Import as a Three-Node Cluster

Pass the complete three-node `--initial-cluster` explicitly during import. This rewrites the imported raft-dir with the new three-node membership.

If the three nodes run on separate machines, use their real raft advertise addresses. The example uses `meta-1`, `meta-2`, and `meta-3` as host names:

```bash
export CLUSTER_ARGS="\
--initial-cluster 1=meta-1:29103 \
--initial-cluster 2=meta-2:29103 \
--initial-cluster 3=meta-3:29103"
```

Import node 1:

```bash
"$META_CTL" import \
  --raft-dir /data/databend-meta/node-1 \
  --db "$WORK_DIR/tenant-filtered.data" \
  --id 1 \
  $CLUSTER_ARGS
```

Import node 2:

```bash
"$META_CTL" import \
  --raft-dir /data/databend-meta/node-2 \
  --db "$WORK_DIR/tenant-filtered.data" \
  --id 2 \
  $CLUSTER_ARGS
```

Import node 3:

```bash
"$META_CTL" import \
  --raft-dir /data/databend-meta/node-3 \
  --db "$WORK_DIR/tenant-filtered.data" \
  --id 3 \
  $CLUSTER_ARGS
```

For a local single-machine test, use different raft directories and different raft ports for the three nodes, such as `29103`, `29203`, and `29303`.

Check the imported membership offline:

```bash
"$META_CTL" export \
  --raft-dir /data/databend-meta/node-1 \
  --db "$WORK_DIR/check-node-1.data"

rg 'LastMembership|Nodes|NodeId' "$WORK_DIR/check-node-1.data"
```

The membership configs should contain `1`, `2`, and `3`, and the `Nodes` records should contain all three nodes.

## 4. Start MetaService

The imported raft directories already contain the cluster membership, so do not pass `--single` or `--join` when starting the nodes. Each node should use its own raft-dir, node id, and raft endpoint.

Node 1:

```bash
"$META_SRV" \
  --id 1 \
  --raft-dir /data/databend-meta/node-1 \
  --raft-listen-host 0.0.0.0 \
  --raft-advertise-host meta-1 \
  --raft-api-port 29103 \
  --grpc-api-address 0.0.0.0:9191 \
  --admin-api-address 0.0.0.0:28101
```

Node 2:

```bash
"$META_SRV" \
  --id 2 \
  --raft-dir /data/databend-meta/node-2 \
  --raft-listen-host 0.0.0.0 \
  --raft-advertise-host meta-2 \
  --raft-api-port 29103 \
  --grpc-api-address 0.0.0.0:9191 \
  --admin-api-address 0.0.0.0:28101
```

Node 3:

```bash
"$META_SRV" \
  --id 3 \
  --raft-dir /data/databend-meta/node-3 \
  --raft-listen-host 0.0.0.0 \
  --raft-advertise-host meta-3 \
  --raft-api-port 29103 \
  --grpc-api-address 0.0.0.0:9191 \
  --admin-api-address 0.0.0.0:28101
```

After startup, check the running cluster with `databend-metactl status`:

```bash
"$META_CTL" status --grpc-api-address meta-1:9191
```

Confirm the following:

- `State` is `Leader` or `Follower`.
- A `Leader` has been elected.
- `Voters` contains all three nodes.
- `DataVersion` matches the import output.
