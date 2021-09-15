---
id: databend-store-design
title: DatabendStore Design Draft
---

DatabendStore is the storage layer in charge of:
- meta data storage such as user, db, table and schema.
- blocks life cycle management such as allocation, compaction etc.
- data/metadata consistency and reliability.

```
                  DatabendQuery(client)
                      |
                      | rpc
                      v
  DatabendStore       | flightServer                // network, auth
                |     |
                |     v
                |  Handler                    // execution engine
                |     |
                |     v
                | IFileSystem                 // abstract storage layer
```

# IFileSystem

`IFileSystem` defines an abstract storage layer that DatabendStore would runs on.
An `IFileSystem` impl in the cluster is the only stateful component.

- Local FS: impl `IFileSystem` API and use a local disk folder as storage.
    Suitable for a single node DatabendQuery deployment.

- DFS: impl `IFileSystem` and setup an aws-S3 like storage service.
    A DFS organizes multiple `LocalFS` with a centralized meta data service.

- Object Storage Adapters: an `IFileSystem` impl that builds upon an object
  storage service on cloud.


`IFileSystem` defines following API-s:

- `add`: AKA put-if-absent: add a file only if it is absent.
- `read_all`: read all bytes of a file.
- `list`: retrieve a list of files with specified prefix.
- TODO

# API

DatabendQuery and DatabendStore talks arrow-flight protocol.

Schema related operations such as `create table` or `create database` are wrapped with a `FlightService::do_action` RPC.
Data operation such as reading or writing a block are done with
`FlightService::do_get` and `FlightService::do_put`.

See `common/flights/src/store_client.rs`.


# DFS

The most important component in DatabendStore is the DFS.
DFS mainly consists of two parts: the meta data cluster and block storage
cluster.

Block cluster is unaware of data placement and is purely a set of servers
providing object like write and read API.

Data placement is part of the meta data and is stored in the meta cluster.


## Meta data cluster

All meta(the cluster meta and file keys) has a copy that resides in memory on every node for instant access.

Every update to meta is done by committing a raft log of the updated entry into meta cluster.

A cluster keeps its meta data in a raft group with typically 5 candidate nodes
and all other nodes as learners.
Candidate nodes provides meta data read and write API.
Every other node is a **learner**, which does not elect but just subscribes
meta change message from the 5 candidates.


## In-process metadata components

A DatabendStore process includes two grpc API: the flight service and the meta
service.

- Meta related components are wrapped into `MetaNode`, in which a `Raft` instance
    is maintained along with storage and network engines.

    `MetaNode` is the only entry for other DatabendStore components to access meta data.

- `RaftNode` communicates with remote RaftNode through `Network`, which send
    messages to meta-grpc service on other DatabendStore nodes.

- `Network` relies on `Storage` to find out info of other nodes.


```
DatabendStore components:
.---------------------------.
|                           |
| flight-grpc     meta-grpc |
|     |               |     |
|     '--.      .-----'     |
|        v      v           |
|        MetaNode           |
|        |     |            |
|        |     v            |
|        |    RaftNode      |
|        |  .--'   |        |
|        v  v      v        |
|      Storage <- Network   |
|                           |
'---------------------------'
```


## Meta data structure

Meta data includes hardware information: nodes, the file information: keys and
data placement information: slots.

```proto
message Meta {
    map<string, string> keys;
    repeated Slot slots;
    map<int64, Node> nodes
}

message Node {
    int64 NodeId
    repeated string Addresses
}

message Slot {
    repeated int64 node_ids;
}
```

## File format

A data block in DFS or local-FS is a complete Parquet data, with schema embedded.
The schema in block should always be identical to the schema stored in
table-meta. Otherwise it is a severe bug.

Parquet has its own segmentation and index in it which is similar to ClickHouse
file structure.

See: https://parquet.apache.org/documentation/latest/

A schema file such as table or database is in protobuf format.


## Data placement

A file in DFS has 3 copies.
A file is identified with a unique `key`.
Every `key` is assigned to a virtual allocation unit `slot` by some hash algo.
A `slot` is assigned to 3 nodes. The slot-to-nodes mapping is part of the DFS meta data.

## Replication

Once a data block is persisted on local fs and the corresponding meta data is
committed, DFS ack the client an OK message.

Every node is either a follower or leaner of the meta data raft group thus will
receive the meta changes.
If a node found that a `key` is uploaded and the `slot` for the `key` is on this
node, it pulls the data block from the uploading node.


## Meta cluster startup

A DatabendStore cluster is stateful thus the startup is done in several steps:

- Boot up the first node in a cluster, by calling `MetaNode::boot()`.
  This func creates an empty raft instance add initialize itself as the leader of the solo cluster.
  
- Creating node is done with `MetaNode::boot_non_voter()`. 
  This func does nothing more than initializing a raft instance.
  It returns the `MetaNode` instance with network API ready for accepting raft communications.
  At this point it is unaware of anything about the cluster.
  
  To add this new node to a cluster, send an `Cmd::AddNode` request to the leader.
  The leader will then commit the node info into its own storage and start sending 
  raft snapshot and logs to the new node.

When a node is shutting down and restarted, just the `MetaNode::new()` is used to start the meta-service.
When a node is restarted:
 
- A candidate(AKA voter) that becomes the new leader is able to find out every node from its local storage and then add them as non-voter in order to replicate logs to them.
- A non-voter has nothing to do other than receiving logs from the leader.

# DFS Example

```
FQ:      DatabendQuery node

flight:  arrow-flight server
handler: execution handler
DFS:     distributed FS
FS:      local FS

L:       leader of raft for meta
F:       follower of raft
lnr:     learner

                 FQ
                 |
                 | 1. put block
                 |    or create table
                 |
-----------------|-------------------------------------
                 v
                 flight <-----.  flight
                 |            |
                 | 2.         | 8. pull block
    5. commit    |            |
       meta      v            |
         .-------handler      '- handler
         |       |               |  ^
         |       | 3.         9. |  |
         |       v               v  |
         |       DFS             DFS|
         |       |               |  |
         |       | 4.        10. |  |
         |       v               v  |
         |       FS              FS | 7. notify handler
         |                          |    of meta changes
         v      6. meta bcast       |
meta   : L----+-------+------+------|-+-------.
              `->F    `->F    `->F  | `->F    `->lnr
                                  `-'
nodes:   N1      N2      N3      N4      N5      N6 ...
```

# Table format

A table in `IFileSystem` consists of several files and the structure is similar
to a append-only log:

- Table head: contains schema and a pointer to the latest manifest file.
    The table head must be updated **atomically**.

- Manifest: describes what data blocks belongs to a table.
    There is a list of data block files pointing to the latest updates,
    and a pointer to previous manifest i.e., the last version this update based
    on.

- Data: a collection of operation logs.

```
T: table head
Mi: manifest
di: data block files

                   T
                   |
                   v
M0 <---- M1 <----- M2
|        |         |
`-> d0   +-> d1    +-> d3
         |         |
         `-> d2    `-> d4

```

A typical update workflow would be like the following:

- 1. Handler receives a batch of updates, including inserts and deletes.
  It writes to one or more data blocks, with some unique keys.

- 2. Handler reads the latest table head file, finds out the latest manifest,
    e.g., M1, and compose a new manifest e.g., M2, containing the data block
    keys from step-1 and a pointer to M1.
    Write it to DFS with a unique key.

- 3. Handler atomically update the table head file to change the latest manifest
    pointer to M2. If race encountered, retry from step-2.

The challenge is on step-3:

An `IFileSystem` impl may not support atomic update.
In this case, write the table head file with a mono-incremental key, e.g.
`t-001`, then `t-002` ...
And a read operation should list all of the heads and find out the latest version(resulting in eventual consistency).
This strategy only requires atomic-add operation, i.e., put-if-absent.

Atomic-add can be done on behalf of the meta cluster, since it is an easy job
for a raft group or any other consensus group to generate mono-incremental ids.

# Table compaction

Compaction merges several earliest manifest into one, and optionally merges
overlapping data blocks.

Compaction generates a new manifest e.g., M1' from M1. Then it removes M0 and M1.

A reading process should try to read both M1 and M1', and use either one it
sees.

A manifest or data block will be added or removed, but never updated,
**since in a distributed system updating data results in super complicated consistency
challenges**.

```
                   T
                   |
                   v
M0 <---- M1 <----+ M2
|        |       | |
`-> d0   +-> d1  | +-> d3
         |       | |
         `-> d2  | `-> d4
                 |
                 |
                 |
                 |
         M1' <---'
         +-> d0
         |
         +-> d1
         |
         `-> d2

```


<!--

## Compaction

There is only one valid block in a group.
New write generates a unmerged temp block which is invisible to read request,
until it is merged.

## Tiering:

A block is either a file on local fs, or an object in S3.
It is possible a block has copies on both local fs and S3 if:
- The block has not yet been cleaned from local fs after dumping to S3.
- The block is pinned on local fs to speed up read.

Since a persistent block does not change, reading either one is valid.


## Write

W-1: write directly to group leader:
Leader persists block on local fs and update the meta(ReplicationGroupBlocks) in the replication group raft to commit a block-write.

W-2: write to a follower:
The simple way is to let the follower foward write request to the leader it
knows of.

The efficient way is to let the follower replicate data block to a quorum,
then forward meta change to the leader to commit this write request.


## Read

Reading from a follower provides no consistency guarantee.
To be consistent, i.e., the last write must be seen, a read request must be
dealt by a leader.
And the leader has two options:
- Read the latest log index, then concact a quorum to ensure leadership, respond
    the applied value of the latest log. This is similar to the `ReadIndex` in
    raft.
- Leader keeps a **read-lease** before which no other candidate is allowed to elect.
    With this lease, a leader is able to responds a read request without
    additional RPC.


## local-WAL and the replicated log

A replicated state machine is typically driven by a replicated log, just like
raft does.
A replicated log has more state to keep persistent than a traditional WAL on a
single machine, e.g., the leader or the term(in raft, or ballot in paxos).

Thus in a replication group:

- Local-machine WAL, the local-WAL guarantees the consistency of one copy of the repliated
    log, e.g. no data loss when process crashes.

- And the repicated log guarantees the consistency across multiple(typically, 3)
    state machines on different servers.

    The state machine in our system represents the meta data.

E.g., local-WAL contains an entry of leader change event which is not one of the
replicated log entry. Replicated log entry contains only meta data change
events.

In short, the **local-WAL** is the WAL log of the **replicated log**, the **replicated log**
is the WAL log of the state machine.


## Replication group maintenance

A replication group is only in charge of repairing data blocks in it.
Resource related tasks such as replacing a crashed server is triggered by other coordinating
process. TODO


# Roadmap

- [ ] Mock User, DB, Table and Write/Read API to pass basic API test.

- [ ] Impl meta data Write and Read.

- [ ] Impl DataBlock from/to Parquet.

- [ ] Compaction: periodically merge overlapping blocks and elaborate the
    compaction policy.

- [ ] Replication: grouping up to 3 Nodes into a replication group with strongly
    consistent consensus such as raft or paxos.
    - Append WAL with only delta instead of entire meta.

- [ ] Tiering: dump less access `Block` to external storage such as aws-S3
    compatible cluster.

- [ ] Sharding: a storage cluster with more than 3 nodes should be organized
    with multiple consensus groups: a group contains 1/n of all of the blocks.

    - Write/Query request routing
-->
