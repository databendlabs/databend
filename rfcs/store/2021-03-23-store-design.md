# FuseStore

FuseStore is the storage layer in charge of:
- meta data storage such as user, db, table and schema.
- blocks life cycle management such as allocation, compaction etc.
- data/metadata consistency and reliability.

# API

FuseStore provides the following APIs to FuseQuery:

TODO: update user/db/table API to query-plan style

```proto
// pseudo code
service FuseStore {
    rpc UserCreate (Username) returns (UserId) {}
    rpc UserGet (Username) returns (UserId) {}

    rpc DBCreate (UserId, Schema) returns (DBId) {}
    rpc DBGet (UserId, DBName) returns (DBId, Schema) {}
    rpc DBGet (DBId) returns (DBId, Schema) {}

    rpc TableCreate (DBId, Schema) returns (TableId) {}
    rpc TableGet (DBId, TableName) returns (TableId, Schema) {}
    rpc TableGet (TableId) returns (TableId, Schema) {}


    rpc Write(DataBlock) returns () {}
    rpc Read(Plan) returns (Stream) {}
}
```

# Meta data

All meta(the cluster meta and replication group meta) resides in memory for quick access.

Every update to meta is done by commit a raft log of the updated entire meta data.


# On-disk file format

A block is a complete Parquet data, with schema embedded.
The schema in block should always be identical to the schema stored in
table-meta. Otherwise it is a severe bug.

Parquet has its own segmentation and index in it which is similar to ClickHouse
file structure.

See: https://parquet.apache.org/documentation/latest/



# Meta data

## Cluster meta

A cluster keeps its meta data in a raft group with typically 5 candidate nodes
and all other nodes as learners.
Candidate nodes provides cluster-wise meta data read and write.
Every other node is a **learner**, which does not elect but just subscribes
cluster meta change message from the 5 candidates.

The first part of meta data is the hardware information, stored in cluster raft
group:

```proto
message Cluster {
    map<int64, Node> Nodes
}

message Node {
    int64 NodeId
    repeated string Addresses
}

message ReplicationGroup {
    int64 ReplicationGroupId
    map<int64, Node> Nodes
}
```

## DB meta

DB meta describes the hierarchical structure of user, database and tables,
and is stored in cluster raft group too.

```
^ +-> User-1 +-> DB-1 +-> Table-1 +-> ReplicationGroup-1 --> Block-1
  |          |        |           |                          Block-2
  |          |        |           `-> ReplicationGroup-2 --> ...
  |          |        |
  |          |        `-> Table-2 --> ...
  |          `-> DB-2 --> ...
  `-> User-2 --> ...
```

```proto
message Meta {
    map<string, int64> UserNameToId
    map<int64, User> Users

    // monotonically incremental Id generator
    int64 NextUserId
    int64 NextDBId
    int64 NextTableId
    int64 NextBlockId
    int64 NextReplicationGroupId
}

message User {
    int64 UserId
    map<string, int64> DBNameToId
    map<int64, DB> DBs

    // bytes Quota
}

message DB {
    int64 DBId
    map<string, int64> TableNameToId
    map<int64, Table> Tables
}

message Table {
    int64    TableId
    bytes    Schema
    // a func(string, Vec<ReplicationGroupId>) mapping PartitionBy expr to replication group.
    // A FuseQuery process should consider this to determine where to send the
    // read or write operations.
    bytes    PlacementPolicy
    repeated ReplicationGroupId ReplicationGroupIds
}
```

## Replication group meta

The meta data of a replication group is stored in the replication group raft group, e.g. 3 servers a group.
Replication group meta includes only block info.
A replication group is not necessary to be aware of what table a block belongs to,
although there is a TableId in a block meta.

```proto

message ReplicationGroupBlocks {
    int64 ReplicationGroupId 
    // PartitionBy is the serialized value of `partition by` expr in a table schema.
    string PartitionBy
    repeated Block Blocks
    // Removed blocks waiting to be cleared.
    repeated Block DetachedBlocks
}

// Block is a segment of a partition in a table.
message Block {
    int64 BlockId

    int64 TableId
    string PartitionBy

    string Tier // one of ["local", "s3"]

    // The bottom level is 0.
    int64 Level
    int64 Version

    // The on-disk fn of a block is in form of:
    // "{TableId}-{PartitionBy}-{Level}-{Version}"
}
```

> A server runs two raft instance: the replication group raft and the global
> meta raft.



# Example of a cluster

```
Lr:leader of replication group
Fr:follower of replication group

Lc: leader of cluster
Fc: follower of cluster
l:  learner


cluster: Lc---+-------+------+--------+-------.
              `->Fc   `->Fc   `->Fc   `->Fc   `->l
         
RG:      Lr---+-------.          Lr---+-------.    
              `->Fr   `->Fr           `->Fr   `->Fr
         
nodes:   N1      N2      N3      N4      N5      N6 ...
         -------------------     -------------------
         replication group 1     replication group 2
```


# Replication group

A **replication group** has 3 servers in it and constitutes a logic storage unit
with durability and availability guarantees.

A replication group is organized with a consensus protocol such as raft or
paxos which keeps block meta data synchronized among these 3 servers.
The meta data includes:
- what blocks a group has.
- etc.

A FuseStore cluster has one or more **replication-group**.

Data is split into non-overlapping ranges in some way, e.g., by  PartitionBy and OrderBy, 
it.

A replication group will be like:

```
FuseQuery:|
          |     Q1                 Q2
          |     |W-2               | W-1(write block)
          |     |                  |
          |     |                  |
----------------|------------------|-------------------------------
          |     v                  v     
          |     Follower-1 <-----> Leader <-------> Follower-2      // consensus protocol
          | 
          | -------------------------------------------------------
          | 
          |     block-3            block...         block-3      
          |                                                      
          |     block-2                             block-2      
FuseStore:|                                                      
          | block-1   block-1                   block-1   block-1
          |                                                      
          |  block-0  block-0 ...             block-0  block-0 ...
          |
          | -------------------------------------------------------
          |
          |     Node-1             Node-2           Node-3
          |
-------------------------------------------------------------------
          |
S3:       |              blocks...
          |
```

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
