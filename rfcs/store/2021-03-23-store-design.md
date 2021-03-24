# FuseStore V1: Single node storage

FuseStore is the storage layer in charge of:
- meta data storage such as user, db, table and schema.
- blocks life cycle management such as allocation, compaction etc.
- data/metadata consistency and reliability.

# API

FuseStore provides the following APIs to FuseQuery:

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

# Commit protocol for Write

- Handle request: Store Node (S) receives a Block-Write grpc request.

- Load overlapping Blocks, merge them.
    Write the merged block to disk.

    > This simplified impl amplifies write IO but keeps blocks all at the
    > same level.

- Build updated meta, write WAL.

- Update meta, respond OK.


# Startup

When starting up a FuseStore server, it:

- Load the last valid WAL into memory.

- Start listening.


# Meta data

All meta resides in memory for quick access.

Every update to meta includes two steps:
- Append a WAL entry with all serialized meta data.
- Update in-memory meta.

Loading meta data currently is just a read of the last valid WAL log.


## Concepts

```
^ +-> User-1 +-> DB-1 +-> Table-1 +-> Partition-1 --> Block-1
  |          |        |           |                   Block-2
  |          |        |           `-> Partition-2 --> ...
  |          |        |
  |          |        `-> Table-2 --> ...
  |          `-> DB-2 --> ...
  `-> User-2 --> ...
```


## Cluster meta

Cluster meta includes info such as server id, location and consensus group etc.

With a single-node deploy, there is only one node with id 0.

```proto
message Cluster {
    map<int64, Node> Nodes
}

message Node {
    int64 NodeId
    repeated string Addresses
}
```

## DB meta

DB meta describes the hierarchical structure of user, database and tables.

```proto
message Meta {
    map<string, int64> UserNameToId
    map<int64, User> Users

    // monotonically incremental Id generator
    int64 NextUserId
    int64 NextDBId
    int64 NextTableId
    int64 NextBlockId
}

message User {
    int64 UserId
    map<string, int64> DBNameToId
    map<int64, DB> DBs

    // bytes Quota
}

message DB {
    int64 DBId
    bytes Schema
    map<string, int64> TableNameToId
    map<int64, Table> Tables
}

message Table {
    int64    TableId
    map<string, Partition> Partitions
}

message Partition {
    // PartitionBy is the serialized value of `partition by` expr in a table schema.
    string PartitionBy
    repeated Block Blocks
    // Removed blocks waiting to be cleared.
    repeated Block DetachedBlocks
}

// Block is a segment of a partition in a table.
// [OrderByStart, OrderByStart) is the range a Block spans in the `order by` value space.
//
// Tow blocks with the same PartitionBy and overlapping [OrderByStart, OrderByEnd) will be merged to one with the lower level.
message Block {
    string PartitionBy

    bytes OrderByStart
    bytes OrderByEnd

    int64 BlockId
    string Tier // one of ["local", "s3"]

    // The bottom level is 0.
    int64 Level
}
```

# On-disk file format

A block is a complete Parquet data, with schema embedded.
The schema in block should always be identical to the schema stored in
table-meta. Otherwise it is a severe bug.

Parquet has its own segmentation and index in it which is similar to ClickHouse
file structure.

See: https://parquet.apache.org/documentation/latest/


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


A replicated storage will be like:

```
FuseQuery:|
          |     Q1                 Q2
          |                        | W-1(write block)
          |                        |
          |                        |
-----------------------------------|-------------------------------
          |                        v     
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

A block is either a file on local fs, or an object in S3.
It is possible a block has copies on both local fs and S3 if:
- The block has not yet been cleaned from local fs after dumping to S3.
- The block is pinned on local fs to speed up read.
