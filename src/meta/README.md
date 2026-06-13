# Databend Meta

2026 Feb 01

Since v1.2.874, core meta-service implementation is moved to [databend-meta](https://github.com/databendlabs/databend-meta) repository.

Databend Meta is a transactional metadata service.

**Application layer**:
- [`api`](./api/): User-level API interface exposed based on the KVApi implementation.
- [`app`](./app/): Meta data types used by meta-client applications.
- [`app-storage`](./app-storage/): Storage-related types split from app.
- [`store`](./store/): MetaStore impl with either a local embedded store or a grpc-client.

**Serialization**:
- [`protos`](./protos/): Protobuf message definitions for client-server communication.
- [`proto-conv`](./proto-conv/): Conversion between protobuf messages and Rust types.

**Higher-level features**:
- [`admin`](./admin/): Admin HTTP API service for databend-meta.
- [`cache`](./plugins/cache/): Distributed cache that synchronizes with meta-service via watch API.
- [`control`](./control/): Meta control utilities.
- [`process`](./process/): Tools for processing meta-service data.
- [`semaphore`](./plugins/semaphore/): Distributed semaphore implementation based on meta-service.

**Configuration and runtime**:
- [`cli-config`](./cli-config/): CLI config parsing for databend-meta.
- [`runtime`](./runtime/): Runtime adapter for meta-service using databend-common-base.

**Binaries** (`binaries/`):
- `databend-meta`: The meta service binary.
- `databend-metabench`: Benchmarking tool.
- `databend-metactl`: Command-line control tool.
- `databend-metaverifier`: Data verification tool.

**Test suites**:
- [`kv-tests`](./kv-tests/): Test suite for KV API implementations.
- [`schema-api-test-suite`](./schema-api-test-suite/): Test suite for schema API.

**Enterprise**:
- [`ee`](./ee/): Enterprise functionalities.


## How to add new meta data types to store in meta-service

Databend meta-service stores raw bytes and does not understand what the bytes are.

Databend-query use rust types in its runtime, these types such as `TableMeta`
must be serialized to be stored in meta-service.

The serialization is implemented with `protobuf` and a protobuf message provides
the backward compatibility, i.e., a newer version(version-B) protobuf message can be deserialized
from an older version(version-A) of serialized bytes, and version-B protobuf
message can be converted to version-B rust types.

- Rust types are defined in `src/meta/app/src/`,  such as `TableMeta` that is
  defined in `src/meta/app/src/schema/table.rs`.

- The corresponding protobuf message is defined in `src/meta/protos/proto/`,
  such as `src/meta/protos/proto/table.proto`.

- The conversion between protobuf message and rust type is defined in
  `src/meta/proto-conv/`, such as
  `src/meta/proto-conv/src/table_from_to_protobuf_impl.rs`,
  by implementing a `FromToProto` trait.

To add a new feature(add new type or update an type), the developer should do:

- Add the rust types, in one mod in the `src/meta/app/src/`;

- Add a new version in `src/meta/proto-conv/src/util.rs`. The versions track
    change history and will be checked when converting protobuf message to rust
    types:

    ```rust
    const META_CHANGE_LOG: &[(u64, &str)] = &[
        //
        ( 1, "----------: Initial", ),
        ( 2, "2022-07-13: Add: share.proto", ),
        ( 3, "2022-07-29: Add: user.proto/UserOption::default_role", ),
        ...
        (37, "2023-05-05: Add: index.proto", ),
        (38, "2023-05-19: Rename: table.proto/TableCopiedFileLock to EmptyProto", ),
        (39, "2023-05-22: Add: data_mask.proto", ),
    ];
    ```

    Note that only add new version to the bottom and remove old version from the
    top.

- Add the conversion implementation to `src/meta/proto-conv/src/`, refer to
    other files in this crate.

- Add a compatibility test to ensure that compatibility will always be kept in
    future, a good example is: `src/meta/proto-conv/tests/it/v039_data_mask.rs`





## Compatibility with Databend Query

The Query-Meta compatibility changes when `MIN_METASRV_SEMVER` or `MIN_METACLI_SEMVER` changes.

The compatibility is stored in [compat.py](compat.py), and a corresponding illustration is generated:

![](compatibility_chart.png)

History versions that are not included in the above chart:

- Query `[0.7.59, 0.8.80)` is compatible with Meta `[0.8.30, 0.9.23)`.
- Query `[0.8.80, 0.9.41)` is compatible with Meta `[0.8.35, 0.9.42)`.
- Query `[0.9.41, 1.1.34)` is compatible with Meta `[0.8.35, 1.2.663)`.
- Query `[1.1.34, 1.2.287)` is compatible with Meta `[1.1.32, 1.2.63)`.


## Compatibility between databend-meta

| Meta version        | Backward compatible with |
|:--------------------|:-------------------------|
| [0.9.41,   1.2.212) | [0.9.41,  1.2.212)       |
| [1.2.212,  1.2.479) | [0.9.41,  1.2.479)       |
| [1.2.479,  1.2.655) | [1.2.288, 1.2.655)       |
| [1.2.655,  1.2.769) | [1.2.288, 1.2.769)       |
| [1.2.769, +∞)       | [1.2.547, +∞)            |


![Image](https://github.com/user-attachments/assets/f63d80ee-f646-4d6a-9bec-be607e47088d)

- `1.2.53` Incompatible, rolling upgrade is allowed without snapshot transmitting.
  Snapshot format changed thus during rolling upgrading,
  it requires all node data to be up-to-date, ensure there is no need to replicate with snapshot.

- `1.2.163` Feature: gRPC API: `kv_read_v1()` is added. For stream reading.

- `1.2.212` 2023-11-16 Feature: raft API: `install_snapshot_v1()`. Compatible with old versions.
  Rolling upgrade is supported.
  In this version, databend-meta raft-server introduced a new API `install_snapshot_v1()`.
  The raft-client will try to use either this new API or the original `install_snapshot()`.

- `1.2.479` 2024-05-21 Remove: `install_snapshot()`(v0) from client and server.
  The `install_snapshot_v1()` is the only API to install snapshot, and becomes **REQUIRED** for the client.

- `1.2.528` 2024-06-13 Remove on-disk data version `V001`. The first version using `V002` is `1.2.53`, 2023-08-08.
  Therefore, since `1.2.528`, the oldest compatible version is `1.2.53`.
  Consequently, compatibility remains unchanged from this version onward.

- `1.2.552` 2024-07-02 Introduce on-disk `V003`, using `rotbl` format snapshot,
  which is compatible with `V002`. The oldest compatible version is `1.2.288`(`1.2.212~1.2.287` are removed).

- `1.2.655` 2024-11-11 Introduce on-disk `V004`, using WAL based Raft log storage,
  which is compatible with `V002`. The oldest compatible version is `1.2.288`(`1.2.212~1.2.287` are removed).
    

## Compatibility of databend-meta on-disk data

The on-disk data of Databend-meta evolves over time while maintaining backward compatibility.

| DataVersion | Databend-version | Min Compatible with |
|:------------|:-----------------|:--------------------|
| V004        | 1.2.655          | V002                | 
| V003        | 1.2.547          | V002                | 
| V002        | 1.2.53           | V001                | 
| V001        | 1.1.40           | V0                  |

### Identifying the versions

Upon startup, Databend-meta will display the on-disk data version:

For example, running `databend-meta --single` produces:

```
Databend Metasrv

Version: v1.1.33-nightly-...
Working DataVersion: V0

On Disk Data:
    Dir: ./.databend/meta
    Version: version=V0, upgrading=None
```

- `Working DataVersion` denotes the version Databend-meta operates on.
- `On Disk Data -- DataVersion` denotes the version of the on-disk data.

The Working DataVersion must be greater than or equal to the on-disk DataVersion; otherwise, it will panic.

The on-disk DataVersion must be compatible with the current Databend-meta version.
If not, the system will prompt the user to downgrade Databend-meta and quit with a panic.

### Automatic upgrade

When `databend-meta` starting up, the on-disk is upgraded if it is compatible with the working DataVersion.
The upgrade progress will be printed to `stderr` and to log file at INFO level, e.g.:

```text
Upgrade on-disk data
    From: V0(2023-04-21: compatible with openraft v07 and v08, using openraft::compat)
    To:   V001(2023-05-15: Get rid of compat, use only openraft v08 data types)
Begin upgrading: version: V0, upgrading: V001
Write header: version: V0, upgrading: V001
Upgraded 167 records
Finished upgrading: version: V001, upgrading: None
Write header: version: V001, upgrading: None
```

If `databend-meta` crashes before upgrading finishes,
it will clear partially upgraded data and resume the upgrade when it starts up again.

### Backup data compatibility

- The exported backup data **can only be imported** with the same version of `databend-metactl`.

- The first line of the backup is the version, e.g.:
  `["header",{"DataHeader":{"key":"header","value":{"version":"V100","upgrading":null}}}]`

- **NO automatic upgrade** will be done when importing.
  Automatic upgrade will only be done when `databend-meta` is brought up.
