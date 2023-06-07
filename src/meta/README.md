# Databend Meta

Databend Meta is a transactional metadata service.

- [`api`](./api/), the user level api interface exposed based on the KVApi implementation.
- [`app`](./app/), defines meta data types used by meta-client application.
- [`embedded`](./embedded/), a meta store backed with a local sled::Tree.
- [`grpc`](client/), the client library based on grpc and is used to communicate with meta service.
- [`protos`](./protos/) defines the protobuf messages a meta client talks to a meta server.
- [`proto-conv`](./proto-conv/) defines how to convert metadata types in rust from and to protobuf messages.
- [`raft-store`](./raft-store/), the storage layer implementation of openraft, including the state machine.
- [`service`](./service/) -> `databend-meta`, the meta service library of Databend.
- [`sled-store`](./sled-store/) wrapped sled-related operational interfaces.
- [`store`](./store/), impl with either a local embedded meta store, or a grpc-client of meta service.
- [`types`](./types/): defines the rust types for metadata.
- [`ee`](./ee/) contains enterprise functionalities.


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
