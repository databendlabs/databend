# Databend Meta

Databend Meta is a transactional metadata service.

- [`api`](./api/), the user level api interface exposed based on the KVApi implementation.
- [`app`](./app/), defines meta data types used by meta-client application.
- [`embedded`](./embedded/), a meta store backed with a local sled::Tree.
- [`grpc`](./grpc/), the client library based on grpc and is used to communicate with meta service.
- [`protos`](./protos/) defines the protobuf messages a meta client talks to a meta server.
- [`proto-conv`](./proto-conv/) defines how to convert metadata types in rust from and to protobuf messages.
- [`raft-store`](./raft-store/), the storage layer implementation of openraft, including the state machine.
- [`service`](./service/) -> `databend-meta`, the meta service library of Databend.
- [`sled-store`](./sled-store/) wrapped sled-related operational interfaces.
- [`store`](./store/), impl with either a local embedded meta store, or a grpc-client of meta service.
- [`types`](./types/): defines the rust types for metadata.
