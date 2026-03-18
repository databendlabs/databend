# Databend Query

Databend Query is a Distributed Query Engine at scale.

- [`ast`](./ast/) contains the SQL parser and related parser tooling.
- [`catalog`](./catalog/) defines catalog-facing abstractions such as `Catalog`, `Database`, `Table`, and `TableContext`.
- [`codegen`](./codegen/) contains code generators used by query-side crates, including arithmetic result-type generation.
- [`config`](./config/) provides query-service configuration types and parsing.
- [`datavalues`](./datavalues/) is the legacy in-memory value/column representation layer; newer work continues to move toward [`expression`](./expression/).
- [`expression`](./expression/) is the core scalar expression framework, including expression representation, type checking, and evaluation.
- [`formats`](./formats/) handles data serialization and deserialization for external formats.
- [`functions`](./functions/) implements scalar functions, aggregate functions, and related function infrastructure.
- [`management`](./management/) contains query-side management utilities such as cluster and quota support.
- [`pipeline`](./pipeline/) implements the execution pipeline and scheduling framework for physical operators.
- [`script`](./script/) contains script execution support used by the query service.
- [`service`](./service/) is the `databend-query` service crate and runtime integration layer.
- [`settings`](./settings/) defines global and session settings.
- [`sql`](./sql/) contains SQL-side planning, binding, optimization, and related execution support. Read `./sql/README.md` for crate structure and shared optimizer test-support entry points.
- [`storages`](./storages/) contains table engines and storage-layer integrations, including Fuse and related indexing components.
- [`users`](./users/) implements user, role, and access-control support.
- [`ee`](./ee/) contains enterprise query functionality.
- [`ee_features`](./ee_features/) contains enterprise feature modules used by the query layer.
