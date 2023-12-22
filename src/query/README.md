# Databend Query

Databend Query is a Distributed Query Engine at scale.

- [`ast`](./ast/), new version of sql parser based on `nom-rule`.
- [`catalog`](./catalog/) contains structures and traits for catalogs management, `Catalog`, `Database`, `Table` and `TableContext`.
- [`codegen`](./codegen/) is used to generate the arithmetic result type.
- [`config`](./config/) provides config support for databend query.
- [`datavalues`](./datavalues/), the definition of each type of Column, which represents the layout of data in memory, will be gradually migrated to `expressions`.
- [`expression`](./expression/), the new scalar expression framework with expression definition (AST), type checking, and evaluation runtime.
- [`formats`](./formats/), the serialization and deserialization of data in various formats to the outside.
- [`functions`](./functions/), scalar functions and aggregate functions, etc.
- [`management`](./management/) for clusters, quotas, etc.
- [`pipeline`](./pipeline/) implements the scheduling framework for physical operators.
- [`service`](./service/) -> `databend-query`, the query service library of Databend.
- [`settings`](./settings/), global and session level settings.
- [`storages`](./storages/) relates to table engines, including the commonly used fuse engine and indexes etc.
- [`users`](./users/), role-based access and control.
- [`ee`](ee/) contains enterprise functionalities.
- [`ee-features`](ee_features/) contains enterprise features.
