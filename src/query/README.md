# Databend Query

Databend Query is a Distributed Query Engine at scale.

- [`ast`](./ast/), new version of sql parser based on `nom-rule`.
- [`catalog`](./catalog/) contains structures and traits for catalogs management, `Catalog`, `Database`, `Table` and `TableContext`.
- [`codegen`](./codegen/) is used to generate the arithmetic result type.
- [`config`](./config/) provides config support for databend query.
- [`datablocks`](./datablocks/), `Vec` collection, which encapsulates some common methods, and will be gradually migrated to `expressions`.
- [`datavalues`](./datavalues/), the definition of each type of Column, which represents the layout of data in memory, will be gradually migrated to `expressions`.
- [`expression`](./expression/), the new scalar expression framework with expression definition (AST), type checking, and evaluation runtime.
- [`formats`](./formats/), the serialization and deserialization of data in various formats to the outside.
- [`functions`](./functions/), scalar functions and aggregate functions, etc., will be gradually migrated to `functions-v2`.
- [`functions-v2`](./functions-v2/), scalar functions and aggregate functions, etc., based on `expression`.
- [`legacy-parser`](./legacy-parser/), the old parser, which will be replaced by ast, built with sqlparser-rs.
- [`menagement`](./menagement/) for clusters, quotas, etc.
- [`pipeline`](./pipeline/) implements the scheduling framework for physical operators.
- [`planners`](./planners/) builds an execution plan from the user's SQL statement and represents the query with different types of relational operators.
- [`service`](./service/) -> `databend-query`, the query service library of Databend.
- [`settings`](./settings/), global and session level settings.
- [`storages`](./storages/) relates to table engines, including the commonly used fuse engine and indexes etc.
- [`streams`](./streams/) contains data sources and streams.
- [`users`](./users/), role-based access and control.
