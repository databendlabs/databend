# Databend Query

Databend Query is a Distributed Query Engine at scale.

- `ast`, new version of sql parser based on `nom-rule`.
- `catalog` contains structures and traits for catalogs management, `Catalog`, `Database`, `Table` and `TableContext`.
- `codegen` is used to generate the arithmetic result type.
- `config` provides config support for databend query.
- `datablocks`, `Vec` collection, which encapsulates some common methods, and will be gradually migrated to `expressions`.
- `datavalues`, the definition of each type of Column, which represents the layout of data in memory, will be gradually migrated to `expressions`.
- `expression`, the new scalar expression framework with expression definition (AST), type checking, and evaluation runtime.
- `formats`, the serialization and deserialization of data in various formats to the outside.
- `functions`, scalar functions and aggregate functions, etc.
- `functions-v2`, scalar functions and aggregate functions, etc., based on `expression`.
- `legacy-parser`, the old parser, which will be replaced by ast, built with sqlparser-rs.
- `menagement` for clusters, quotas, etc.
- `pipeline` implements the scheduling framework for physical operators.
- `planners` builds an execution plan from the user's SQL statement and represents the query with different types of relational operators.
- `service` -> `databend-query`, the query service library of Databend.
- `settings`, global and session level settings.
- `storages` relates to table engines, including the commonly used fuse engine and indexes etc.
- `streams` contains data sources and streams.
- `users`, role-based access and control.
