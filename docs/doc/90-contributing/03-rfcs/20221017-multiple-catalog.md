---
title: Multiple Catalog
description: multiple catalog support for databend
---

- RFC PR: [datafuselabs/databend#8255](https://github.com/datafuselabs/databend/pull/8255)
- Tracking Issue: [datafuselabs/databend#8300](https://github.com/datafuselabs/databend/issues/8300)

## Summary

Allow users to maintain multiple catalogs for the databend.

## Motivation

Databend organize data in three layers:

```txt
catalog -> database -> table
```

- `catalog`: the biggest layer for databend, contains all databases and tables, provided by [`Catalog`](https://github.com/datafuselabs/databend/blob/556aedc00e5e8a95a7551d0ec21b8e6fa7573e0a/src/query/catalog/src/catalog.rs#L80)
- `database`: the container of tables, provided by [`Database`](https://github.com/datafuselabs/databend/blob/556aedc00e5e8a95a7551d0ec21b8e6fa7573e0a/src/query/catalog/src/database.rs#L44)
- `table`: the smallest unit in databend, provided by [`Table`](https://github.com/datafuselabs/databend/blob/556aedc00e5e8a95a7551d0ec21b8e6fa7573e0a/src/query/catalog/src/table.rs#L44)

By default, all databases and tables will be stored in `default` catalog (powered by `metasrv`).

Databend supports multiple catalogs now, but only in a static way.

To allow accessing the `hive` catalog, users need to configure `hive` inside `databend-query.toml` in this way:

```toml
[catalog]
meta_store_address = "127.0.0.1:9083"
protocol = "binary"
```

Users can't add/alter/remove the catalogs during runtime.

By allowing users to maintain multiple catalogs for the databend, we can integrate more catalogs like `iceberg` more quickly.

## Guide-level explanation

After this RFC has been implemented, users can create new catalogs like:

```sql
CREATE CATALOG my_hive
  TYPE=HIVE
  CONNECTION = (URL='<hive-meta-store>' THRIFT_PROTOCOL=BINARY);
SELECT * FROM my_hive.DB.table;
```

Besides, users can alter or drop a catalog:

```sql
DROP CATALOG my_hive;
```

Users can add more catalogs like:

```sql
CREATE CATALOG my_iceberg
  TYPE=ICEBERG
  CONNECTION = (URL='s3://my_bucket/path/to/iceberg');
SELECT * FROM my_iceberg.DB.table;
```

With this feature, users can join data from different catalogs now:

```sql
select
    my_iceberg.DB.purchase_records.Client_ID,
    my_iceberg.DB.purchase_records.Item,
    my_iceberg.DB.purchase_records.QTY
from my_hive.DB.vip_info
inner join my_iceberg.DB.purchase_records
    on my_hive.DB.vip_info.Client_ID = my_iceberg.DB.purchase_records.Client_ID;
```

## Reference-level explanation

Databend has a framework for multiple catalogs now. The only change for us is to store catalog-related information in metasrv instead.

To make it possible to start a query without `metasrv`, we will also support configuring catalogs in config like:

```toml
[catalogs.my_hive]
meta_store_address = "127.0.0.1:9083"
protocol = "binary"

[catalogs.my_iceberg]
URL = "s3://bucket"
```

Static catalogs will always be loaded from configs and can't be altered or dropped.

## Drawbacks

None.

## Rationale and alternatives

None.

## Prior art

### Presto

[Presto](https://prestodb.io/) is an open-source SQL query engine that's fast, reliable, and efficient at scale. It doesn't have persisted states, so all its connectors will be configured.

Take iceberg as an example:

```ini
connector.name=iceberg
hive.metastore.uri=hostname:port
iceberg.catalog.type=hive
```

While using:

```sql
USE iceberg.tpch;
CREATE TABLE IF NOT EXISTS ctas_nation AS (SELECT * FROM nation);
DESCRIBE ctas_nation;
```

## Unresolved questions

None.

## Future possibilities

### Iceberg Catalog

Discussed in RFC [Iceberg External Table](https://github.com/datafuselabs/databend/pull/8215)

### Delta Sharing Catalog

Discussed in [Tracking issues of integration with delta sharing](https://github.com/datafuselabs/databend/issues/7830)
