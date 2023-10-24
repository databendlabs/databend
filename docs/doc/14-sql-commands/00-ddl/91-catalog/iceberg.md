---
title: Apache Iceberg
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.83"/>

Databend supports the integration of an [Apache Iceberg](https://iceberg.apache.org/) catalog, enhancing its compatibility and versatility for data management and analytics. This extends Databend's capabilities by seamlessly incorporating the powerful metadata and storage management capabilities of Apache Iceberg into the platform.

## Datatype Mapping to Databend

This table maps data types between Apache Iceberg and Databend. Please note that Databend does not currently support Iceberg data types that are not listed in the table.

| Apache Iceberg                  | Databend                |
| ------------------------------- | ----------------------- |
| boolean                         | boolean                 |
| int                             | int32                   |
| long                            | int64                   |
| date                            | date                    |
| timestamp/timestampz            | timestamp               |
| float                           | float                   |
| double                          | double                  |
| string/binary                   | string                  |
| decimal                         | decimal                 |
| array&lt;type&gt;                   | array, supports nesting |
| map&lt;KeyType, ValueType&gt;       | map                     |
| struct&lt;col1: Type1, col2: Type2, ...&gt; | tuple           |
| list                            | array                   |

## Managing Apache Iceberg Catalogs

Databend provides you the following commands to manage Apache Iceberg catalogs:

- CREATE CATALOG
- SHOW CREATE CATALOG

These commands share the same syntax as those used for Apache Hive. For more detailed information, see [Managing Apache Hive Catalogs](hive.md#managing-apache-hive-catalogs).

## Usage Examples

This example demonstrates the creation of a catalog configured to interact with an Iceberg data storage located in MinIO at 's3://databend/iceberg/'.

```sql
CREATE CATALOG iceberg_ctl
TYPE = ICEBERG
CONNECTION = (
    URL = 's3://databend/iceberg/'
    AWS_KEY_ID = 'minioadmin'
    AWS_SECRET_KEY = 'minioadmin'
    ENDPOINT_URL = 'http://127.0.0.1:9000'
    REGION = 'us-east-2'
);

SHOW CREATE CATALOG iceberg_ctl;

┌─────────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────┐
│  Catalog    │  Type   │  Option                                                                                │
├─────────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────┤
│ iceberg_ctl │ iceberg │ STORAGE PARAMS s3 | bucket=databend, root=/iceberg/, endpoint=http://127.0.0.1:9000    │
└─────────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────┘
```