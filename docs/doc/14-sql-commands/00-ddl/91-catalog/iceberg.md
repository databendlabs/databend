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
| BOOLEAN                         | [BOOLEAN](../../../13-sql-reference/10-data-types/00-data-type-logical-types.md)                 |
| INT                             | [INT32](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#integer-data-types)                   |
| LONG                            | [INT64](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#integer-data-types)                   |
| DATE                            | [DATE](../../../13-sql-reference/10-data-types/20-data-type-time-date-types.md)                    |
| TIMESTAMP/TIMESTAMPZ            | [TIMESTAMP](../../../13-sql-reference/10-data-types/20-data-type-time-date-types.md)               |
| FLOAT                           | [FLOAT](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#floating-point-data-types)                   |
| DOUBLE                          | [DOUBLE](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#floating-point-data-types)                  |
| STRING/BINARY                   | [STRING](../../../13-sql-reference/10-data-types/30-data-type-string-types.md)                  |
| DECIMAL                         | [DECIMAL](../../../13-sql-reference/10-data-types/11-data-type-decimal-types.md)                 |
| ARRAY&lt;TYPE&gt;               | [ARRAY](../../../13-sql-reference/10-data-types/40-data-type-array-types.md), supports nesting |
| MAP&lt;KEYTYPE, VALUETYPE&gt;       | [MAP](../../../13-sql-reference/10-data-types/42-data-type-map.md)                     |
| STRUCT&lt;COL1: TYPE1, COL2: TYPE2, ...&gt; | [TUPLE](../../../13-sql-reference/10-data-types/41-data-type-tuple-types.md)           |
| LIST                            | [ARRAY](../../../13-sql-reference/10-data-types/40-data-type-array-types.md)                   |

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