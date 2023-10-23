---
title: Apache Iceberg
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.83"/>

Databend supports the integration of an [Apache Iceberg](https://iceberg.apache.org/) catalog, enhancing its compatibility and versatility for data management and analytics. This extends Databend's capabilities by seamlessly incorporating the powerful metadata and storage management capabilities of Apache Iceberg into the platform.

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
);

SHOW CREATE CATALOG iceberg_ctl;

| Catalog     | Type    | Option                                                                            |
|-------------|---------|-----------------------------------------------------------------------------------|
| iceberg_ctl | iceberg | STORAGE PARAMS s3 | bucket=databend,root=/iceberg/,endpoint=http://127.0.0.1:9000 |
```