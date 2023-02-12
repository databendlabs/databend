---
title: Unloading Data from Databend
slug: ./
---

Databend provides a variety of tools for loading and unloading data. One of the methods of unloading data from Databend is using the [`COPY INTO <location>`](../14-sql-commands/10-dml/dml-copy-into-location.md) command. 

This command is used to export data from a Databend table or query into a file format such as CSV, Parquet, Or JSON.


## Unloading Data

### Creating a Stage

Before unloading data, you need to create a stage, which acts as a storage location for the exported data. 
You can choose to [create an external or internal stage](../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md), depending on your requirements.

Here's an example of how to create an external stage:
```sql
CREATE STAGE unload url='s3://unload/files/' connection=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z');
```

### Unloading Data

Once you have created a stage, you can use the COPY INTO command to export data:
```sql
COPY INTO @unload FROM (SELECT * FROM numbers(10000000)) FILE_FORMAT = (TYPE = PARQUET);
```

### Verifying the Exported Data

You can use the [`LIST STAGE`](../14-sql-commands/50-list/list-stage.md) command to show the exported data files:
```sql
LIST @unload;
+--------------------------------------------------------+----------+------------------------------------+-------------------------------+---------+
| name                                                   | size     | md5                                | last_modified                 | creator |
+--------------------------------------------------------+----------+------------------------------------+-------------------------------+---------+
| data_8799a438-9788-4dcb-bd45-3aa23ea9c6a3_32_0.parquet | 41486538 | "F187251F37666928684DBED4AF0523DF" | 2023-02-12 03:45:03.000 +0000 | NULL    |
+--------------------------------------------------------+----------+------------------------------------+-------------------------------+---------+
```

You can also query the exported data to confirm its validity:
```sql
SELECT sum(number) FROM @unload (PATTERN => '.*parquet');
+----------------+
| sum(number)    |
+----------------+
| 49999995000000 |
+----------------+
```
