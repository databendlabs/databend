---
title: Unloading Data
---

Unloading data refers to the process of extracting or transferring data stored in a database to another storage location. This can involve exporting data from the database to a file or another database, or copying data from the database to a backup or archiving system. 

Databend recommends using the `COPY INTO <location>` command to export your data to a stage or an external location as a file in one of the supported formats. This command is a convenient and efficient way to transfer data out of the database and into a file for further processing or analysis. 

For more information about the command, see [`COPY INTO <location>`](https://databend.rs/doc/sql-commands/dml/dml-copy-into-location). To view the list of supported file formats that can be used to save the exported data, see [Input & Output File Formats](https://databend.rs/doc/sql-reference/file-format-options).

## Tutorial - Unload to an External Stage

In this tutorial, you will first create an external stage and then use the COPY INTO command to export the result of a query as a parquet file to the external stage.

### Step 1. Create External Stage

Create an external stage named `unload` with the [CREATE STAGE](https://databend.rs/doc/sql-commands/ddl/stage/ddl-create-stage) command:

```sql
CREATE STAGE unload url='s3://unload/files/' connection=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z');
```

### Step 2. Export Data

Export the query result as a parquet file to the external stage `unload`:

```sql
COPY INTO @unload FROM (SELECT * FROM numbers(10000000)) FILE_FORMAT = (TYPE = PARQUET);
```

### Step 3. Verify Export File

Show the exported file with the [LIST STAGE](https://databend.rs/doc/sql-commands/ddl/stage/ddl-list-stage) command:

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