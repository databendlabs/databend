---
title: ATTACH TABLE
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.10"/>

Attaches an existing table to another one. The command moves the data and schema of a table from one database to another, but without actually copying the data. Instead, it creates a link that points to the original table data for accessing the data.

Attach Table enables you to seamlessly connect a table in the cloud service platform to an existing table deployed in a private deployment environment without the need to physically move the data. This is particularly useful when you want to migrate data from a private deployment of Databend to Databend Cloud while minimizing the data transfer overhead.

## Syntax

```sql
ATTACH TABLE <target_table_name> '<source-table-data-URI>' CONNECTION=(<connection_parameters>);
```

`<source-table-data-URI>` represents the path to the source table's data. For S3-like object storage, the format is `s3://<bucket-name>/<database_ID>/<table_ID>`, for example, *s3://databend-toronto/1/23351/*, which represents the exact path to the table folder within the bucket.

![Alt text](../../../../public/img/sql/attach.png)

To obtain the database ID and table ID of a table, use the [FUSE_SNAPSHOT](../../../15-sql-functions/111-system-functions/fuse_snapshot.md) function. In the example below, the part **1/23351/** in the value of *snapshot_location* indicates that the database ID is **1**, and the table ID is **23351**.

```sql
SELECT * FROM FUSE_SNAPSHOT('default', 'employees');

Name                |Value                                              |
--------------------+---------------------------------------------------+
snapshot_id         |d6cd1f3afc3f4ad4af298ad94711ead1                   |
snapshot_location   |1/23351/_ss/d6cd1f3afc3f4ad4af298ad94711ead1_v4.mpk|
format_version      |4                                                  |
previous_snapshot_id|                                                   |
segment_count       |1                                                  |
block_count         |1                                                  |
row_count           |3                                                  |
bytes_uncompressed  |122                                                |
bytes_compressed    |523                                                |
index_size          |470                                                |
timestamp           |2023-07-11 05:38:27.0                              |
```

## Examples

This example demonstrates how to attach a new table to the data of an existing table stored in a bucket named "databend-toronto" on Amazon S3.

```sql
CREATE TABLE employees (
  id INT,
  name VARCHAR(50),
  salary DECIMAL(10, 2)
) ;

INSERT INTO employees (id, name, salary) VALUES
  (1, 'John Doe', 5000.00),
  (2, 'Jane Smith', 6000.00),
  (3, 'Mike Johnson', 7000.00);
 
SELECT * FROM employees;

id|name        |salary |
--+------------+-------+
 1|John Doe    |5000.00|
 2|Jane Smith  |6000.00|
 3|Mike Johnson|7000.00|

-- Obtain database ID and table ID
SELECT * FROM FUSE_SNAPSHOT('default', 'employees');

Name                |Value                                              |
--------------------+---------------------------------------------------+
snapshot_id         |d6cd1f3afc3f4ad4af298ad94711ead1                   |
snapshot_location   |1/23351/_ss/d6cd1f3afc3f4ad4af298ad94711ead1_v4.mpk|
format_version      |4                                                  |
previous_snapshot_id|                                                   |
segment_count       |1                                                  |
block_count         |1                                                  |
row_count           |3                                                  |
bytes_uncompressed  |122                                                |
bytes_compressed    |523                                                |
index_size          |470                                                |
timestamp           |2023-07-11 05:38:27.0                              |

ATTACH TABLE employees_backup 's3://databend-toronto/1/23351/' CONNECTION=(aws_key_id='<your-key-id>' aws_secret_key='<your-secret-key>' region='us-east-2');

SELECT * FROM employees_backup;

id|name        |salary |
--+------------+-------+
 1|John Doe    |5000.00|
 2|Jane Smith  |6000.00|
 3|Mike Johnson|7000.00|
```