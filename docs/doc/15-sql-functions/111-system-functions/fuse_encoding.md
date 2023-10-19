---
title: FUSE_ENCODING
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.162"/>

Returns the encoding types applied to a specific column within a table. It helps you understand how data is compressed and stored in a native format within the table.

## Syntax

```sql
FUSE_ENCODING('<database_name>', '<table_name>', '<column_name>')
```

The function returns a result set with the following columns:

| Column            | Data Type        | Description                                                                                                                                                                              |
|-------------------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| VALIDITY_SIZE     | Nullable(UInt32) | The size of a bitmap value that indicates whether each row in the column has a non-null value. This bitmap is used to track the presence or absence of null values in the column's data. |
| COMPRESSED_SIZE   | UInt32           | The size of the column data after compression.                                                                                                                                           |
| UNCOMPRESSED_SIZE | UInt32           | The size of the column data before applying encoding.                                                                                                                                    |
| LEVEL_ONE         | String           | The primary or initial encoding applied to the column.                                                                                                                                   |
| LEVEL_TWO         | Nullable(String) | A secondary or recursive encoding method applied to the column after the initial encoding.                                                                                               |

## Examples

```sql
-- Create a table with an integer column 'c' and apply 'Lz4' compression
CREATE TABLE t(c INT) STORAGE_FORMAT = 'native' COMPRESSION = 'lz4';

-- Insert data into the table.
INSERT INTO t SELECT number FROM numbers(2048);

-- Analyze the encoding for column 'c' in table 't'
SELECT LEVEL_ONE, LEVEL_TWO, COUNT(*) 
FROM FUSE_ENCODING('default', 't', 'c') 
GROUP BY LEVEL_ONE, LEVEL_TWO;

level_one   |level_two|count(*)|
------------+---------+--------+
DeltaBitpack|         |       1|

--  Insert 2,048 rows with the value 1 into the table 't'
INSERT INTO t (c)
SELECT 1
FROM numbers(2048);

SELECT LEVEL_ONE, LEVEL_TWO, COUNT(*) 
FROM FUSE_ENCODING('default', 't', 'c') 
GROUP BY LEVEL_ONE, LEVEL_TWO;

level_one   |level_two|count(*)|
------------+---------+--------+
OneValue    |         |       1|
DeltaBitpack|         |       1|
```