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

| Column    | Description                                                                                   |
|-----------|-----------------------------------------------------------------------------------------------|
| LEVEL_ONE | The primary or initial encoding applied to the column.                                        |
| LEVEL_TWO | A secondary or recursive encoding method applied to the column after the initial encoding.    |
| COUNT(*)  | The count of occurrences of each unique combination of LEVEL_ONE and LEVEL_TWO in the result. |

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