---
title: INSERT INTO
---

Writes data into a table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Insert Direct Values

### Syntax

```sql
INSERT INTO|OVERWRITE [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

### Examples

```sql
CREATE TABLE test(a INT UNSIGNED, b Varchar);

INSERT INTO test(a,b) VALUES(888, 'stars');
INSERT INTO test VALUES(1024, 'stars');

SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
| 1024 | stars |
+------+-------+

INSERT OVERWRITE test VALUES(2048, 'stars');
SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
| 2048 | stars |
+------+-------+
```

## Insert Query Results

When inserting the results of a SELECT statement, the mapping of columns follows their positions in the SELECT clause. Therefore, the number of columns in the SELECT statement must be equal to or greater than the number of columns in the INSERT table. In cases where the data types of the columns in the SELECT statement and the INSERT table differ, type casting will be performed as needed.

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

### Examples

```sql
CREATE TABLE select_table(a VARCHAR, b VARCHAR, c VARCHAR);
INSERT INTO select_table VALUES('1','11','abc');

SELECT * FROM select_table;
+------+------+------+
| a    | b    | c    |
+------+------+------+
| 1    | 11   | abc  |
+------+------+------+

CREATE TABLE test(c1 TINTINT UNSIGNED, c2 BIGINT UNSIGNED, c3 VARCHAR);
INSERT INTO test SELECT * FROM select_table;

SELECT * from test;
+------+------+------+
| c1   | c2   | c3   |
+------+------+------+
|    1 |   11 | abc  |
+------+------+------+
```

Aggregate Example:

```sql
-- create table
CREATE TABLE base_table(a INT);
CREATE TABLE aggregate_table(b INT);

-- insert some data to base_table
INSERT INTO base_table VALUES(1),(2),(3),(4),(5),(6);

-- insert into aggregate_table from the aggregation
INSERT INTO aggregate_table SELECT SUM(a) FROM base_table GROUP BY a%3;

SELECT * FROM aggregate_table ORDER BY b;
+------+
| b    |
+------+
|    5 |
|    7 |
|    9 |
+------+
```

## Insert Default Values

Databend allows you to use the INSERT INTO statement to add data into a table, specifying values or defaults for columns as needed.

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v1|DEFAULT, v2|DEFAULT, v3|DEFAULT) ...
```

### Examples

```sql
CREATE TABLE t_insert_default(a int null, b int default 2, c float, d varchar default 'd');

INSERT INTO t_insert_default VALUES (default, default, default, default), (1, default, 1.0, default), (3, 3, 3.0, default), (4, 4, 4.0, 'a');

SELECT * FROM t_insert_default;
+------+------+------+------+
| a    | b    | c    | d    |
+------+------+------+------+
| NULL |    2 |  0.0 | d    |
|    1 |    2 |  1.0 | d    |
|    3 |    3 |  3.0 | d    |
|    4 |    4 |  4.0 | a    |
+------+------+------+------+
```

## Insert with Staged Files

Databend enables you to insert data into a table from staged files with the INSERT INTO statement. This is achieved through Databend's capacity to [Query Staged Files](../../12-load-data/00-transform/05-querying-stage.md) and subsequently incorporate the query result into the table.

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

### Examples

1. Create a table called "sample":

```sql
CREATE TABLE sample
(
    Id      INT,
    City    VARCHAR,
    Score   INT,
    Country VARCHAR DEFAULT 'China'
);
```

2. Create an internal stage and upload a sample CSV file called [sample_3_replace.csv](https://github.com/ZhiHanZ/databend/blob/0f333a13fc38548595ea58242a37c5f4a73e9c88/tests/data/sample_3_replace.csv) to the stage with PRESIGN:

```sql
CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);

PRESIGN UPLOAD @s1/sample_3_replace.csv;

| method | headers                               | url                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PUT    | {"host":"s3.us-east-2.amazonaws.com"} | https://s3.us-east-2.amazonaws.com/query-storage-53b9412/tn3ftqihs/stage/internal/s1/sample_3_replace.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAT2EUTJTKFPM7OYFZ%2F20230913%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230913T023209Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDsaCXVzLWVhc3QtMiJIMEYCIQDfDn6EBei4IHRhmFNldu%2FpafMhHwx%2B934HQDafsfFQOAIhAI38G%2FaKG3GFso8qHBCguoL3GvXUIDaKDJ3bJs5VBSwoKvwECCQQABoMMjYyMzA0NjQ4NDA0IgxyYEa7Xes%2Bb%2FnDT%2Fkq2QRkkQi83V9qVKyZJP0UoOBZEaFIS1qkPd2gEObVd3%2BA8yq8wVhdr749DmvZ7sXWlcXsXmXOjnl9cxwkvcJuXZ%2F1LVO5Kh3vTSF3dbNkbkIY3z9pEOX70llHFSenSSo8f44wqzsFkuLanwpzWjL2eFn%2B1boz7iDuWY7p2bb7ZtoTkYat4TrHQWpG2hPayk3Sn6ueAfBMCnYJ3oMy2a1G7F0onz3pM%2FFSRxCe7tsPMAEg2wP24YnXhKCUaq7xo8Gvy81FKNhhPr8XWYW0tHBON3aWh7t1q6mJw%2B3KeUtMI6Cdz1BsqhGpLgMUB%2BPctxHmlm2UVUk72LsmxioAKq4Fl48jFsMz7fwKjbheMqv6jKlzgu%2B8B4V6DCo2KqsTsip%2FoOevk4R4X5OTqA4FQ3Qy%2BX%2FtMUMKohXkXKYSJPP15XPOYsogXrQWhszK%2B%2FaUth%2FzY8GAzYf0MemnooACTkDE6A8v5uB%2FRoPQwSPCQ0Dwbn%2FNrLVC3c649l%2FWh7iy2FcE2CDm7yppj5XklttYuhwiuQ%2B2WnDcRn0yesqeTeRoDP0lBZyGj%2FlB7hATTqDZV2lSSFI737sU8BWBncOoTqBltaClBdtIQkTtmheDAMtNdQ8zvF5ZmFetF4eUU0D3AZ3FD90lTUZ6gSPGfVlIZbwY%2BBW%2FmG1tP5%2BaokXkMnywPaYvtep1HwR3cHg%2B8qoZW5o11yPCRAd0MEZmOaYO18JSYuwejam8pb%2F1BVbi%2B6a1W62ohAa4zCH29%2BGGISNqjLcKTZQOA6gEt7%2BZoUxd4mQ5wg4BxIpqEXL%2F0YpcMKm%2BhKgGOpoBWEV2udBO%2FX9wSP%2FAMK4KwmeIboZ1aQpwkBgUmtP%2FsgXErKghAm54PA7dK1n7sm%2FqOBQjXuRWTj%2B3iykJaT97dWoutgmqYgqj377TweIVffXF0cSHx1%2F3ri3aXmZ9fh4GAfcfhzs7NugH%2Fk2IkORKHHv3tGmlKGHLVp8XL0bXIqTsCthRRJvOwlYIaPumBhfaEA38PAs%2BSeEwwA%3D%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=43e3aa7c2bb5a08ce8ae9746b70b1d1c743a57937fa2bd596b1170f00bcf4f34 |
```

```shell
curl -X PUT -T sample_3_replace.csv "https://s3.us-east-2.amazonaws.com/query-storage-53b9412/tn3ftqihs/stage/internal/s1/sample_3_replace.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAT2EUTJTKFPM7OYFZ%2F20230913%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230913T023209Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDsaCXVzLWVhc3QtMiJIMEYCIQDfDn6EBei4IHRhmFNldu%2FpafMhHwx%2B934HQDafsfFQOAIhAI38G%2FaKG3GFso8qHBCguoL3GvXUIDaKDJ3bJs5VBSwoKvwECCQQABoMMjYyMzA0NjQ4NDA0IgxyYEa7Xes%2Bb%2FnDT%2Fkq2QRkkQi83V9qVKyZJP0UoOBZEaFIS1qkPd2gEObVd3%2BA8yq8wVhdr749DmvZ7sXWlcXsXmXOjnl9cxwkvcJuXZ%2F1LVO5Kh3vTSF3dbNkbkIY3z9pEOX70llHFSenSSo8f44wqzsFkuLanwpzWjL2eFn%2B1boz7iDuWY7p2bb7ZtoTkYat4TrHQWpG2hPayk3Sn6ueAfBMCnYJ3oMy2a1G7F0onz3pM%2FFSRxCe7tsPMAEg2wP24YnXhKCUaq7xo8Gvy81FKNhhPr8XWYW0tHBON3aWh7t1q6mJw%2B3KeUtMI6Cdz1BsqhGpLgMUB%2BPctxHmlm2UVUk72LsmxioAKq4Fl48jFsMz7fwKjbheMqv6jKlzgu%2B8B4V6DCo2KqsTsip%2FoOevk4R4X5OTqA4FQ3Qy%2BX%2FtMUMKohXkXKYSJPP15XPOYsogXrQWhszK%2B%2FaUth%2FzY8GAzYf0MemnooACTkDE6A8v5uB%2FRoPQwSPCQ0Dwbn%2FNrLVC3c649l%2FWh7iy2FcE2CDm7yppj5XklttYuhwiuQ%2B2WnDcRn0yesqeTeRoDP0lBZyGj%2FlB7hATTqDZV2lSSFI737sU8BWBncOoTqBltaClBdtIQkTtmheDAMtNdQ8zvF5ZmFetF4eUU0D3AZ3FD90lTUZ6gSPGfVlIZbwY%2BBW%2FmG1tP5%2BaokXkMnywPaYvtep1HwR3cHg%2B8qoZW5o11yPCRAd0MEZmOaYO18JSYuwejam8pb%2F1BVbi%2B6a1W62ohAa4zCH29%2BGGISNqjLcKTZQOA6gEt7%2BZoUxd4mQ5wg4BxIpqEXL%2F0YpcMKm%2BhKgGOpoBWEV2udBO%2FX9wSP%2FAMK4KwmeIboZ1aQpwkBgUmtP%2FsgXErKghAm54PA7dK1n7sm%2FqOBQjXuRWTj%2B3iykJaT97dWoutgmqYgqj377TweIVffXF0cSHx1%2F3ri3aXmZ9fh4GAfcfhzs7NugH%2Fk2IkORKHHv3tGmlKGHLVp8XL0bXIqTsCthRRJvOwlYIaPumBhfaEA38PAs%2BSeEwwA%3D%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=43e3aa7c2bb5a08ce8ae9746b70b1d1c743a57937fa2bd596b1170f00bcf4f34"
```

```sql
LIST @s1;

| name                 | size | md5                                | last_modified                 | creator |
|----------------------|------|------------------------------------|-------------------------------|---------|
| sample_3_replace.csv | 83   | "42807a735a36f9bde392fee5834b22c4" | 2023-09-13 02:43:29.000 +0000 | NULL    |
```

3. Insert data from the staged CSV file with INSERT INTO:

:::tip
You can specify the file format and various copy-related settings with the FILE_FORMAT and COPY_OPTIONS available in the [COPY INTO](dml-copy-into-table.md) command. When `purge` is set to `true`, the original file will only be deleted if the data update is successful. 
:::

```sql
INSERT INTO sample (Id, City, Score) SELECT $1, $2, $3 FROM @s1 (FILE_FORMAT=>'csv');

-- Verify the inserted data
SELECT * FROM sample;

id|city       |score|country|
--+-----------+-----+-------+
 1|'Chengdu'  |   80|China  |
 3|'Chongqing'|   90|China  |
 6|'HangZhou' |   92|China  |
 9|'Changsha' |   91|China  |
10|'Hong Kong'|   88|China  |
```