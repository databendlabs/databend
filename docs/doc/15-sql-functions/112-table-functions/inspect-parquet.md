---
title: INSPECT_PARQUET
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.180"/>

Retrieves a table of comprehensive metadata from a staged Parquet file, including the following columns:

| Column                           | Description                                                    |
|----------------------------------|----------------------------------------------------------------|
| created_by                       | The entity or source responsible for creating the Parquet file |
| num_columns                      | The number of columns in the Parquet file                      |
| num_rows                         | The total number of rows or records in the Parquet file        |
| num_row_groups                   | The count of row groups within the Parquet file                |
| serialized_size                  | The size of the Parquet file on disk (compressed)              |
| max_row_groups_size_compressed   | The size of the largest row group (compressed)                 |
| max_row_groups_size_uncompressed | The size of the largest row group (uncompressed)               |

## Syntax

```sql
INSPECT_PARQUET('@<path-to-file>')
```

## Examples

This example retrieves the metadata from a staged sample Parquet file named [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet). The file contains two records:

```text title='books.parquet'
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```

```sql
-- Show the staged file
LIST @my_internal_stage;

┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│      name     │  size  │        md5       │         last_modified         │      creator     │
├───────────────┼────────┼──────────────────┼───────────────────────────────┼──────────────────┤
│ books.parquet │    998 │ NULL             │ 2023-04-19 19:34:51.303 +0000 │ NULL             │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

-- Retrieve metadata from the staged file
SELECT * FROM INSPECT_PARQUET('@my_internal_stage/books.parquet');

┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│             created_by             │ num_columns │ num_rows │ num_row_groups │ serialized_size │ max_row_groups_size_compressed │ max_row_groups_size_uncompressed │
├────────────────────────────────────┼─────────────┼──────────┼────────────────┼─────────────────┼────────────────────────────────┼──────────────────────────────────┤
│ parquet-cpp version 1.5.1-SNAPSHOT │           3 │        2 │              1 │             998 │                            332 │                              320 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```