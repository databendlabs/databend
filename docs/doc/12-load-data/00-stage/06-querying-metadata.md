---
title: Retrieving Metadata
---

### Why and What is Metadata?

Databend allows you to retrieve metadata from your data files using the [INFER_SCHEMA](../../15-sql-functions/112-table-functions/infer_schema.md) function. This means you can extract column definitions from data files stored in internal or external stages. Retrieving metadata through the INFER_SCHEMA function provides a better understanding of the data structure, ensures data consistency, and enables automated data integration and analysis. The metadata for each column includes the following information:

- **column_name**: Indicates the name of the column.
- **type**: Indicates the data type of the column.
- **nullable**: Indicates whether the column allows null values.
- **order_id**: Represents the column's position in the table.

:::note
This feature is currently only available for the Parquet file format.
:::

The syntax for INFER_SCHEMA is as follows. For more detailed information about this function, see [INFER_SCHEMA](../../15-sql-functions/112-table-functions/infer_schema.md).

```sql
INFER_SCHEMA(
  LOCATION => '{ internalStage | externalStage }'
  [ PATTERN => '<regex_pattern>']
)
```

### Tutorial: Querying Column Definitions

In this tutorial, we will guide you through the process of uploading the sample file to an internal stage, querying the column definitions, and finally creating a table based on the staged file. Before you start, download and save the sample file [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet) to a local folder.

1. Create an internal stage named *my_internal_stage*:

```sql
CREATE STAGE my_internal_stage;
```

2. Use cURL to make a request to the File Upload API:

```shell title='Put books.parquet to stage'
curl -u root: -H "stage_name:my_internal_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

3. Query the column definitions from the staged sample file:

```sql
SELECT * FROM INFER_SCHEMA(location => '@my_internal_stage/books.parquet');

---
column_name|type   |nullable|order_id|
-----------+-------+--------+--------+
title      |VARCHAR|       0|       0|
author     |VARCHAR|       0|       1|
date       |VARCHAR|       0|       2|
```

4. Create a table named *mybooks* based on the staged sample file:

```sql
CREATE TABLE mybooks AS SELECT * FROM @my_internal_stage/books.parquet;
```

Check the created table:

```sql
DESC mybooks;

---
Field |Type   |Null|Default|Extra|
------+-------+----+-------+-----+
title |VARCHAR|NO  |''     |     |
author|VARCHAR|NO  |''     |     |
date  |VARCHAR|NO  |''     |     |

SELECT * FROM mybooks;

---
title                       |author             |date|
----------------------------+-------------------+----+
Transaction Processing      |Jim Gray           |1992|
Readings in Database Systems|Michael Stonebraker|2004|
```