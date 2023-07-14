---
title: Bitmap
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.45"/>

Bitmap in Databend is an efficient data structure used to represent the presence or absence of elements or attributes in a collection. It has wide applications in data analysis and querying, providing fast set operations and aggregation capabilities.

:::tip Why Bitmap?

- Distinct Count: Bitmaps are used for efficient calculation of the number of unique elements in a set. By performing bitwise operations on bitmaps, it is possible to quickly determine the existence of elements and achieve distinct count functionality.

- Filtering and Selection: Bitmaps are effective for fast data filtering and selection. By performing bitwise operations on bitmaps, it becomes efficient to identify elements that satisfy specific conditions, enabling efficient data filtering and selection.

- Set Operations: Bitmaps can be used for various set operations such as union, intersection, difference, and symmetric difference. These set operations can be achieved through bitwise operations, providing efficient set operations in data processing and analysis.

- Compressed Storage: Bitmaps offer high compression performance in terms of storage. Compared to traditional storage methods, bitmaps can effectively utilize storage space, saving storage costs and improving query performance.
:::

Databend enables the creation of bitmaps using two formats with the TO_BITMAP function:

- String format: You can create a bitmap using a string of comma-separated values. For example, TO_BITMAP('1,2,3') creates a bitmap with bits set for values 1, 2, and 3.

- uint64 format: You can also create a bitmap using a uint64 value. For example, TO_BITMAP(123) creates a bitmap with bits set according to the binary representation of the uint64 value 123.

In Databend, a bitmap can store a maximum of 2^64 bits. The bitmap data type in Databend is a binary type that differs from other supported types in terms of its representation and display in SELECT statements. Unlike other types, bitmaps cannot be directly shown in the result set of a SELECT statement. Instead, they require the use of [Bitmap Functions](../../15-sql-functions/05-bitmap-functions/index.md) for manipulation and interpretation:

```sql
SELECT TO_BITMAP('1,2,3')

+---------------------+
|  to_bitmap('1,2,3') |
+---------------------+
|  <bitmap binary>    |
+---------------------+

SELECT TO_STRING(TO_BITMAP('1,2,3'))

+-------------------------------+
| to_string(to_bitmap('1,2,3')) |
--------------------------------+
|            1,2,3              |
+-------------------------------+
```

**Example**:

This example illustrates how bitmaps in Databend enable efficient storage and querying of data with a large number of possible values, such as user visit history.

```sql
-- Create table user_visits with user_id and page_visits columns, using build_bitmap for representing page_visits.
CREATE TABLE user_visits (
  user_id INT,
  page_visits Bitmap
)

-- Insert user visits for 3 users, calculate total visits using bitmap_count.
INSERT INTO user_visits (user_id, page_visits)
VALUES
  (1, build_bitmap([2, 5, 8, 10])),
  (2, build_bitmap([3, 7, 9])),
  (3, build_bitmap([1, 4, 6, 10]))

-- Query the table
SELECT user_id, bitmap_count(page_visits) AS total_visits
FROM user_visits

+--------+------------+
|user_id |total_visits|
+--------+------------+
|       1|           4|
|       2|           3|
|       3|           4|
+--------+------------+
```