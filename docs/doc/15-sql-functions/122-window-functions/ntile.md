---
title: NTILE
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.50"/>

Divides the sorted result set into a specified number of buckets or groups. It evenly distributes the sorted rows into these buckets and assigns a bucket number to each row. The NTILE function is typically used with the ORDER BY clause to sort the results. 

Please note that the NTILE function evenly distributes the rows into buckets based on the sorting order of the rows and ensures that the number of rows in each bucket is as equal as possible. If the number of rows cannot be evenly distributed into the buckets, some buckets may have one extra row compared to the others.

## Syntax

```sql
NTILE(n) OVER (
	PARTITION BY expr, ...
	ORDER BY expr [ASC | DESC], ...
)
```

## Examples

This example retrieves the students' names, scores, grades, and assigns them to buckets based on their scores within each grade using the NTILE() window function.

```sql
CREATE TABLE students (
    name VARCHAR(20),
    score INT NOT NULL,
    grade CHAR(1) NOT NULL
);

INSERT INTO students (name, score, grade)
VALUES
    ('Smith', 81, 'A'),
    ('Jones', 55, 'C'),
    ('Williams', 55, 'C'),
    ('Taylor', 62, 'B'),
    ('Brown', 62, 'B'),
    ('Davies', 84, 'A'),
    ('Evans', 87, 'A'),
    ('Wilson', 72, 'B'),
    ('Thomas', 72, 'B'),
    ('Johnson', 100, 'A');

SELECT
    name,
    score,
    grade,
    ntile(3) OVER (PARTITION BY grade ORDER BY score DESC) AS bucket
FROM
    students;

name    |score|grade|bucket|
--------+-----+-----+------+
Johnson |  100|A    |     1|
Evans   |   87|A    |     1|
Davies  |   84|A    |     2|
Smith   |   81|A    |     3|
Wilson  |   72|B    |     1|
Thomas  |   72|B    |     1|
Taylor  |   62|B    |     2|
Brown   |   62|B    |     3|
Jones   |   55|C    |     1|
Williams|   55|C    |     2|
```