---
title: CUME_DIST
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.7"/>

Returns the cumulative distribution of a given value in a set of values. It calculates the proportion of rows that have values less than or equal to the specified value, divided by the total number of rows. Please note that the resulting value falls between 0 and 1, inclusive.

See also: [PERCENT_RANK](percent_rank.md)

## Syntax

```sql
CUME_DIST() OVER (
	PARTITION BY expr, ...
	ORDER BY expr [ASC | DESC], ...
)
```

## Examples

This example retrieves the students' names, scores, grades, and the cumulative distribution values (cume_dist_val) within each grade using the CUME_DIST() window function.

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
    CUME_DIST() OVER (PARTITION BY grade ORDER BY score) AS cume_dist_val
FROM
    students;

name    |score|grade|cume_dist_val|
--------+-----+-----+-------------+
Smith   |   81|A    |         0.25|
Davies  |   84|A    |          0.5|
Evans   |   87|A    |         0.75|
Johnson |  100|A    |          1.0|
Taylor  |   62|B    |          0.5|
Brown   |   62|B    |          0.5|
Wilson  |   72|B    |          1.0|
Thomas  |   72|B    |          1.0|
Jones   |   55|C    |          1.0|
Williams|   55|C    |          1.0|
```