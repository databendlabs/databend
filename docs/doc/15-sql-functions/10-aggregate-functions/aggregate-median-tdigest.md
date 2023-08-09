---
title: MEDIAN_TDIGEST
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.41"/>

Computes the median of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm.

:::note
NULL values are not included in the calculation.
:::

## Syntax

```sql
MEDIAN_TDIGEST(<expr>)
```

## Arguments

| Arguments | Description              |
|-----------|--------------------------|                                                                                                                 
| `<expr>`  | Any numerical expression |                                                                                                     

## Return Type

Returns a value of the same data type as the input values.

## Examples

```sql
-- Create a table and insert sample data
CREATE TABLE exam_scores (
  id INT,
  student_id INT,
  score INT
);

INSERT INTO exam_scores (id, student_id, score)
VALUES (1, 1, 80),
       (2, 2, 90),
       (3, 3, 75),
       (4, 4, 95),
       (5, 5, 85);

-- Calculate median exam score
SELECT MEDIAN_TDIGEST(score) AS median_score
FROM exam_scores;

|  median_score  |
|----------------|
|      85.0      |
```