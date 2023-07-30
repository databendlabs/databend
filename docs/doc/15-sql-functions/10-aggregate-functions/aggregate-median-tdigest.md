---
title: MEDIAN_TDIGEST
---

Aggregate function.

The MEDIAN_TDIGEST() function computes the median of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm.

:::caution
NULL values are not counted.
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

the type of the value.

## Example

**Create a Table and Insert Sample Data**
```sql
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
```

**Query Demo: Calculate Median Exam Score**
```sql
SELECT MEDIAN_TDIGEST(score) AS median_score
FROM exam_scores;
```

**Result**
```sql
|  median_score  |
|----------------|
|      85.0      |
```