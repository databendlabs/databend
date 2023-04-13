---
title: MEDIAN
---

Aggregate function.

The MEDIAN() function computes the median of a numeric data sequence.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
MEDIAN(expression)
```

## Arguments

| Arguments   | Description|
| ----------- | ----------- |                                                                                                                 
| expression  | Any numerical expression|                                                                                                     

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
SELECT MEDIAN(score) AS median_score
FROM exam_scores;
```

**Result**
```sql
|  median_score  |
|----------------|
|      85.0      |
```