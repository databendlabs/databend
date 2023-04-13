---
title: APPROX_COUNT_DISTINCT
---

Estimates the number of distinct values in a data set with the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm. 

The HyperLogLog algorithm provides an approximation of the number of unique elements using little memory and time. Consider using this function when dealing with large data sets where an estimated result can be accepted. In exchange for some accuracy, this is a fast and efficient method of returning distinct counts.

To get an accurate result, use [COUNT_DISTINCT](aggregate-count-distinct.md). See [Examples](#examples) for more explanations.

## Syntax

```sql
APPROX_COUNT_DISTINCT(<expr>)
```

## Return Type

Integer.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE user_events (
  id INT,
  user_id INT,
  event_name VARCHAR
);

INSERT INTO user_events (id, user_id, event_name)
VALUES (1, 1, 'Login'),
       (2, 2, 'Login'),
       (3, 3, 'Login'),
       (4, 1, 'Logout'),
       (5, 2, 'Logout'),
       (6, 4, 'Login'),
       (7, 1, 'Login');
```

**Query Demo: Estimate the Number of Distinct User IDs**
```sql
SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_distinct_user_count
FROM user_events;
```

**Result**
```sql
| approx_distinct_user_count |
|----------------------------|
|             4              |
```
