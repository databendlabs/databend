---
title: RETENTION
---

Aggregate function

The RETENTION() function takes as arguments a set of conditions from 1 to 32 arguments of type UInt8 that indicate whether a certain condition was met for the event.

Any condition can be specified as an argument (as in WHERE).

The conditions, except the first, apply in pairs: the result of the second will be true if the first and second are true, of the third if the first and third are true, etc.

## Syntax

```sql
RETENTION( <cond1> , <cond2> , ..., <cond32> );
```

## Arguments

| Arguments | Description                                 |
|-----------|---------------------------------------------|
| `<cond>`  | An expression that returns a Boolean result |

## Return Type

The array of 1 or 0.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE user_events (
  id INT,
  user_id INT,
  event_date DATE,
  event_type VARCHAR
);

INSERT INTO user_events (id, user_id, event_date, event_type)
VALUES (1, 1, '2022-01-01', 'signup'),
       (2, 1, '2022-01-02', 'login'),
       (3, 2, '2022-01-01', 'signup'),
       (4, 2, '2022-01-03', 'purchase'),
       (5, 3, '2022-01-01', 'signup'),
       (6, 3, '2022-01-02', 'login');
```

**Query Demo: Calculate User Retention Based on Signup, Login, and Purchase Events**
```sql
SELECT
  user_id,
  RETENTION(event_type = 'signup', event_type = 'login', event_type = 'purchase') AS retention
FROM user_events
GROUP BY user_id;
```

**Result**
```sql
| user_id | retention |
|---------|-----------|
|   1     | [1, 1, 0] |
|   2     | [1, 0, 1] |
|   3     | [1, 1, 0] |
```