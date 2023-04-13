---
title: MAX
---

Aggregate function.

The MAX() function returns the maximum value in a set of values.

## Syntax

```
MAX(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The maximum value, in the type of the value.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE temperatures (
  id INT,
  city VARCHAR,
  temperature FLOAT
);

INSERT INTO temperatures (id, city, temperature)
VALUES (1, 'New York', 30),
       (2, 'New York', 28),
       (3, 'New York', 32),
       (4, 'Los Angeles', 25),
       (5, 'Los Angeles', 27);
```

**Query Demo: Find Maximum Temperature for New York City**

```sql
SELECT city, MAX(temperature) AS max_temperature
FROM temperatures
WHERE city = 'New York'
GROUP BY city;
```

**Result**
```sql
|    city    | max_temperature |
|------------|-----------------|
| New York   |       32        |
```