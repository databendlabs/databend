---
title: SKEWNESS
---

Aggregate function.

The `SKEWNESS()` function returns the skewness of all input values.

## Syntax

```sql
SKEWNESS(expression)
```

## Arguments

| Arguments   | Description                     |
| ----------- | -----------                     |
| expression  | Any numerical expression        |

## Return Type

Nullable Float64.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE temperature_data (
                                  id INT,
                                  city_id INT,
                                  temperature FLOAT
);

INSERT INTO temperature_data (id, city_id, temperature)
VALUES (1, 1, 60),
       (2, 1, 65),
       (3, 1, 62),
       (4, 2, 70),
       (5, 2, 75);
```

**Query Demo: Calculate Skewness of Temperature Data**

```sql
SELECT SKEWNESS(temperature) AS temperature_skewness
FROM temperature_data;
```

**Result**
```sql
| temperature_skewness |
|----------------------|
|      0.68            |
```



