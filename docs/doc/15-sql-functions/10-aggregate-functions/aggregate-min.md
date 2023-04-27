---
title: MIN
---

Aggregate function.

The MIN() function returns the minimum value in a set of values.

## Syntax

```
MIN(<expr>)
```

## Arguments

| Arguments | Description    |
|-----------|----------------|
| `<expr>`  | Any expression |

## Return Type

The minimum value, in the type of the value.

## Example

---
title: MIN
---

Aggregate function.

The MIN() function returns the minimum value in a set of values.

## Syntax

```
MIN(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The minimum value, in the type of the value.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE gas_prices (
  id INT,
  station_id INT,
  price FLOAT
);

INSERT INTO gas_prices (id, station_id, price)
VALUES (1, 1, 3.50),
       (2, 1, 3.45),
       (3, 1, 3.55),
       (4, 2, 3.40),
       (5, 2, 3.35);
```

**Query Demo: Find Minimum Gas Price for Station 1**
```sql
SELECT station_id, MIN(price) AS min_price
FROM gas_prices
WHERE station_id = 1
GROUP BY station_id;
```

**Result**
```sql
| station_id | min_price |
|------------|-----------|
|     1      |   3.45    |
```