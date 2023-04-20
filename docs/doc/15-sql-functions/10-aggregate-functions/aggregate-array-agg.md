---
title: ARRAY_AGG
title_includes: LIST
---

Aggregate function.

The `ARRAY_AGG()` function converts all the values of a column to an Array.

:::tip
The `LIST` function is alias to `ARRAY_AGG`.
:::

## Syntax

```sql
ARRAY_AGG(expression)
LIST(expression)
```

## Arguments

| Arguments   | Description    |
| ----------- | -------------- |
| expression  | Any expression |

## Return Type

the Array type that use the type of the value as inner type.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE movie_ratings (
  id INT,
  movie_title VARCHAR,
  user_id INT,
  rating INT
);

INSERT INTO movie_ratings (id, movie_title, user_id, rating)
VALUES (1, 'Inception', 1, 5),
       (2, 'Inception', 2, 4),
       (3, 'Inception', 3, 5),
       (4, 'Interstellar', 1, 4),
       (5, 'Interstellar', 2, 3);
```

**Query Demo: List All Ratings for Inception Movie**

```sql
SELECT movie_title, ARRAY_AGG(rating) AS ratings
FROM movie_ratings
WHERE movie_title = 'Inception'
GROUP BY movie_title;
```

**Result**
```sql
| movie_title |  ratings   |
|-------------|------------|
| Inception   | [5, 4, 5]  |
```
