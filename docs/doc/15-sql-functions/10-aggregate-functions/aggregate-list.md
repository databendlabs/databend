---
title: LIST
---

Aggregate function.

The LIST() function converts all the values of a column to an Array.

## Syntax

```sql
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
SELECT movie_title, LIST(rating) AS ratings
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