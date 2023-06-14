---
title: ARRAY_AGG
title_includes: LIST
---

The ARRAY_AGG function (also known by its alias LIST) transforms all the values, including NULL, of a specific column in a query result into an array.

## Syntax

```sql
ARRAY_AGG(<expr>)

LIST(<expr>)
```

## Arguments

| Arguments | Description    |
|-----------| -------------- |
| `<expr>`  | Any expression |

## Return Type

Returns an [Array](../../13-sql-reference/10-data-types/40-data-type-array-types.md) with elements that are of the same type as the original data.

## Examples

This example demonstrates how the ARRAY_AGG function can be used to aggregate and present data in a convenient array format:

```sql
-- Create a table and insert sample data
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

-- List all ratings for Inception in an array
SELECT movie_title, ARRAY_AGG(rating) AS ratings
FROM movie_ratings
WHERE movie_title = 'Inception'
GROUP BY movie_title;

| movie_title |  ratings   |
|-------------|------------|
| Inception   | [5, 4, 5]  |
```