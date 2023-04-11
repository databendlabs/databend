---
title: Window Function Syntax
---

## Window Function Syntax
```
{ Aggregate | Rank | Value } functions OVER ( window_specification ) 
```

```sql
window_specification:
    [ window_name ]
    [ PARTITION BY expression [, ...] ]
    [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
    [ frame_clause ]
```

```sql
frame_clause:
    { RANGE | ROWS } frame_start
    { RANGE | ROWS } BETWEEN frame_start AND frame_end

frame_start, frame_end:
    UNBOUNDED PRECEDING
    | <expression> PRECEDING
    | CURRENT ROW
    | UNBOUNDED FOLLOWING
    | <expression> FOLLOWING
```


## The window can be specified in two ways:
* By a reference to a named window specification defined in the `WINDOW` clause.
* By an in-line window specification.

### Named Window Specification

> Note: Named window can be inherited by other windows.

```sql
SELECT col1, rank() OVER w1, row_number() OVER w2, sum(col1) OVER w3  
FROM table_name 
WINDOW w1 AS (PARTITION BY col1),
       w2 AS (w1 ORDER BY col2 DESC),
       w3 AS (w2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) 
ORDER BY count() OVER w1 DESC;
```

### In-line Window Specification
```sql
SELECT col1, rank() OVER (PARTITION BY col1 ORDER BY col2 DESC) 
FROM table_name
```