---
title: POINT_IN_POLYGON
---

Calculates whether a given point falls within the polygon formed by joining multiple points.

## Syntax

```sql
POINT_IN_POLYGON((x,y), [(a,b), (c,d), (e,f) ... ])
```

## Arguments

| Argument                | Type              | Description                                                         |
|-------------------------|-------------------|---------------------------------------------------------------------|
| (x,y)                   | (Float64,Float64) | Coordinates of the given point.                                     |
| (a,b), (c,d), (e,f) ... | VARIANT           | An ordered list of coordinate pairs defining the shape of a polygon.|

:::note
A polygon is a closed shape connected by coordinate pairs in the order they appear. Changing the order of coordinate pairs can result in a different shape.
:::

## Return Type

Returns 1 if the given point falls within the formed polygon; otherwise, returns 0.

## Examples

```sql
SELECT POINT_IN_POLYGON((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)])

----
1
```