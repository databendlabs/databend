---
title: GEOHASH_DECODE
---

Converts a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string into latitude/longitude coordinates.

See also: [GEOHASH_ENCODE](geohash_encode.md)

## Syntax

```sql
GEOHASH_DECODE('<geohashed-string>')
```

## Return Type

Returns a latitude(Float64) and longitude(Float64) pair. 

## Examples

```sql
SELECT GEOHASH_DECODE('ezs42')

----
(-5.60302734375,42.60498046875)
```