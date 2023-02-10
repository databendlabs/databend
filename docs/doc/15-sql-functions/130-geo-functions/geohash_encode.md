---
title: GEOHASH_ENCODE
---

Converts a pair of latitude and longitude coordinates into a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string.

See also: [GEOHASH_DECODE](geohash_decode.md)

## Syntax

```sql
GEOHASH_ENCODE(lon, lat)
```
## Arguments

| Argument   | Type    | Description                                                                                               |
|------------|---------|-----------------------------------------------------------------------------------------------------------|
| lon        | Float64 | Specifies the location's longitude, for example, `37.79506683`.                                           |
| lat        | Float64 | Specifies the location's latitude, for example, `55.71290588`.                                            |

## Return Type

Returns a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string.

## Examples

```sql
SELECT GEOHASH_ENCODE(-5.60302734375, 42.593994140625)

----
ezs42d000000
```