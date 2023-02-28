---
title: 'Geography Functions'
---

| Function                                                | Description                                                                                                                   | Example                                                          | Result                          |
|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------------------------|
| **GEO_TO_H3(lon, lat, res)**                            | Returns the [H3](https://eng.uber.com/h3/) index of the hexagon cell where the given location resides.                        | **GEO_TO_H3(37.79506683, 55.71290588, 15)**                      | 644325524701193974              |
| **GEOHASH_DECODE('<geohashed-string\>')**               | Converts a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string into latitude/longitude coordinates.               | **GEOHASH_DECODE('ezs42')**                                      | (-5.60302734375,42.60498046875) |
| **GEOHASH_ENCODE(lon, lat)**                            | Converts a pair of latitude and longitude coordinates into a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string. | **GEOHASH_ENCODE(-5.60302734375, 42.593994140625)**              | ezs42d000000                    |
| **POINT_IN_POLYGON((x,y), [(a,b), (c,d), (e,f) ... ])** | Calculates whether a given point falls within the polygon formed by joining multiple points.                                  | **POINT_IN_POLYGON((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)])** | 1                               |

:::note

- `GEO_TO_H3(lon, lat, res)` returning 0 means an error occurred.
- `POINT_IN_POLYGON((x,y), [(a,b), (c,d), (e,f) ... ])` A polygon is a closed shape connected by coordinate pairs in the order they appear. Changing the order of coordinate pairs can result in a different shape.

:::
