---
title: 'Geography Functions'
---

Geography Functions in SQL.

| Function                                                | Description                                                                                                                   | Example                                                          | Result                          |
|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------------------------|
| **GEO_TO_H3(lon, lat, res)**                            | Returns the [H3](https://eng.uber.com/h3/) index of the hexagon cell where the given location resides.                        | **GEO_TO_H3(37.79506683, 55.71290588, 15)**                      | 644325524701193974              |
| **GEOHASH_DECODE('<geohashed-string\>')**               | Converts a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string into latitude/longitude coordinates.               | **GEOHASH_DECODE('ezs42')**                                      | (-5.60302734375,42.60498046875) |
| **GEOHASH_ENCODE(lon, lat)**                            | Converts a pair of latitude and longitude coordinates into a [Geohash](https://en.wikipedia.org/wiki/Geohash)-encoded string. | **GEOHASH_ENCODE(-5.60302734375, 42.593994140625)**              | ezs42d000000                    |
| **POINT_IN_POLYGON((x,y), [(a,b), (c,d), (e,f) ... ])** | Calculates whether a given point falls within the polygon formed by joining multiple points.                                  | **POINT_IN_POLYGON((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)])** | 1                               |
| **H3_TO_GEO(h3)**                                       | Return the longitude and latitude corresponding to the given [H3](https://eng.uber.com/h3/) index.                             | **H3_TO_GEO(644325524701193974)** | (37.79506616830255,55.712902431456676)                               |
| **H3_TO_GEO_BOUNDARY(h3)**                              | Return an array containing the longitude and latitude coordinates of the vertices of the hexagon corresponding to the [H3](https://eng.uber.com/h3/) index.        | **H3_TO_GEO_BOUNDARY(644325524701193974)**              | [(37.79505811173477,55.712900225355526),(37.79506506997187,55.71289713485416),(37.795073126539855,55.71289934095484),(37.795074224871684,55.71290463755745),(37.79506726663349,55.71290772805916),(37.79505921006456,55.712905521957914)] |
| **H3_K_RING(h3, k)**                                    | Return an array containing the [H3](https://eng.uber.com/h3/) indexes of the k-ring hexagons surrounding the input H3 index. Each element in this array is an H3 index. | **H3_K_RING(644325524701193974, 1)**             | [644325524701193897,644325524701193899,644325524701193869,644325524701193970,644325524701193968,644325524701193972]                    |

:::note

- `GEO_TO_H3(lon, lat, res)` returning 0 means an error occurred.
- `POINT_IN_POLYGON((x,y), [(a,b), (c,d), (e,f) ... ])` A polygon is a closed shape connected by coordinate pairs in the order they appear. Changing the order of coordinate pairs can result in a different shape.

:::
