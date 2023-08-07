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
| **H3_TO_GEO(h3)**                                       | Returns the longitude and latitude corresponding to the given [H3](https://eng.uber.com/h3/) index.                             | **H3_TO_GEO(644325524701193974)** | (37.79506616830255,55.712902431456676)                               |
| **H3_TO_GEO_BOUNDARY(h3)**                              | Returns an array containing the longitude and latitude coordinates of the vertices of the hexagon corresponding to the [H3](https://eng.uber.com/h3/) index.        | **H3_TO_GEO_BOUNDARY(644325524701193974)**              | [(37.79505811173477,55.712900225355526),(37.79506506997187,55.71289713485416),(37.795073126539855,55.71289934095484),(37.795074224871684,55.71290463755745),(37.79506726663349,55.71290772805916),(37.79505921006456,55.712905521957914)] |
| **H3_K_RING(h3, k)**                                    | Returns an array containing the [H3](https://eng.uber.com/h3/) indexes of the k-ring hexagons surrounding the input H3 index. Each element in this array is an H3 index. | **H3_K_RING(644325524701193974, 1)**             | [644325524701193897,644325524701193899,644325524701193869,644325524701193970,644325524701193968,644325524701193972]                    |
| **H3_IS_VALID(h3)**                                     | Checks if the given [H3](https://eng.uber.com/h3/) index is valid.  | **H3_IS_VALID(644325524701193974)**             | 1                    |
| **H3_GET_RESOLUTION(h3)**                               | Returns the resolution of the given [H3](https://eng.uber.com/h3/) index.  | **H3_GET_RESOLUTION(644325524701193974)**             | 15                    |
| **H3_EDGE_LENGTH_M(res)**                               | Returns the average hexagon edge length in meters at the given resolution. Excludes pentagons.  | **H3_EDGE_LENGTH_M(1)**             | 418676.0055         |
| **H3_EDGE_LENGTH_KM(res)**                              | Returns the average hexagon edge length in  kilometers at the given resolution. Excludes pentagons.   | **H3_EDGE_LENGTH_KM(1)**     | 418.6760055  |
| **H3_GET_BASE_CELL(h3)**                                | Returns the base cell number of the given [H3](https://eng.uber.com/h3/) index.   | **H3_GET_BASE_CELL(644325524701193974)**     |  8  |
| **H3_HEX_AREA_M2(res)**                                 | Returns the average hexagon area in square meters at the given resolution. Excludes pentagons.   | **H3_HEX_AREA_M2(1)**     |  6.097884417941339e11  |
| **H3_HEX_AREA_KM2(res)**                                | Returns the average hexagon area in square kilometers at the given resolution. Excludes pentagons.   | **H3_HEX_AREA_KM2(1)**     |  609788.4417941332  |
| **H3_INDEXES_ARE_NEIGHBORS(h3, a_h3)**                  | Returns whether or not the provided [H3](https://eng.uber.com/h3/) indexes are neighbors.   | **H3_INDEXES_ARE_NEIGHBORS(644325524701193974, 644325524701193897)**     |  1  |
| **H3_TO_CHILDREN(h3, child_res)**                       | Returns the indexes contained by `h3` at resolution `child_res`.      | **H3_TO_CHILDREN(635318325446452991, 14)**     |  [639821925073823431,639821925073823439,639821925073823447,639821925073823455,639821925073823463,639821925073823471,639821925073823479]  |
| **H3_TO_PARENT(h3, parent_res)**                        | Returns the parent index containing the `h3` at resolution `parent_res`.      | **H3_TO_PARENT(635318325446452991, 12)**     |  630814725819082751  |
| **H3_TO_STRING(h3)**                                    | Converts the representation of the given [H3](https://eng.uber.com/h3/) index to the string representation.        | **H3_TO_STRING(635318325446452991)**     | 8d11aa6a38826ff   |
| **STRING_TO_H3(h3)**                                    | Converts the string representation to [H3](https://eng.uber.com/h3/) (uint64) representation.        | **STRING_TO_H3('8d11aa6a38826ff')**     | 635318325446452991      |
| **H3_IS_RES_CLASS_III(h3)**                             | Checks if the given [H3](https://eng.uber.com/h3/) index has a resolution with Class III orientation.           | **H3_IS_RES_CLASS_III(635318325446452991)**     | 1              |
| **H3_IS_PENTAGON(h3)**                                  | Checks if the given [H3](https://eng.uber.com/h3/) index represents a pentagonal cell.                  | **H3_IS_PENTAGON(599119489002373119)**     | 1           |
| **H3_GET_FACES(h3)**                                    | Finds all icosahedron faces intersected by the given [H3](https://eng.uber.com/h3/) index. Faces are represented as integers from 0-19.                                  | **H3_GET_FACES(599119489002373119)**     | [0,1,2,3,4]          |
| **H3_CELL_AREA_M2(h3)**                                 | Returns the exact area of specific cell in square meters.                         | **H3_CELL_AREA_M2(599119489002373119)**     | 127785582.60810876   |
| **H3_CELL_AREA_RADS2(h3)**                              | Returns the exact area of specific cell in square radians.                    | **H3_CELL_AREA_RADS2(599119489002373119)**     | 3.1482243104279148e-6              |

:::note

- `GEO_TO_H3(lon, lat, res)`, `H3_TO_PARENT(h3, parent_res)` returning 0 means an error occurred.
- `POINT_IN_POLYGON((x,y), [(a,b), (c,d), (e,f) ... ])` A polygon is a closed shape connected by coordinate pairs in the order they appear. Changing the order of coordinate pairs can result in a different shape.

:::
