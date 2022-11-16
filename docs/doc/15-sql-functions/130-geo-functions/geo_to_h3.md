---
title: GeoToH3
---

Returns [H3](https://eng.uber.com/h3/) point index **(lon, lat)** with specified resolution.

## Syntax

```sql
geo_to_h3(lon, lat, resolution)
```

## Arguments

| Arguments    | Description |
|--------------| ----------- |
| `lon`        | Longitude. Type: Float64
| `lon`        | Latitude. Type: Float64
| `resolution` | Index resolution. Range: `[0, 15]`. Type: UInt8

## Return Type

* Hexagon index number
* 0 in case of error.

Type: UInt64

## Examples

```sql
SELECT geo_to_h3(37.79506683, 55.71290588, 15) AS h3Index;
+-------------------------------+
|        h3Index                |
+-------------------------------+
| 644325524701193974            |
+-------------------------------+
```