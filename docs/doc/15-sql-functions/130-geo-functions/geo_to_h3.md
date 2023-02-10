---
title: GEO_TO_H3
---

Returns the [H3](https://eng.uber.com/h3/) index of the hexagon cell where the given location resides.

## Syntax

```sql
GEO_TO_H3(lon, lat, res)
```

## Arguments

| Argument   | Type    | Description                                                                                               |
|------------|---------|-----------------------------------------------------------------------------------------------------------|
| lon        | Float64 | Specifies the location's longitude, for example, `37.79506683`.                                           |
| lat        | Float64 | Specifies the location's latitude, for example, `55.71290588`.                                            |
| res | UInt8   | Sets an [H3 resolution](https://h3geo.org/docs/core-library/restable) ranging from 0 to 15.|

## Return Type

UInt64.

:::note
Returning 0 means an error occurred.
:::

## Examples

```sql
SELECT GEO_TO_H3(37.79506683, 55.71290588, 15) AS h3Index;
+-------------------------------+
|        h3Index                |
+-------------------------------+
| 644325524701193974            |
+-------------------------------+
```