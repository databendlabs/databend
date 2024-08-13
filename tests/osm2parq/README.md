# OpenStreetMap (OSM) dataset for benchmark

The OSM dataset is a real geographic dataset that more accurately reflects the true performance of geospatial features.

## Download dataset

You can download the `osm.pbf` file from [here](https://download.geofabrik.de/).

## How to read OpenStreetMap data

And here's an [article](https://towardsdatascience.com/how-to-read-osm-data-with-duckdb-ffeb15197390) explaining how to read `osm.pbf`

## Using `quackosm` to Constructing geometries

Using `quackosm` we can quickly generate directly usable geometries data.

https://github.com/kraina-ai/quackosm

```sql
create table osm
(
    feature_id string not null,
    tags map(string, string not null),
    geometry geometry
);
```

## Using `Databend` to Constructing geometries

Alternatively, we can manually construct geometries with `Databend`, which will verify that the corresponding features is working.

First, use `osm2parq` to convert a `osm.pbf` file to parquet format.

```sql
CREATE TABLE osm_raw (
  kind INT8 NOT NULL,
  id INT64 NOT NULL,
  tags Map(STRING, STRING NOT NULL),
  nano_lon INT64,
  nano_lat INT64,
  refs Array(INT64 NOT NULL),
  ref_types Array(INT8 NOT NULL),
  ref_roles Array(INT32 NOT NULL)
) CLUSTER BY (kind, id)
```

```sql
CREATE TABLE matching_ways AS
SELECT
  id,
  tags
FROM
  osm_raw
WHERE
  kind = 1
  AND tags IS NOT NULL
  AND map_size(tags) > 0
```

```sql
CREATE TABLE matching_ways_with_nodes_refs AS
SELECT
  id,
  UNNEST(refs) AS ref,
  UNNEST(RANGE(0, length(refs))) AS ref_idx
FROM
  osm_raw SEMI
  JOIN matching_ways USING (id)
WHERE
  kind = 1
```

```sql
CREATE TABLE required_nodes_with_geometries AS
SELECT
  id,
  st_geom_point(nano_lon * 1e-9, nano_lat * 1e-9) geometry
FROM
  osm_raw nodes SEMI
  JOIN matching_ways_with_nodes_refs ON nodes.id = matching_ways_with_nodes_refs.ref
WHERE
  kind = 0
```

Fail, fix me
```sql
CREATE TEMP TABLE matching_ways_linestrings AS
SELECT
    matching_ways.id,
    matching_ways.tags,
    ST_MakeLine(list(nodes.geometry ORDER BY ref_idx ASC)) linestring
FROM matching_ways
JOIN matching_ways_with_nodes_refs
ON matching_ways.id = matching_ways_with_nodes_refs.id
JOIN required_nodes_with_geometries nodes
ON matching_ways_with_nodes_refs.ref = nodes.id
GROUP BY 1, 2;
```
no supported: list(nodes.geometry ORDER BY ref_idx ASC) 
no supported: st_make_geom_Line(list(...)) 
no supported: array_reduce(list(...), (x,y) -> st_make_geom_Line(x,y))
