---
title: streaming load data into table
---

Load data into a table using HTTP streaming API.

## HTTP Streaming Load
### Syntax

```
curl -u user:passwd -H <options>  -F  "upload=@<files_location>"  -XPUT http://localhost:8001/v1/streaming_load

```
### Parameters

  * `options`: key value options, supported options: `insert_sql`, `field_delimitor`, `record_delimitor`, `csv_header`
  * `insert_sql`: must be specified in options, eg: `insert into table_name (a,b,c) format CSV`
  * `files_location`: local file path, eg: `/tmp/data.csv`

:::note Notes
Currently, only csv format is supported for streaming load.
:::

### Response
```
{
	"id": "af101056-116a-4c3a-b42b-0c56ade3885d",  // unique id for this load
	"state": "SUCCESS",   // load state
	"stats": {   // progress stats
		"read_rows": 371357,
		"read_bytes": 271086978,
		"total_rows_to_read": 0
	},
	"error": null  // error msg
}

```

### Examples

#### Load from csv files

Example:
```shell
curl -H "insert_sql:insert into ontime format CSV" -H "csv_header:1" -F  "upload=@/tmp/ontime.csv"  -XPUT http://localhost:8001/v1/streaming_load

{"id":"af101056-116a-4c3a-b42b-0c56ade3885d","state":"SUCCESS","stats":{"read_rows":371357,"read_bytes":271086978,"total_rows_to_read":0},"error":null}%

```

```sql
:) select count(1) ,avg(Year), sum(DayOfWeek)  from ontime;
+----------+-----------+----------------+
| count(1) | avg(Year) | sum(DayOfWeek) |
+----------+-----------+----------------+
|   371357 |      2020 |        1457181 |
+----------+-----------+----------------+
1 row in set (0.06 sec)
```
