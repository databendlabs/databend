---
title: HTTP Handler
---

## async endpoint: /v1/query

This handler return results in "pages" without waiting for the query to finish.

usage:

1. A POST to /v1/query with JSON of type `QueryRequest` in the POST body, and returns a JSON of type `QueryResults`.
2. use fields of QueryRequest for further processing:
    1. A GET to the `next_uri` returns the next "page" of query results. It returns `QueryResults` too, processing it
       the same way recursively until `next_uri` is nil.
    2. A GET to the `final_uri` finally after all results is fetched (`next_uri = nil`) or the remaining is not needed.
       Return empty body.
    3. A GET to the `stats_uri` to get stats only at once (without long-polling), return `QueryRequest` with
       empty `data` field.

### QueryRequest

example:

```
{
   "sql": "select * from numbers(10)"
}
```

### QueryResults

example:

```
{
    "id": "93114794-a532-4706-84c9-61a137398fb8",
    "stats_uri": "/v1/...",
    "next_uri": "/v1/...",
    "final_uri": "/v1/...",
     "data": [
        [1,],
        [2,],
    ],
    "schema":{
        "fields":[{"name":"number","data_type":"UInt64","nullable":false}],
        "metadata":{}
    },
    "state": "SUCCEEDED",
    "stats":{
        "progress":
            {"read_rows":10,
            "read_bytes":80,
            "total_rows_to_read":0
            },
        "wall_time_ms": 10
    },
    error: nil
}
```

fields need explain:

| field  | type       | description                              |
|--------|------------|------------------------------------------|
| state  | string     | choices: "Running","Failed", "Succeeded" |
| error  | QueryError | error of the sql parsing or execution    |
| id     | string     | a uniq query_id for this POST request    |
| data   | array      | each item is a row of results            |
| schema | Schema     | the schema of the results                |

Schema

| field    | type   | description                                               |
|----------|--------|-----------------------------------------------------------|
| fields   | array  | An ordered sequence of Field                              |
| metadata | object | A map of key-value pairs containing additional meta data. |

Field

| field     | type   |
|-----------|--------|
| name      | string |
| data_type | string |
| nullable  | bool   |

QueryStats

| field        | type          | description          |
|--------------|---------------|----------------------|
| wall_time_ms | int           | query execution time |
| progress     | QueryProgress | query progress       |

QueryProgress

| field              | type |
|--------------------|------|
| read_rows          | int  |
| read_bytes         | int  |
| total_rows_to_read | int  |

QueryError

| field     | type   | description                     |
|-----------|--------|---------------------------------|
| stats     | int    | error code used inside databend |
| message   | string | error message                   |
| backtrace | string |                                 |

### HTTP response status code

the usage of status code for different kinds of errors:

| code | error                                                                       |
|------|-----------------------------------------------------------------------------|
| 200  | if sql is invalid or failed, the detail is in the `error` field of the JSON |
| 404  | "query_id" or "page" not found                                              |
| 400  | invalid request format                                                      |

check the response body for error reason as a string when status code is not 200.

## sync endpoint: /v1/statement

1. POST raw sql as body instead of json.
2. return the same QueryResults as `/v1/query`, but return results all at once, so there is no `final_uri` or `next_uri`
   .

## curl examples

/v1/statement

```shell
curl --request POST '127.0.0.1:8001/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'SELECT avg(number) FROM numbers(100000000)'
```

/v1/query

```shell
curl --request POST '127.0.0.1:8001/v1/query/' --header 'Content-Type: application/json' --data-raw '{"sql": "SELECT avg(number) FROM numbers(100000000)"}'"#
```
