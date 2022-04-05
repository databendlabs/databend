---
title: HTTP Handler
sidebar_label: HTTP Handler
description:
  HTTP Handler
---

Databend's HTTP API(Databend's REST API) is the communication protocol between server and client. It’s used to send query statements for execution on the server and to receive results back to the client.

## HTTP Methods

### Overview

This handler return results in `pages` with long-polling.

1. Start with A `POST` to /v1/query with JSON of type `QueryRequest` which contains the SQL to execute, returns a JSON
   of type `QueryResponse`.
2. Use fields of `QueryResponse` for further processing:
    1. A `GET` to the `next_uri` returns the next "page" of query results. It returns `QueryResponse` too, processing it
       the same way recursively until `next_uri` is nil.
    2. A `GET` to the `final_uri` finally after all results is fetched (`next_uri = nil`) or the remaining is not
       needed. Return empty body.
    3. (optional) A `GET` to the `stats_uri` to get stats only at once (without long-polling), return `QueryResponse`
       with empty `data` field.
3. Use session(Optional)
    1. By default, each request has its own session inside server which is destroyed right after the SQL execution
       finished, even before the result data is fetched by client.
    2. To use SQLs like `set` and `use` which change context for the following SQLs:
        1. For the first `QueryRequest` which **change** context: set `QueryRequest.session.max_idle_time` to positive
           int like 10. remember the returned `QueryResponse.session_id`.
        2. For the following `QueryRequest` which **use** the context: use the `QueryResponse.session_id `
           for `QueryRequest.session.id`.

### Quick Example

```shell
curl --request POST '127.0.0.1:8001/v1/query/' --header 'Content-Type: application/json' --data-raw '{"sql": "SELECT avg(number) FROM numbers(100000000)"}'
```

the SQL will be run with default session and pagination settings, mainly:

1. use a new one-off session with the `default` database.
2. each request wait for at most 1 second for results before return.

for more advanced configs, see the Reference below:

you are expected to get JSON like this (formatted):

```
{
  "id": "3cd25ab7-c3a4-42ce-9e02-e1b354d91f06",
  "session_id": "8d3a737d-2f6c-4df7-ba44-6dfc818255ce",
  "schema": {
    "fields": [
      {
        "name": "avg(number)",
        "default_expr": null,
        "data_type": {
          "type": "Float64Type"
        }
      }
    ],
    "metadata": {}
  },
  "data": [
    [
      49999999.5
    ]
  ],
  "state": "Succeeded",
  "error": null,
  "stats": {
    "scan_progress": {
      "rows": 100000000,
      "bytes": 800000000
    },
    "running_time_ms": 466.85395800000003
  },
  "stats_uri": "/v1/query/3cd25ab7-c3a4-42ce-9e02-e1b354d91f06",
  "final_uri": "/v1/query/3cd25ab7-c3a4-42ce-9e02-e1b354d91f06/kill?delete=true",
  "next_uri": null
}
```

Note:

1. next_uri is null because all data is returned.
1. client should call final_uri to tell the server the client has received the results and server can delete them.


## Query Request

QueryRequest

| field      | type                  | Required | Default    | description                           |
|------------|-----------------------|----------|------------|---------------------------------------|
| sql        | string                | Yes      |            | the sql to execute                    |
| session    | NewSession/OldSession | No       | NewSession | error of the sql parsing or execution |
| pagination | Pagination            | No       |            | a uniq query_id for this POST request |

NewSession

| field         | type   | Required | Default   | description                                                   |
|---------------|--------|----------|-----------|---------------------------------------------------------------|
| database      | string | No       | "default" | set current_database                                          |
| max_idle_time | int    | No       | 0         | secs the Session will be retain after the last query finished |

OldSession

| field | type   | Required | Default | description                              |
|-------|--------|----------|---------|------------------------------------------|
| id    | string | Yes      |         | session_id from QueryResponse.session_id |

PaginationConf: critical conditions for each HTTP request to return (before all remaining result is ready to return)

| field          | type | Required | Default | description       |
|----------------|------|----------|---------|-------------------|
| wait_time_secs | i32  | No       | 1       | long polling time |

## Query Response

QueryResponse:

| field  | type       | description                              |
|--------|------------|------------------------------------------|
| state  | string     | choices: "Running","Failed", "Succeeded" |
| error  | QueryError | error of the sql parsing or execution    |
| id     | string     | a uniq query_id for this POST request    |
| data   | array      | each item is a row of results            |
| schema | Schema     | the schema of the results                |

Schema:

| field    | type   | description                                               |
|----------|--------|-----------------------------------------------------------|
| fields   | array  | An ordered sequence of Field                              |
| metadata | object | A map of key-value pairs containing additional meta data. |

Field:

| field     | type   |
|-----------|--------|
| name      | string |
| data_type | string |
| nullable  | bool   |

Stats:

| field           | type          | description                                                                                                      |
|-----------------|---------------|------------------------------------------------------------------------------------------------------------------|
| running_time_ms | float         | million secs elapsed since query begin to execute internally, stop timing when query Finished (state != Running) |
| scan_progress   | QueryProgress | query scan progress                                                                                              |

Progress:

| field              | type |
|--------------------|------|
| read_rows          | int  |
| read_bytes         | int  |

Error:

| field     | type   | description                     |
|-----------|--------|---------------------------------|
| stats     | int    | error code used inside databend |
| message   | string | error message                   |
| backtrace | string |                                 |

## Response Status Code

The usage of status code for different kinds of errors:

| code | error                                                                       |
|------|-----------------------------------------------------------------------------|
| 200  | if sql is invalid or failed, the detail is in the `error` field of the JSON |
| 404  | "query_id" or "page" not found                                              |
| 400  | invalid request format                                                      |

Check the response body for error reason as a string when status code is not 200.


