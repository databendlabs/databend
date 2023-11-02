---
title: HTTP Handler
sidebar_label: REST
description:
  HTTP Handler.
---

The Databend HTTP handler is a REST API that used to send query statement for execution on the server and to receive results back to the client.

The HTTP handler is hosted by databend-query, it can be specified by using `--http_handler_host` and `--http_handler_port`(This defaults to 8000).


## HTTP Methods

### Overview

This handler return results in `pages` with long-polling.

1. Start with A `POST` to `/v1/query` with JSON of type `QueryRequest` which contains the SQL to execute, returns a JSON
   of type `QueryResponse`.
2. Use fields of `QueryResponse` for further processing:
    1. A `GET` to the `next_uri` returns the next `page` of query results. It returns `QueryResponse` too, processing it
       the same way until `next_uri` is null.
    2. (optional) A `GET` to the `kill_uri` to kill the query. Return empty body.
    3. (optional) A `GET` to the `stats_uri` to get stats only at once (without long-polling), return `QueryResponse`
       with empty `data` field.

### Quick Example

```shell
curl -u root: \
  --request POST \
  '127.0.0.1:8001/v1/query/' \
  --header 'Content-Type: application/json' \
  --data-raw '{"sql": "SELECT avg(number) FROM numbers(100000000)"}'
```

the SQL will be run with default session and pagination settings, mainly:

1. use a new one-off session with the `default` database.
2. each request wait for at most 1 second for results before return.

for more advanced configs, see the Reference below:

you are expected to get JSON like this (formatted):

```json
{
  "id":"b22c5bba-5e78-4e50-87b0-ec3855c757f5",
  "session_id":"5643627c-a900-43ac-978f-8c76026d9944",
  "session":{
    
  },
  "schema":[
    {
      "name":"avg(number)",
      "type":"Nullable(Float64)"
    }
  ],
  "data":[
    [
      "49999999.5"
    ]
  ],
  "state":"Succeeded",
  "error":null,
  "stats":{
    "scan_progress":{
      "rows":100000000,
      "bytes":800000000
    },
    "write_progress":{
      "rows":0,
      "bytes":0
    },
    "result_progress":{
      "rows":1,
      "bytes":9
    },
    "total_scan":{
      "rows":100000000,
      "bytes":800000000
    },
    "running_time_ms":446.748083
  },
  "affect":null,
  "stats_uri":"/v1/query/b22c5bba-5e78-4e50-87b0-ec3855c757f5",
  "final_uri":"/v1/query/b22c5bba-5e78-4e50-87b0-ec3855c757f5/final",
  "next_uri":"/v1/query/b22c5bba-5e78-4e50-87b0-ec3855c757f5/final",
  "kill_uri":"/v1/query/b22c5bba-5e78-4e50-87b0-ec3855c757f5/kill"
}
```


## Query Request

QueryRequest

| field         | type         | Required | Default | description                                      |
|---------------|--------------|----------|---------|--------------------------------------------------|
| sql           | string       | Yes      |         | the sql to execute                               |
| session_id    | string       | No       |         | used only when reuse server-side session         |
| session       | SessionState | No       |         |                                                  |
| pagination    | Pagination   | No       |         | a uniq query_id for this POST request            |

SessionState

| field                    | type                | Required | Default   | description                                                   |
|--------------------------|---------------------|----------|-----------|---------------------------------------------------------------|
| database                 | string              | No       | "default" | set current_database                                          |
| keep_server_session_secs | int                 | No       | 0         | secs the Session will be retain after the last query finished |
| settings                 | map(string, string) | No       | 0         |                                                               |

OldSession

| field | type   | Required | Default | description                              |
|-------|--------|----------|---------|------------------------------------------|
| id    | string | Yes      |         | session_id from QueryResponse.session_id |

Pagination: critical conditions for each HTTP request to return (before all remaining result is ready to return)

| field          | type | Required | Default | description       |
|----------------|------|----------|---------|-------------------|
| wait_time_secs | u32  | No       | 1       | long polling time |

## Query Response

QueryResponse:

| field      | type          | description                              |
|------------|---------------|------------------------------------------|
| state      | string        | choices: "Running","Failed", "Succeeded" |
| error      | QueryError    | error of the sql parsing or execution    |
| id         | string        | a uniq query_id for this POST request    |
| data       | array         | each item is a row of results            |
| schema     | array         | An ordered sequence of Field             |
| affect     | Affect        | the affect of some queries               |
| session_id | String        |                                          |
| session    | SessionState  |                                          |

Field:

| field    | type   |
|----------|--------|
| name     | string |
| type     | string |

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

Affect:

| field | type   | description         |
|-------|--------|---------------------|
| type  | string | ChangeSetting/UseDB |
| ...   |        | according to type   |

### Response Status Code

The usage of status code for different kinds of errors:

| code | error                                                                       |
|------|-----------------------------------------------------------------------------|
| 200  | if sql is invalid or failed, the detail is in the `error` field of the JSON |
| 404  | "query_id" or "page" not found                                              |
| 400  | invalid request format                                                      |

Check the response body for error reason as a string when status code is not 200.

### data format

all field value in `.data` is represented in string,
client need to interpreter the values with the help of information in the `schema` field.

### client-side session

Dur to the stateless nature of HTTP, it is hard to maintain session in server side.
client need to config the session in the `session` field when starting a new Request.

```json
{
  "sql": "select 1", 
  "session": {
    "database": "db2",
    "settings": {
      "max_threads": "1"
    }
  } 
}
```

all the values of settings should be string.

If the SQL is `set` or `use`, the `session` will be change, can carry back to client in side response, 
client need to record it and put it in the following Request.

### QueryAffect (Experimental)

For each SQL, client get an optional table-formed `result`.
Client also get info about `progress` about rows/bytes read/write.
Due to the limit of them, client may not get all interesting information about the Query.
So we add `QueryAffect` to carry some extra information about the Query.

Note that `QueryAffect` is for advanced user and is not stable. 
It is not recommended to QueryAffect to maintain session.

### Example for Session and QueryAffect: 


set statement:

```json
{
  "sql": "set max_threads=1;",
  "session": {
    "database": "db1",
    "settings": {
      "max_threads": "6"
    }
  }
}
```

response:

```json
{
  "affect": {
    "type": "ChangeSetting",
    "key": "max_threads",
    "value": "1",
    "is_global": false
  },
  "session": {
    "database": "db1",
    "settings": {
      "max_threads": "1"
    }
  }
}
```

use statement:

```json
{"sql": "use db2",
  "session": {
    "database": "db1",
    "settings": {
      "max_threads": "6"
    }
  }
}
```

response:

```json
{
  "affect": {
    "type": "UseDB",
    "name": "db2"
  },
  "session": {
    "database": "db2",
    "settings": {
      "max_threads": "1"
    }
  }
}
```

## client implementations

The official client [bendsql](https://github.com/datafuselabs/bendsql) is mainly base on HTTP handler.

The most simple example of http handler client implementation is in [sqllogictest](https://github.com/datafuselabs/databend/blob/main/tests/sqllogictests/src/client/http_client.rs) for databend.


