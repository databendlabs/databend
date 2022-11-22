---
title: Databend Meta CLI
sidebar_label: Meta Service Command Line API
description:
    Access Databend Meta Service Cluster with Command Line Interface
---

The binary `databend-meta` provides several handy commands to access the KVApi of databend meta service.
`databend-meta --help` also includes a simple guide on using these CLI commands.

:::caution

These are low level API and should be avoided using if possible:


- Some data stored in databend-meta are correlated, adding or deleting may break these internal consistency.
  E.g., deleting a `database` when there are still `table`s belonging to it.

- Most data in databend-mata are raw bytes. Data interpreting is done on the client side, i.e., by databend-query.
  Modifying data may lead to compatibility issues.

:::

:::note

The command line API is limited that:
- only string key and string value are supported.
- `seq` is not supported.

:::


### Set `foo=bar`:
```shell
databend-meta --grpc-api-address 1.2.3.4:5678 --cmd kvapi::upsert --key foo --value bar
```
Output is the state before and after applying this command:
```json
{
  "ident": null,
  "prev": {
    "seq": 18,
    "meta": null,
    "data": [ 98, 97, 114 ]
  },
  "result": {
    "seq": 20,
    "meta": null,
    "data": [ 98, 97, 114 ]
  }
}
```

### Set `foo=bar` and inform databend-meta to delete it after 5 seconds:
```shell
databend-meta --grpc-api-address 1.2.3.4:5678 --cmd kvapi::upsert --key foo --value bar --expire-after 5
```
Output is the state before and after applying this command and `expire_at` is set.
```json
{
  "ident": null,
  "prev": {
    "seq": 20,
    "meta": null,
    "data": [ 98, 97, 114 ]
  },
  "result": {
    "seq": 21,
    "meta": {
      "expire_at": 1668996718
    },
    "data": [ 98, 97, 114 ]
  }
}
```

### Delete `foo`:
```shell
databend-meta --grpc-api-address 1.2.3.4:5678 --cmd kvapi::delete --key foo
```
Output is the state before and after applying this command and `result` is always `null`.
```json
{
  "ident": null,
  "prev": {
    "seq": 22,
    "meta": null,
    "data": [ 98, 97, 114 ]
  },
  "result": null
}
```


### Get `foo`:
```shell
databend-meta --grpc-api-address 1.2.3.4:5678 --cmd kvapi::get --key foo
```
Output is the state of key `foo`.
```json
{
  "seq": 23,
  "meta": null,
  "data": [ 98, 97, 114 ]
}
```

### Get multiple keys with mget: `foo,bar,wow`:
```shell
databend-meta --grpc-api-address 1.2.3.4:5678 --cmd kvapi::mget --key foo bar wow
```
Output is the states of every specified key.
```json
[
  {
    "seq": 23,
    "meta": null,
    "data": [ 98, 97, 114 ]
  },
  null,
  null
]
```

### List keys starting with `foo/`:
```shell
databend-meta --grpc-api-address 1.2.3.4:5678 --cmd kvapi::list --prefix foo/
```
Output is the key values of every key starting with `prefix`.
```json
[
  [
    "foo/a",
    {
      "seq": 24,
      "meta": null,
      "data": [ 98, 97, 114 ]
    }
  ],
  [
    "foo/b",
    {
      "seq": 25,
      "meta": null,
      "data": [ 119, 111, 119 ]
    }
  ]
]
```
