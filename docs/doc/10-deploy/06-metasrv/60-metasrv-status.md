---
title: Databend Meta Cluster Status
sidebar_label: Meta Service Status
description: 
  Databend Meta Service Cluster Status
---

## Cluster Status

The simplest way to see the cluster status is to cURL the status HTTP API `HTTP_ADDRESS:HTTP_PORT/v1/status`, the API will return json format of cluster status(after pretty formating):

``` json
{
    "id":1,
    "endpoint":"localhost:29103",
    "db_size":1050036,
    "state":"Leader",
    "is_leader":true,
    "current_term":46,
    "last_log_index":14,
    "last_applied":{
        "term":46,
        "index":14
    },
    "leader":{
        "name":"1",
        "endpoint":{
            "addr":"localhost",
            "port":29103
        },
        "grpc_api_addr":"0.0.0.0:19191"
    },
    "voters":[
        {
            "name":"1",
            "endpoint":{
                "addr":"localhost",
                "port":29103
            },
            "grpc_api_addr":"0.0.0.0:19191"
        },
        {
            "name":"2",
            "endpoint":{
                "addr":"localhost",
                "port":29203
            },
            "grpc_api_addr":"0.0.0.0:29191"
        },
        {
            "name":"3",
            "endpoint":{
                "addr":"localhost",
                "port":29303
            },
            "grpc_api_addr":"0.0.0.0:39191"
        }
    ],
    "non_voters":[

    ]
}
```
