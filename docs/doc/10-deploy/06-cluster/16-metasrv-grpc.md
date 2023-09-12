---
title: Send & Receive gRPC Metadata
sidebar_label: Send & Receive gRPC Metadata
description: 
  Send & Receive gRPC Metadata
---

Databend allows you to send and receive gRPC (gRPC Remote Procedure Calls) metadata (key-value pairs) to and from a running meta service cluster with the command-line interface (CLI) commands.

## Update and Create a Key-Value Pair

This command updates an existing key-value pair if the specified key already exists, and creates a new key-value pair if the specified key doesn't exist:

```shell
./databend-meta --grpc-api-address "<grpc-api-address>" --cmd kvapi::upsert --key <key> --value <value>
```

### Examples

```shell
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 1:key1 --value value1
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 1:key2 --value value2
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 1:key3 --value value3
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 2:key1 --value value1
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 2:key2 --value value2
```

## Get Value by a Key

This command gets the value for a specified key:

```shell
./databend-meta --grpc-api-address "<grpc-api-address>" --cmd kvapi::get --key <key>
```

### Examples

```shell
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::get --key 1:key1
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::get --key 2:key2
```

## Get Values by Multiple Keys

This command gets values for multiple specified keys:

```shell
./databend-meta --grpc-api-address "<grpc-api-address>" --cmd kvapi::mget --key <key1> <key2> ...
```

### Examples

```shell
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::mget --key 1:key1 2:key2
```

## List Key-Value Pairs by a Prefix

This command lists the existing key-value pairs by a specified prefix for the key:

```shell
./databend-meta --grpc-api-address "<grpc-api-address>" --cmd kvapi::list --prefix <prefix>
```

### Examples

```shell
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::list --prefix 1:
./databend-meta --grpc-api-address "127.0.0.1:9191" --cmd kvapi::list --prefix 2:
```