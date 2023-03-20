---
title: bendsql
sidebar_label: bendsql
description:
  Databend-native CLI
---

[bendsql](https://github.com/databendcloud/bendsql) is a command line tool that has been designed specifically for Databend. It allows users to establish a connection with Databend and execute queries directly from a CLI window. 

This tool is particularly useful for those who prefer a command line interface and need to work with Databend on a regular basis. With bendsql, users can easily and efficiently manage their databases, tables, and data, and perform a wide range of queries and operations with ease. 

## Downloading and Installing bendsql

To download and install bendsql, please go to the [bendsql](https://github.com/databendcloud/bendsql) repository on GitHub and follow the README instructions.

## Connecting to Databend

Use `bendsql connect` to connect to a Databend instance:

```shell
eric@ericdeMacBook rsdoc % bendsql connect -h
Connect to Databend Instance

USAGE
  bendsql connect [flags]

FLAGS
  -d, --database string    (default "default")
  -H, --host string        (default "localhost")
  -p, --password string   
  -P, --port int           (default 8000)
      --ssl               
  -u, --user string        (default "root")

INHERITED FLAGS
  --help   Show help for command

LEARN MORE
  Use 'bendsql <command> <subcommand> --help' for more information about a command.
```

To connect to a local Databend, simply run `bendsql connect`:

```shell
eric@ericdeMacBook rsdoc % bendsql connect   
Connected to Databend on Host: localhost
Version: DatabendQuery v0.9.58-nightly-790be61(rust-1.68.0-nightly-2023-03-01T16:41:18.376657Z)
```
## Running Queries with bendsql

After connecting bendsql to your Databend instance, you can use `bendsql query` to run queries with the tool:

```shell
eric@ericdeMacBook rsdoc % bendsql query
Connected with driver databend (DatabendQuery v0.9.58-nightly-790be61(rust-1.68.0-nightly-2023-03-01T16:41:18.376657Z))
Type "help" for help.

dd:root@localhost/default=> SELECT NOW();
+------------------------+
|         now()          |
+------------------------+
| 2023-03-02T21:47:10.4Z |
+------------------------+
(1 row)
```