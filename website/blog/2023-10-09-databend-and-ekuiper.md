---
title: "Sending IoT Stream Data to Databend with LF Edge eKuiper"
date: 2023-10-09
slug: 2023-10-09-databend-and-ekuiper
cover_url: 'ekuiper/databend-and-ekuiper-0.jpg'
tags: [databend, eKuiper, IoT]
description: "eKuiper, a stream processing software under EMQ, is known for its compact size  and robust functionality. It is widely utilized in various applications  such as industrial IoT, vehicular networks, and public data analysis.  This article has provided insights into how to use eKuiper for writing  IoT stream processing data into Databend."
authors:
- name: hantmac
  url: https://github.com/hantmac
  image_url: https://github.com/hantmac.png
---

[LF Edge eKuiper](https://github.com/lf-edge/ekuiper) is a lightweight IoT data analytics and stream processing engine  running on resource-constraint edge devices. The major goal for eKuiper  is to provide a streaming software framework (similar to [Apache Flink](https://flink.apache.org/)) in edge side. eKuiper’s **rule engine** allows users to provide either SQL based or graph based (similar to  Node-RED) rules to create IoT edge analytics applications within few  minutes.

eKuiper supports extensions in three aspects: Source, SQL functions, and Sink,  through both Golang and Python. By supporting different Sinks, it allows users to send analysis results to various external systems. Databend  has also been integrated into the eKuiper plugin as a Sink. Below is an  example demonstrating how to use eKuiper to write IoT stream processing  data into Databend.

## Building eKuiper and Databend SQL Plugin

### eKuiper

```bash
git clone https://github.com/lf-edge/ekuiper & cd ekuiper
make
```

### Databend SQL Plugin

Build the sink plugin:

```bash
go build -trimpath --buildmode=plugin -tags databend -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
```

Copy the built sink plugin to the build directory:

```bash
cp plugins/sinks/Sql.so _build/kuiper-1.11.1-18-g42d9147f-darwin-arm64/plugins/sinks
```

## Creating Table in Databend

Create the target table "ekuiper_test" in Databend:

```sql
create table ekuiper_test (name string,size bigint,id bigint);
```

## Starting eKuiper

```bash
cd _build/kuiper-1.11.1-18-g42d9147f-darwin-arm64 
./bin/kuiperd
```

![img](../static/img/blog/ekuiper/databend-and-ekuiper-2.PNG)

## Creating Streams and Rules

eKuiper offers two methods for managing streams, rules, and target  destinations. One approach is to launch a visual management interface  through the ekuiper-manager Docker image (https://hub.docker.com/r/lfedge/ekuiper), while the other is to manage them using the CLI tool.

### Creating Stream

A stream is the operational form of data source connectors in eKuiper. It must specify a source type to define how it connects to external  resources. Here, we create a stream to retrieve data from a JSON file  data source and send it to eKuiper. First, configure the file data source; the connector’s configuration file is located at `/etc/sources/file.yaml`.

```yaml
default:
  # File type, supports json, csv, and lines
  fileType: json
  # Absolute path to the directory or file under eKuiper's root directory.
  # Do not include the filename here. The filename should be defined in the stream data source.
  path: data
  # Time interval for reading files, in milliseconds. Set to 0 if reading only once.
  interval: 0
  # Interval between sending two pieces of data after reading.
  sendInterval: 0
  # Whether to read files in parallel from the directory.
  parallel: false
  # Action after reading the file
  # 0: Keep the file unchanged
  # 1: Delete the file
  # 2: Move the file to the location defined in moveTo
  actionAfterRead: 0
  # Location to move the file, only used for actionAfterRead = 2
  moveTo: /tmp/kuiper/moved
  # Whether to include a file header, mainly used for CSV. If true, the first line is parsed as the file header.
  hasHeader: false
  # Define the columns of the file. If a file header is defined, this option will be overridden.
  # columns: [id, name]
  # Ignore the content of how many lines at the beginning.
  ignoreStartLines: 0
  # Ignore the content of how many lines at the end. The last empty line is not counted.
  ignoreEndLines: 0
  # Use the specified compression method to decompress the file. Supported methods now include 'gzip' and 'zstd'.
  decompression: ""
```

Create a stream named "stream1" in the terminal:

```bash
./bin/kuiper create stream stream1 '(id BIGINT, name STRING,size BIGINT) WITH (DATASOURCE="test.json", FORMAT="json", TYPE="file");'
```

![img](../static/img/blog/ekuiper/databend-and-ekuiper-3.PNG)

Content in the JSON file:

```json
[
  {"id": 1,"size":100, "name": "John Doe"},
  {"id": 2,"size":200, "name": "Jane Smith"},
  {"id": 3,"size":300, "name": "Kobe Brant"},
  {"id": 4,"size":400, "name": "Alen Iverson"}
]
```

### Creating Databend Sink Rule

A rule represents a stream processing workflow, outlining the journey of  data from its input source through various processing logic, and  ultimately to actions that dispatch the data to external systems.  eKuiper offers two methods to define the business logic of rules: either through SQL/action combinations or utilizing the newly introduced graph API. In this context, we define the business logic of a rule in a  declarative manner by specifying the `sql` and `actions` attributes. Within this approach, the `sql` defines SQL queries to be executed against predefined streams, thereby  transforming the data. Subsequently, the resulting data can be directed  to multiple destinations through the `action`."

Rules are defined using JSON. Below is the rule that we are preparing to create, named "myRule.json":

```json
{
  "id": "myRule",
  "sql": "SELECT id, name from stream1",
  "actions": [
    {
      "log": {
      },
      "sql": {
        "url": "databend://databend:databend@localhost:8000/default?sslmode=disable",
        "table": "ekuiper_test",
        "fields": ["id","name"]
      }
    }
  ]
}
```

Create a rule in the terminal:

```bash
./bin/kuiper create rule myRule -f myRule.json
```

![img](../static/img/blog/ekuiper/databend-and-ekuiper-4.PNG)

View the status of the created rule:

```bash
./bin/kuiper getstatus rule myRule
```

![img](../static/img/blog/ekuiper/databend-and-ekuiper-5.PNG)

Once the rule is created, data that satisfies the rule’s criteria is  immediately sent to the destination. At this juncture, when we inspect  the ‘ekuiper_test’ table in Databend, we can observe that the data from  the file data source has been successfully ingested into Databend:

![img](../static/img/blog/ekuiper/databend-and-ekuiper-6.PNG)

It can be observed that, because our SQL rule only specified the `id` and `name` fields, only these two fields have been included in the output.

## Summary

eKuiper, a stream processing software under EMQ, is known for its compact size  and robust functionality. It is widely utilized in various applications  such as industrial IoT, vehicular networks, and public data analysis.  This article has provided insights into how to use eKuiper for writing  IoT stream processing data into Databend.