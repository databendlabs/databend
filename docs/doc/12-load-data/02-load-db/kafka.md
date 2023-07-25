---
title: Kafka
---

[Apache Kafka](https://kafka.apache.org/) is an open-source distributed event streaming platform that allows you to publish and subscribe to streams of records. It is designed to handle high-throughput, fault-tolerant, and real-time data feeds. Kafka enables seamless communication between various applications, making it an ideal choice for building data pipelines and streaming data processing applications.

Databend provides an efficient data ingestion tool ([bend-ingest-kafka](https://github.com/databendcloud/bend-ingest-kafka)), specifically designed to load data from Kafka into Databend. With this tool, you can seamlessly transfer data from Kafka topics and insert it into your target Databend table, streamlining your data ingestion workflow. 

## Installing bend-ingest-kafka

To install the tool, make sure you have Go programming language installed on your computer, and then use the "go get" command as shown:

```bash
go get https://github.com/databendcloud/bend-ingest-kafka
```
## Usage Example

This section assumes your data in Kafka appears as follows and explains how to load data into Databend using the tool bend-ingest-kafka.

```json
{
  "employee_id": 10,
  "salary": 30000,
  "rating": 4.8,
  "name": "Eric",
  "address": "123 King Street",
  "skills": ["Java", "Python"],
  "projects": ["Project A", "Project B"],
  "hire_date": "2011-03-06",
  "last_update": "2016-04-04 11:30:00"
}
```

### Step 1. Create a Table in Databend

Before ingesting data, you need to create a table in Databend that matches the structure of your Kafka data.

```sql
CREATE TABLE employee_data (
  employee_id Int64,
  salary UInt64,
  rating Float64,
  name String,
  address String,
  skills Array(String),
  projects Array(String),
  hire_date Date,
  last_update DateTime
);
```

### Step 2. Run bend-ingest-kafka

Once the table is created, execute the bend-ingest-kafka command with the required parameters to initiate the data loading process. The command will start the data ingester, which continuously monitors your Kafka topic, consumes the data, and inserts it into the specified table in Databend.

```bash
bend-ingest-kafka \
  --kafka-bootstrap-servers="127.0.0.1:9092,127.0.0.2:9092" \
  --kafka-topic="Your Topic" \
  --kafka-consumer-group="Consumer Group" \
  --databend-dsn="http://root:root@127.0.0.1:8000" \
  --databend-table="default.employee_data" \
  --data-format="json" \
  --batch-size=100000 \
  --batch-max-interval=300s
```

| Parameter                 	| Description                                                                                         	|
|---------------------------	|-----------------------------------------------------------------------------------------------------	|
| --kafka-bootstrap-servers 	| Comma-separated list of Kafka bootstrap servers to connect to.                                      	|
| --kafka-topic             	| The Kafka topic from which the data will be ingested.                                               	|
| --kafka-consumer-group    	| The consumer group for Kafka consumer to join.                                                      	|
| --databend-dsn            	| The Data Source Name (DSN) to connect to Databend. Format: `http(s)://username:password@host:port`. 	|
| --databend-table          	| The target Databend table where the data will be inserted.                                          	|
| --data-format             	| The format of the data being ingested.                                                              	|
| --batch-size              	| The number of records per batch during ingestion.                                                   	|
| --batch-max-interval      	| The maximum interval (in seconds) to wait before flushing a batch.                                  	|