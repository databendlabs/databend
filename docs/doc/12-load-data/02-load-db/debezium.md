---
title: Debezium
---

[Debezium](https://debezium.io/) is a set of distributed services to capture changes in your databases so that your applications can see those changes and respond to them. Debezium records all row-level changes within each database table in a change event stream, and applications simply read these streams to see the change events in the same order in which they occurred.

[debezium-server-databend](https://github.com/databendcloud/debezium-server-databend) is a lightweight CDC tool developed by Databend, based on Debezium Engine. Its purpose is to capture real-time changes in relational databases and deliver them as event streams to ultimately write the data into the target database Databend. This tool provides a simple way to monitor and capture database changes, transforming them into consumable events without the need for large data infrastructures like Flink, Kafka, or Spark.

## Installing debezium-server-databend

debezium-server-databend can be installed independently without the need for installing Debezium beforehand. Once you have decided to install debezium-server-databend, you have two options available. The first one is to install it from source by downloading the source code and building it yourself. Alternatively, you can opt for a more straightforward installation process using Docker.

### Installing from Source

Before you start, make sure JDK 11 and Maven are installed on your system.

1. Clone the project:

```bash
git clone https://github.com/databendcloud/debezium-server-databend.git
```

2. Change into the project's root directory:

```bash
cd debezium-server-databend
```

3. Build and package debezium server:

```go
mvn -Passembly -Dmaven.test.skip package
```

4. Once the build is completed, unzip the server distribution package:

```bash
unzip debezium-server-databend-dist/target/debezium-server-databend-dist*.zip -d databendDist
```

5. Enter the extracted folder:

```bash
cd databendDist
```

6. Create a file named *application.properties* in the *conf* folder with the content in the sample [here](https://github.com/databendcloud/debezium-server-databend/blob/main/debezium-server-databend-dist/src/main/resources/distro/conf/application.properties.example), and modify the configurations according to your specific requirements. For description of the available parameters, see this [page](https://github.com/databendcloud/debezium-server-databend/blob/main/docs/docs.md).

```bash
nano conf/application.properties
```

7. Use the provided script to start the tool:

```bash
bash run.sh
```

### Installing with Docker

Before you start, make sure Docker and Docker Compose are installed on your system.

1. Create a file named *application.properties* in the *conf* folder with the content in the sample [here](https://github.com/databendcloud/debezium-server-databend/blob/main/debezium-server-databend-dist/src/main/resources/distro/conf/application.properties.example), and modify the configurations according to your specific requirements. For description of the available Databend parameters, see this [page](https://github.com/databendcloud/debezium-server-databend/blob/main/docs/docs.md).

```bash
nano conf/application.properties
```

2. Create a file named *docker-compose.yml* with the following content: 

```dockerfile
version: '2.1'
services:
  debezium:
    image: ghcr.io/databendcloud/debezium-server-databend:pr-2
    ports:
      - "8080:8080"
      - "8083:8083"
    volumes:
      - $PWD/conf:/app/conf
      - $PWD/data:/app/data
```

3. Open a terminal or command-line interface and navigate to the directory containing the *docker-compose.yml* file.

4. Use the following command to start the tool:

```bash
docker-compose up -d
```

## Usage Example

This section demonstrates the general steps to load data from MySQL into Databend and assumes that you already have a local MySQL instance running.

### Step 1. Prepare Data in MySQL

Create a database and a table in MySQL, and insert sample data into the table.

```sql
CREATE DATABASE mydb;
USE mydb;

CREATE TABLE products (id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,name VARCHAR(255) NOT NULL,description VARCHAR(512));
ALTER TABLE products AUTO_INCREMENT = 10;

INSERT INTO products VALUES (default,"scooter","Small 2-wheel scooter"),
(default,"car battery","12V car battery"),
(default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3"),
(default,"hammer","12oz carpenter's hammer"),
(default,"hammer","14oz carpenter's hammer"),
(default,"hammer","16oz carpenter's hammer"),
(default,"rocks","box of assorted rocks"),
(default,"jacket","water-proof black wind breaker"),
(default,"cloud","test for databend"),
(default,"spare tire","24 inch spare tire");
```

### Step 2. Create database in Databend

Create the corresponding database in Databend. Please note that you don't need to create a table that corresponds to the one in MySQL.

```sql
CREATE DATABASE debezium;
```

### Step 3. Create application.properties

Create the file *application.properties*, then start debezium-server-databend. For how to install and start the tool, see [Installing debezium-server-databend](#installing-debezium-server-databend).

When started for the first time, the tool performs a full synchronization of data from MySQL to Databend using the specified Batch Size. As a result, the data from MySQL is now visible in Databend after successful replication.

```text title='application.properties'
debezium.sink.type=databend
debezium.sink.databend.upsert=true
debezium.sink.databend.upsert-keep-deletes=false
debezium.sink.databend.database.databaseName=debezium
debezium.sink.databend.database.url=jdbc:databend://<your-databend-host>:<port>
debezium.sink.databend.database.username=<your-username>
debezium.sink.databend.database.password=<your-password>
debezium.sink.databend.database.primaryKey=id
debezium.sink.databend.database.tableName=products
debezium.sink.databend.database.param.ssl=true

# enable event schemas
debezium.format.value.schemas.enable=true
debezium.format.key.schemas.enable=true
debezium.format.value=json
debezium.format.key=json

# mysql source
debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector
debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=60000

debezium.source.database.hostname=127.0.0.1
debezium.source.database.port=3306
debezium.source.database.user=root
debezium.source.database.password=123456
debezium.source.database.dbname=mydb
debezium.source.database.server.name=from_mysql
debezium.source.include.schema.changes=false
debezium.source.table.include.list=mydb.products
# debezium.source.database.ssl.mode=required
# Run without Kafka, use local file to store checkpoints
debezium.source.database.history=io.debezium.relational.history.FileDatabaseHistory
debezium.source.database.history.file.filename=data/status.dat
# do event flattening. unwrap message!
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.delete.handling.mode=rewrite
debezium.transforms.unwrap.drop.tombstones=true

# ############ SET LOG LEVELS ############
quarkus.log.level=INFO
# Ignore messages below warning level from Jetty, because it's a bit verbose
quarkus.log.category."org.eclipse.jetty".level=WARN
```

You're all set! If you query the products table in Databend, you will see that the data from MySQL has been successfully synchronized. Feel free to perform insertions, updates, or deletions in MySQL, and you will observe the corresponding changes reflected in Databend as well.