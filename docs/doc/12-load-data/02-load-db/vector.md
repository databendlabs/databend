---
title: Vector
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.55"/>

[Vector](https://vector.dev/) is a high-performance observability data pipeline that puts organizations in control of their observability data. Collect, transform, and route all your logs, metrics, and traces to any vendors you want today and any other vendors you may want tomorrow. Vector enables dramatic cost reduction, novel data enrichment, and data security where you need it, not where is most convenient for your vendors. Open source and up to 10x faster than every alternative.

Vector natively supports delivering data to [Databend as a sink](https://vector.dev/docs/reference/configuration/sinks/databend/), this means that Vector can send data to Databend for storage or further processing. Databend acts as the destination for the data collected and processed by Vector. By configuring Vector to use Databend as a sink, you can seamlessly transfer data from Vector to Databend, enabling efficient data analysis, storage, and retrieval. 

## Integrating with Vector

To integrate Databend with Vector, start by creating an SQL account in Databend and assigning appropriate permissions. This account will be used for communication and data transfer between Vector and Databend. Then, in the Vector configuration, set up Databend as a Sink.

### Step 1: Creating a SQL User in Databend

For instructions on how to create a SQL user in Databend and grant appropriate privileges, see [Create User](../../14-sql-commands/00-ddl/30-user/01-user-create-user.md). Here's an example of creating a user named *user1* with the password *abc123*:

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';

CREATE DATABASE nginx;

GRANT INSERT ON nginx.* TO user1;
```

### Step 2. Configure Databend as a Sink in Vector

In this step, configure Databend as a sink in Vector by specifying the necessary settings such as the input source, compression, database, endpoint, table, and authentication credentials (username and password) for Databend integration. The following is a simple example of configuring Databend as a sink. For a comprehensive list of configuration parameters, refer to the Vector documentation at https://vector.dev/docs/reference/configuration/sinks/databend/

```toml title='vector.toml'
...

[sinks.databend_sink]
type = "databend"
inputs = [ "my-source-or-transform-id" ] # input source
compression = "none"
database = "nginx" #Your database
endpoint = "http://localhost:8000"
table = "mytable" #Your table

...

[sinks.databend_sink.auth]
strategy = "basic"
// highlight-next-line
user = "user1" #Databend username
// highlight-next-line
password = "abc123" #Databend password

...
```

**Related topics**:

- [Analyzing Nginx Access Logs with Databend](../../21-use-cases/02-analyze-nginx-logs-with-databend-and-vector.md): This guide provides step-by-step instructions on gathering and analyzing Nginx access logs in real time using Databend and Vector.