---
title: Configuring the JDBC Driver
sidebar_label: Configuring 
description:
  Configuring the JDBC Driver
---

This topic describes how to configure the JDBC driver, including how to connect to Databend using the driver.

>NOTE:
>The connection parameters are now documented in the [JDBC Driver Connection Parameter Reference](./04-parameters-reference.md).


## JDBC Driver Connection String

```
jdbc:databend://<username>:<password>@<host_port>/<database>?<connection_params>
```


### connection_params
Specifies a series of one or more JDBC connection parameters, in the form of `param=value`, with each parameter separated by the ampersand character (&), and no spaces anywhere in the connection string.
You can set these parameters in the connection string:
```
jdbc:databend://<username>:<password>@<host_port>/<database>?<connection_params>
```
or set these parameters in a Properties object that you pass to the DriverManager.getConnectionIO method:
```java 
Properties props = new Properties();
props.put("parameter1", parameter1Value);
props.put("parameter2", parameter2Value);
Connection con = DriverManager.getConnection("jdbc:databend://user:pass@host/database", props);
```
