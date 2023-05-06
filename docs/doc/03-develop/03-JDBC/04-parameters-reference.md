---
title: JDBC Driver Connection Parameter Reference
sidebar_label: Parameter Reference
description:
  JDBC Driver Connection Parameter Reference
---
This topic lists the connection parameters that you can use to configure the JDBC driver. You can set these parameters in the JDBC connection string or in a Java Properties object.

## Required Parameters
This section lists the parameters that you must set in the connection string or in the Map of properties.

### username
**Description**: Specifies the login name of the user for the connection.


## Authentication Parameters

### password
**Description**: Specifies the password of the user for the connection.

### ssl
**Description**: Specifies the host using http or https.Default: False.

## Timeout Parameters

### wait_time_secs
**Description**: Bloking time before all remaining result is ready to return.Default: 60s.

### connection_timeout
**Description**: Connection timeout config for http request.Default: 0.

### socket_timeout
**Descrition**: Read timeout config for http request.Default: 0.

## Others

### copy_purge
**Description**: If True, the `copy into` will purge the files in the stage after they are loaded successfully into the table. Default: True.