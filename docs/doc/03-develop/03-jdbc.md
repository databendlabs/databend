---
title: Developing with Databend using Java
sidebar_label: JDBC
description:
  Develop with Databend using Java.
---

## Requirements
The Databend JDBC driver requires Java LTS (Long-Term Support) versions 1.8 or higher. If the minimum required version of Java is not installed on the client machines where the JDBC driver is installed, you must install either Oracle Java or OpenJDK.

## Oracle Java
Oracle Java currently supports Java 8. For download and installation instructions, go to:

http://www.java.com/en/download/manual.jsp

## OpenJDK

OpenJDK is an open-source implementation of Java that provides JDK 8 packages for various Linux environments. Packages for non-Linux environments or higher Java versions are only available through 3rd parties. For more information, go to:

http://openjdk.java.net

## Downloading / Integrating the JDBC Driver
The JDBC driver (databend-jdbc) is provided as a JAR file, available as an artifact in Maven for download or integrating directly into your Java-based projects.

## Downloading the Driver
To download the driver:

1. Go to the Maven Central Repository:
   https://repo1.maven.org/maven2/com/databend/databend-jdbc/
2. Click on the directory for the version that you need.
3. Download the databend-jdbc-#.#.#.jar file.

> Note:
>
>If desired, you can verify the JDBC driver version by entering the following command:
java -jar databend-jdbc-#.#.#.jar --version, where #.#.# matches the version numbers in the downloaded file name.
If you plan to verify the driver package signature, download the databend-jdbc-#.#.#.jar.asc file.

## Configuring the JDBC Driver
This topic describes how to configure the JDBC driver, including how to connect to Databend using the driver.

>NOTE:
>The connection parameters are now documented in the [JDBC Driver Connection Parameter Reference](#parameter-reference).


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

## Using the JDBC Driver
This topic provides information about how to use the JDBC driver.

## Declare a Maven dependency.
User should declare a Maven project

```xml
<dependency>
    <groupId>com.databend</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>0.0.7</version>
</dependency>
```

## Examples of Queries

```java
package com.example;

import java.sql.*;
import java.util.Properties;

public class demo {
    static final String DB_URL = "jdbc:databend://127.0.0.1:8000";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "user1");
        properties.setProperty("password", "abc123");
        properties.setProperty("SSL", "false");

        Connection conn = DriverManager.getConnection(DB_URL, properties);

        Statement stmt = conn.createStatement();
        String create_sql = "CREATE DATABASE IF NOT EXISTS book_db";
        stmt.execute(create_sql);

        String use_sql = "USE book_db";
        stmt.execute(use_sql);

        String ct_sql = "CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)";
        stmt.execute(ct_sql);
        // Close conn
        conn.close();
        System.exit(0);
```

## Batch Inserts
In your Java application code, you can insert multiple rows in a single batch by binding parameters in an INSERT statement and calling addBatch() and executeBatch().

As an example, the following code inserts two rows into a table that contains an INT column and a VARCHAR column. The example binds values to the parameters in the INSERT statement and calls addBatch() and executeBatch() to perform a batch insert.
```java
Connection connection = DriverManager.getConnection(url, prop);

PreparedStatement pstmt = connection.prepareStatement("INSERT INTO t(c1, c2) VALUES(?, ?)");
pstmt.setInt(1, 101);
pstmt.setString(2, "test1");
pstmt.addBatch();

pstmt.setInt(1, 102);
pstmt.setString(2, "test2");
pstmt.addBatch();

int[] count = pstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
```

## Upload Data Files Directly from a Stream to an Internal Stage

```java
 /**
     * Upload inputStream to the databend internal stage, the data would be uploaded as one file with no split.
     * Caller should close the input stream after the upload is done.
     *
     * @param stageName the stage which receive uploaded file
     * @param destPrefix the prefix of the file name in the stage
     * @param inputStream the input stream of the file
     * @param destFileName the destination file name in the stage
     * @param fileSize the file size in the stage
     * @param compressData whether to compress the data
     * @throws SQLException failed to upload input stream
     */
    public void uploadStream(String stageName, String destPrefix, InputStream inputStream, String destFileName, long fileSize, boolean compressData) throws SQLException;
```

Sample usage:
```java
        File f = new File("test.csv");
        try (InputStream fileInputStream = Files.newInputStream(f.toPath())) {
            Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.ALL);
            Connection connection = createConnection();
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", f.length(), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            f.delete();
        }
```

## Download Data Files Directly from an Internal Stage to a Stream

```java
 /**
     * Download a file from the databend internal stage, the data would be downloaded as one file with no split.
     *
     * @param stageName the stage which contains the file
     * @param sourceFileName the file name in the stage
     * @param decompress whether to decompress the data
     * @return the input stream of the file
     * @throws SQLException
     */
    public InputStream downloadStream(String stageName, String sourceFileName, boolean decompress) throws SQLException;
```

Sample code:
```Java
        File f = new File("test.csv");
        try (InputStream fileInputStream = Files.newInputStream(f.toPath())) {
            Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.ALL);
            Connection connection = createConnection(true);
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", f.length(), false);
            InputStream downloaded = databendConnection.downloadStream(stageName, "jdbc/test/test.csv", false);
            byte[] arr = streamToByteArray(downloaded);
            System.out.println(arr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            f.delete();
        }
```

## Parameter Reference
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
**Description**: Time before all remaining result is ready to return.Default: 60s.

### connection_timeout
**Description**: Connection timeout config for http request.Default: 0.

### socket_timeout
**Description**: Read timeout config for http request.Default: 0.

## Others

### copy_purge
**Description**: If True, the `copy into` will purge the files in the stage after they are loaded successfully into the table. Default: True.