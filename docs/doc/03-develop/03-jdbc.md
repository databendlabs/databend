---
title: Developing with Databend using Java
sidebar_label: Java
---

You can connect to and interact with Databend or Databend Cloud from various client tools and applications through a native interface designed for Java programming language, the [Databend JDBC driver](https://github.com/databendcloud/databend-jdbc).

## Installing Databend JDBC Driver

This topic outlines the steps to downloads and install the Databend JDBC driver for use in Java-based projects. The driver requires Java LTS (Long-Term Support) versions 1.8 or higher. If your client machine does not have the minimum required version of Java, install [Oracle Java](http://www.java.com/en/download/manual.jsp) or [OpenJDK](http://openjdk.java.net).

To download the Databend JDBC driver:

1. Go to the Maven Central Repository at https://repo1.maven.org/maven2/com/databend/databend-jdbc/
2. Click on the directory of the latest version.
3. Download the jar file, for example, *databend-jdbc-0.0.7.jar*.

To verify the version of Databend JDBC driver, for example, *databend-jdbc-0.0.7.jar*, run the following command in the terminal:

```bash
java -jar databend-jdbc-0.0.7.jar --version
```

The Databend JDBC driver is provided as a JAR file and can be integrated directly into your Java-based projects. Alternatively, you can declare a Maven dependency in your project's pom.xml file, like so:

```xml
<dependency>
    <groupId>com.databend</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>0.0.7</version>
</dependency>
```

:::tip DID YOU KNOW?
You can also connect to Databend from DBeaver through the Databend JDBC driver. For more information, see [Connecting to Databend with JDBC](../11-integrations/30-access-tool/02-jdbc.md).
:::

## Configuring Connection String

Once the driver is installed and integrated into your project, you can use it to connect to Databend using the following JDBC connection string format:

```java
jdbc:databend://<username>:<password>@<host_port>/<database>?<connection_params>
```

The `connection_params` refers to a series of one or more parameters in the format of `param=value`. Each parameter should be separated by the ampersand character (&), and there should be no spaces anywhere in the connection string. These parameters can be set either in the connection string or in a Properties object passed to the DriverManager.getConnection() method. For example:

```java 
Properties props = new Properties();
props.put("parameter1", parameter1Value);
props.put("parameter2", parameter2Value);
Connection con = DriverManager.getConnection("jdbc:databend://user:pass@host/database", props);
```
For the available connection parameters and their descriptions, see https://github.com/databendcloud/databend-jdbc/blob/main/docs/Connection.md#connection-parameters

## Examples

### Example: Creating a Database and Table

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

### Example: Batch Inserting

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

### Example: Uploading Files to an Internal Stage

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

Uploading CSV File to Databend:

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

### Example: Downloading Files from an Internal Stage

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

Downloading CSV File from Databend:
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