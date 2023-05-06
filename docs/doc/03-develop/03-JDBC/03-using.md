---
title: Using the JDBC Driver
sidebar_label: Using
description:
  Using the JDBC Driver
---

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