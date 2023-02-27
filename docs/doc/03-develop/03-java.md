---
title: How to Work With Databend in Java
sidebar_label: Java
description:
    How to work with Databend in Java.
---

## Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* [How to Create User](../14-sql-commands/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to User](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)

## Create Databend User

```shell
mysql -h127.0.0.1 -uroot -P3307
```

### Create a User

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

### Grants Privileges

Grants `ALL` privileges to the user `user1`:
```sql
GRANT ALL on *.* TO user1;
```

## Java

This topic shows how to connect and query Databend using JDBC. We will create a table named books, insert a row, and then query data from the table.

### Maven Dependency

```xml
<dependency>
    <groupId>com.databend</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>0.0.4</version>
</dependency>
```

### demo.java

```java title='demo.java'
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

        // Insert new book.
        String title = "mybook";
        String author = "author";
        String date = "2022";
        String add_book = "INSERT INTO books (title, author, date) VALUES ('" + title + "', '" + author + "', '" + date
                + "')";
        stmt.execute(add_book);

        // Select book
        String sql = "SELECT * FROM books";
        stmt.execute(sql);
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            String col1 = rs.getString("title");
            String col2 = rs.getString("author");
            String col3 = rs.getString("date");

            System.out.print("title: " + col1 + ", author: " + col2 + ", date: " + col3);
        }
        stmt.execute("drop table books");
        stmt.execute("drop database book_db");
        // Close conn
        conn.close();
        System.exit(0);
    }
}
```

### Run demo

```shell
$ mvn compile
$ mvn exec:java -D exec.mainClass="com.example.demo"
```

```text title='Outputs'
title: mybook, author: author, date: 2022
```
