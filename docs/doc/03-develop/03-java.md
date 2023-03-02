---
title: Developing with Databend using Java
sidebar_label: Java
description:
    Develop with Databend using Java.
---

Databend offers a driver (databend-jdbc) written in Java, which facilitates the development of applications using the Java programming language and establishes connectivity with Databend.

For installation instructions, examples, and the source code, see the GitHub [databend-jdbc](https://github.com/databendcloud/databend-jdbc) repo.

In the following tutorial, you'll learn how to utilize the driver `databend-jdbc` to develop your applications. The tutorial will walk you through creating a SQL user in Databend and then writing Java code to create a table, insert data, and perform data queries.

## Tutorial: Developing with Databend using Java

Before you start, make sure you have successfully installed Databend. For how to install Databend, see [How to deploy Databend](/doc/deploy).

### Step 1. Prepare a SQL User Account

To connect your program to Databend and execute SQL operations, you must provide a SQL user account with appropriate privileges in your code. Create one in Databend if needed, and ensure that the SQL user has only the necessary privileges for security.

This tutorial uses a SQL user named 'user1' with password 'abc123' as an example. As the program will write data into Databend, the user needs ALL privileges. For how to manage SQL users and their privileges, see https://databend.rs/doc/reference/sql/ddl/user.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

### Step 2. Write a Java Program

In this step, you'll create a simple Java program that communicates with Databend. The program will involve tasks such as creating a table, inserting data, and executing data queries.

1. Declare a Maven dependency.

```xml
<dependency>
    <groupId>com.databend</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>0.0.4</version>
</dependency>
```

2. Copy and paste the following code to a file named `demo.java`:

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

3. Compile and run the program:

```shell
$ mvn compile
$ mvn exec:java -D exec.mainClass="com.example.demo"
```

```text title='Outputs'
title: mybook, author: author, date: 2022
```
