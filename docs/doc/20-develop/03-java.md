---
title: How to Work With Databend in Java
sidebar_label: Java
description:
    How to work with Databend in Java.
---

## Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* [How to Create User](../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to User](../30-reference/30-sql/00-ddl/30-user/10-grant-privileges.md)

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

### demo.java

```java title='demo.java'
import java.sql.*;

public class demo {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://127.0.0.1:3307/default";

    static final String USER = "user1";
    static final String PASS = "abc123";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            stmt = conn.createStatement();
            String create_sql = "CREATE DATABASE IF NOT EXISTS book_db";
            int rs1 = stmt.executeUpdate(create_sql);

            String use_sql = "USE book_db";
            int rs2 = stmt.executeUpdate(use_sql);

            String ct_sql = "CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)";
            int rs3 = stmt.executeUpdate(ct_sql);


            // Insert new book.
            String title = "mybook";
            String author = "author";
            String date = "2022";
            String add_book = "INSERT INTO books (title, author, date) VALUES ('"+ title +"', '"+ author +"', '" + date + "')";
            int rs4 = stmt.executeUpdate(add_book);


            // Select book
            String sql = "SELECT * FROM books";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String col1 = rs.getString("title");
                String col2 = rs.getString("author");
                String col3 = rs.getString("date");

                System.out.print("title: " + col1 + ", author: " + col2 + ", date: " + col3);
            }
            // Close conn
            rs.close();
            stmt.close();
            conn.close();
        } catch(SQLException se) {
            // throw JDBC err
            se.printStackTrace();
        } catch(Exception e) {
            // throw Class.forName err
            e.printStackTrace();
        } finally {
            // Close source
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try{
                if (conn!=null) conn.close();
            } catch(SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
```

### Run demo.java

In this case:

The demo classpath is located at /home/eason/database/source-code/test/out/test/test

The demo classpath is located at /home/eason/Downloads/jar_files/mysql-connector-java-5.1.48.jar

```shell
$ ~/.jdks/openjdk-17.0.1/bin/java -Dfile.encoding=UTF-8 -classpath /home/eason/database/source-code/test/out/test/test:/home/eason/Downloads/jar_files/mysql-connector-java-5.1.48.jar demo
title: mybook, author: author, date: 2022
```

```text title='Outputs'
title: mybook, author: author, date: 2022
```
