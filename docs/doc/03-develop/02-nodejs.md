---
title: Developing with Databend using Node.js
sidebar_label: Node.js
description:
   Develop with Databend using Node.js.
---

In the following tutorial, you'll learn how to develop a Node.js application that communicates with Databend. The tutorial will walk you through creating a SQL user in Databend and then writing code to create a table, insert data, and perform data queries.

## Tutorial: Developing with Databend using Node.js

Before you start, make sure you have successfully installed Databend. For how to install Databend, see [How to deploy Databend](/doc/deploy).

### Step 1. Prepare a SQL User Account

To connect your program to Databend and execute SQL operations, you must provide a SQL user account with appropriate privileges in your code. Create one in Databend if needed, and ensure that the SQL user has only the necessary privileges for security.

This tutorial uses a SQL user named 'user1' with password 'abc123' as an example. As the program will write data into Databend, the user needs ALL privileges. For how to manage SQL users and their privileges, see https://databend.rs/doc/reference/sql/ddl/user.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

### Step 2. Write a Node.js Program

In this step, you'll create a simple Node.js program that communicates with Databend. The program will involve tasks such as creating a table, inserting data, and executing data queries.

1. Install the MySQL module and add it as a dependency in your Node.js project.

```text
npm install --save mysql
```

2. Copy and paste the following code to a file named `databend.js`:

```js title='databend.js'
const mysql = require('mysql');
const con = mysql.createConnection({
   host: 'localhost',
   port: 3307,
   user: 'user1',
   password: 'abc123',
});

con.connect((err) => {
   if (err) throw err;
   console.log('Connected to Databend Server!');

   var sql = "CREATE DATABASE IF NOT EXISTS book_db";
   con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("Dataabse created");
   });

   var sql = "USE book_db";
   con.query(sql, function (err, result) {
      if (err) throw err;
   });


   var sql = "CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)";
   con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("Table created");
   });

   var sql = "INSERT INTO books VALUES('mybook', 'author', '2022')";
   con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("1 record inserted");
   });

   con.query("SELECT * FROM books", function (err, result, fields) {
      if (err) throw err;
      console.log(result);
   });

});
```

3. Run `nodejs databend.js`:

```text
Connected to Databend Server!
Database created
Table created
1 record inserted
[ RowDataPacket { title: 'mybook', author: 'author', date: '2022' } ]
```