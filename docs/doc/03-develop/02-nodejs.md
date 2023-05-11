---
title: Developing with Databend using Node.js
sidebar_label: Node.js
---

Databend enables you to develop Node.js programs that interact with Databend using the MySQL driver for Node.js. This driver provides an interface for connecting to Databend and performing operations such as executing SQL queries and retrieving results. With the MySQL driver, you can take advantage of the powerful distributed computing capabilities of Databend and build scalable data processing applications. Visit https://www.npmjs.com/package/mysql for more information about the driver.

To install the MySQL driver for Node.js:

```shell
npm install --save mysql
```
:::note
Before installing the driver, make sure to fulfill the following prerequisites:

- Node.js must already be installed on the environment where you want to install the driver.
- Ensure that you can run the `node` and `npm` commands.
- Depending on your environment, you may require sudo privileges to install the driver.
:::

## Tutorial: Developing with Databend using Node.js

Before you start, make sure you have successfully installed a local Databend. For detailed instructions, see [Local and Docker Deployments](../10-deploy/05-deploying-local.md).

### Step 1. Prepare a SQL User Account

To connect your program to Databend and execute SQL operations, you must provide a SQL user account with appropriate privileges in your code. Create one in Databend if needed, and ensure that the SQL user has only the necessary privileges for security.

This tutorial uses a SQL user named 'user1' with password 'abc123' as an example. As the program will write data into Databend, the user needs ALL privileges. For how to manage SQL users and their privileges, see https://databend.rs/doc/reference/sql/ddl/user.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

### Step 2. Write a Node.js Program

1. Copy and paste the following code to a file named `databend.js`:

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
      console.log("Database created");
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

   var sql = "INSERT INTO books VALUES('Readings in Database Systems', 'Michael Stonebraker', '2004')";
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

2. Run `nodejs databend.js`:

```text
Connected to Databend Server!
Database created
Table created
1 record inserted
[ RowDataPacket { title: 'Readings in Database Systems', author: 'Michael Stonebraker', date: '2004' } ]
```