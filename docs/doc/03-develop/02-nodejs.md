---
title: Developing with Databend using Node.js
sidebar_label: Node.js
description:
  Develop with Databend using Node.js.
---

## Installing
This topic describes how to install the Node.js driver using npm, the default package manager for the Node.js JavaScript runtime environment.

## Prerequisites
- Node.js must already be installed in the environment where you wish to install the driver.

- You need to be able to run the node and npm commands.

- Depending on your environment, you may need sudo privileges.

## Installing the Driver
```shell
npm install --save mysql
```

## Connect and execute SQL

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

2. Run `nodejs databend.js`:

```text
Connected to Databend Server!
Database created
Table created
1 record inserted
[ RowDataPacket { title: 'mybook', author: 'author', date: '2022' } ]
