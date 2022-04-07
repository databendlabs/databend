---
title: How to Work with Databend in Node.js
sidebar_label: Node.js
description:
   How to Work with Databend in Node.js.
---

### Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* Install the mysql node module using the NPM: npm install --save mysql

### Node.js

This guideline show how to connect and query to Databend using Node.js.

We will be creating a table named `books` and insert a row, then query it.

```js
const mysql = require('mysql');
const con = mysql.createConnection({
   host: 'localhost',
   port: 3307,
   user: 'root',
   password: '',
});

con.connect((err) => {
   if (err) throw err;
   console.log('Connected to Databend Server!');

   var sql = "create database if not exists book_db";
   con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("Dataabse created");
   });

   var sql = "use book_db";
   con.query(sql, function (err, result) {
      if (err) throw err;
   });


   var sql = "create table if not exists books(title varchar(255), author varchar(255), date varchar(255))";
   con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("Table created");
   });

   var sql = "insert into books values('mybook', 'author', '2022')";
   con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("1 record inserted");
   });

   con.query("select * from books", function (err, result, fields) {
      if (err) throw err;
      console.log(result);
   });

});
```

The output:
```shell
Connected to MySQL Server!
Dataabse created
Table created
1 record inserted
[ RowDataPacket { title: 'mybook', author: 'author', date: '2022' } ]
```
