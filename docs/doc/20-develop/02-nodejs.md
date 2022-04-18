---
title: How to Work with Databend in Node.js
sidebar_label: Node.js
description:
   How to Work with Databend in Node.js.
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

```sql title='mysql>'
create user user1 identified by 'abc123';
```

### Grants Privileges

Grants `ALL` privileges to the user `user1`:
```sql title='mysql>'
grant all on *.* to 'user1';
```

## Node.js

This guideline show how to connect and query to Databend using Node.js.

We will be creating a table named `books` and insert a row, then query it.

```text
npm install --save mysql
```

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

   con.query("SELECT * from books", function (err, result, fields) {
      if (err) throw err;
      console.log(result);
   });

});
```

Run `nodejs databend.js`:

```text
Connected to Databend Server!
Dataabse created
Table created
1 record inserted
[ RowDataPacket { title: 'mybook', author: 'author', date: '2022' } ]
```
