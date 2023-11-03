---
title: Developing with Databend using Node.js
sidebar_label: Node.js
---

import StepsWrap from '@site/src/components/StepsWrap';
import StepContent from '@site/src/components/Steps/step-content';

Databend enables you to develop Node.js programs that interact with Databend using Databend Driver Node.js Binding. This driver provides an interface for connecting to Databend and performing operations such as executing SQL queries and retrieving results. With the Databend driver, you can take advantage of the powerful distributed computing capabilities of Databend and build scalable data processing applications. Visit https://www.npmjs.com/package/databend-driver for more information about the driver.

To install the Databend driver for Node.js:

```shell
npm install --save databend-driver
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

<StepsWrap>

<StepContent number="1" title="Copy and paste the following code to a file named databend.js:">

```js title='databend.js'
const { Client } = require('databend-driver');

const dsn = process.env.DATABEND_DSN
    ? process.env.DATABEND_DSN
    : "databend://user1:abc123@localhost:8000/default?sslmode=disable";

async function create_conn() {
    this.client = new Client(dsn);
    this.conn = await this.client.getConn();
    console.log('Connected to Databend Server!');
}

async function select_books() {
    var sql = "CREATE DATABASE IF NOT EXISTS book_db";
    await this.conn.exec(sql);
    console.log("Database created");

    var sql = "USE book_db";
    await this.conn.exec(sql);
    console.log("Database used");

    var sql = "CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)";
    await this.conn.exec(sql);
    console.log("Table created");

    var sql = "INSERT INTO books VALUES('Readings in Database Systems', 'Michael Stonebraker', '2004')";
    await this.conn.exec(sql);
    console.log("1 record inserted");

    var sql = "SELECT * FROM books";
    const rows = await this.conn.queryIter(sql);
    const ret = [];
    let row = await rows.next();
    while (row) {
        ret.push(row.values());
        row = await rows.next();
    }
    console.log(ret);
}

create_conn().then(conn => {
    select_books()
});
```

</StepContent>

<StepContent number="2" title="Run node databend.js">

```text
Connected to Databend Server!
Database created
Database used
Table created
1 record inserted
[ [ 'Readings in Database Systems', 'Michael Stonebraker', '2004' ] ]
```

</StepContent>

</StepsWrap>

