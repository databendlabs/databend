---
title: Developing with Databend using Rust
sidebar_label: Rust
---

import StepsWrap from '@site/src/components/StepsWrap';
import StepContent from '@site/src/components/Steps/step-content';

Databend offers a driver (databend-driver) written in Rust, which facilitates the development of applications using the Rust programming language and establishes connectivity with Databend.

For installation instructions, examples, and the source code, see [GitHub - databend-driver](https://github.com/datafuselabs/BendSQL/tree/main/driver) or [crates.io - databend-driver](https://crates.io/crates/databend-driver) .

In the following tutorial, you'll learn how to utilize the driver `databend-driver` to develop your applications. The tutorial will walk you through creating a SQL user in Databend and then writing Rust code to create a table, insert data, and perform data queries.

## Tutorial: Developing with Databend using Rust

Before you start, make sure you have successfully installed a local Databend. For detailed instructions, see [Local and Docker Deployments](../10-deploy/05-deploying-local.md).

### Step 1. Prepare a SQL User Account

To connect your program to Databend and execute SQL operations, you must provide a SQL user account with appropriate privileges in your code. Create one in Databend if needed, and ensure that the SQL user has only the necessary privileges for security.

This tutorial uses a SQL user named 'user1' with password 'abc123' as an example. As the program will write data into Databend, the user needs ALL privileges. For how to manage SQL users and their privileges, see https://databend.rs/doc/reference/sql/ddl/user.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

### Step 2. Write a Rust Program

In this step, you'll create a simple Rust program that communicates with Databend. The program will involve tasks such as creating a table, inserting data, and executing data queries.

<StepsWrap>

<StepContent number="1" title="Create a new project">

```shell
cargo new databend-demo --bin
```

```toml title='Cargo.toml'
[package]
name = "databend-demo"
version = "0.1.0"
edition = "2021"

# See more keys and their definitions at https://doc.rust-lang.org/cargo/reference/manifest.html

[dependencies]
databend-driver = "0.7"
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1.12"
```


</StepContent>

<StepContent number="2" title="Copy and paste the following code to the file main.rs">


:::note
The value of `hostname` in the code below must align with your HTTP handler settings for Databend query service.
:::

```rust title='main.rs'
use databend_driver::Client;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let dsn = "databend://user1:abc123@localhost:8000/default?sslmode=disable";
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    let sql_db_create = "CREATE DATABASE IF NOT EXISTS book_db;";
    conn.exec(sql_db_create).await.unwrap();

    let sql_table_create = "CREATE TABLE book_db.books (
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);";

    conn.exec(sql_table_create).await.unwrap();
    let sql_insert = "INSERT INTO book_db.books VALUES ('mybook', 'author', '2022');";
    conn.exec(sql_insert).await.unwrap();

    let mut rows = conn.query_iter("SELECT * FROM book_db.books;").await.unwrap();
    while let Some(row) = rows.next().await {
        let (title, author, date): (String, String, String) = row.unwrap().try_into().unwrap();
        println!("{} {} {}", title, author, date);
    }

    let sql_table_drop = "DROP TABLE book_db.books;";
    conn.exec(sql_table_drop).await.unwrap();

    let sql_db_drop = "DROP DATABASE book_db;";
    conn.exec(sql_db_drop).await.unwrap();
}
```


</StepContent>

<StepContent number="3" title="Run the program. ">

```shell
cargo run
```

```text title='Outputs'
mybook author 2022
```

</StepContent>

</StepsWrap>


