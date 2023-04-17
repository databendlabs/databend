# Databend Driver

Databend unified SQL client for RestAPI and FlightSQL

[![crates.io](https://img.shields.io/crates/v/databend-driver.svg)](https://crates.io/crates/databend-driver)
![License](https://img.shields.io/crates/l/databend-driver.svg)

## usage

### exec

```rust
use databend_driver::new_connection;

let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
let conn = new_connection(dsn).unwrap();

let sql_create = "CREATE TABLE books (
    title VARCHAR,
    author VARCHAR,
    date Date
);";
conn.exec(sql_create).await.unwrap();
let sql_insert = "INSERT INTO books VALUES ('The Little Prince', 'Antoine de Saint-Exupéry', '1943-04-06');";
conn.exec(sql_insert).await.unwrap();
```

### query row

```rust
let row = conn.query_row("SELECT * FROM books;").await.unwrap();
let (title,author,date): (String,String,i32) = row.try_into().unwrap();
println!("{} {} {}", title, author, date);
```

### query iter

```rust
let mut rows = conn.query_iter("SELECT * FROM books;").await.unwrap();
while let Some(row) = rows.next().await {
    let (title,author,date): (String,String,chrono::NaiveDate) = row.unwrap().try_into().unwrap();
    println!("{} {} {}", title, author, date);
}
```
