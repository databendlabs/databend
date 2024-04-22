# Databend Driver

Databend unified SQL client for RestAPI and FlightSQL

[![crates.io](https://img.shields.io/crates/v/databend-driver.svg)](https://crates.io/crates/databend-driver)
![License](https://img.shields.io/crates/l/databend-driver.svg)

## usage

### exec

```rust
use databend_driver::Client;

let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
let client = Client::new(dsn);
let conn = client.get_conn().await.unwrap();

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
let (title,author,date): (String,String,i32) = row.unwrap().try_into().unwrap();
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

## Type Mapping

[Databend Types](https://docs.databend.com/sql/sql-reference/data-types/)

### General Data Types

| Databend    | Rust                    |
| ----------- | ----------------------- |
| `BOOLEAN`   | `bool`                  |
| `TINYINT`   | `i8`,`u8`               |
| `SMALLINT`  | `i16`,`u16`             |
| `INT`       | `i32`,`u32`             |
| `BIGINT`    | `i64`,`u64`             |
| `FLOAT`     | `f32`                   |
| `DOUBLE`    | `f64`                   |
| `DECIMAL`   | `String`                |
| `DATE`      | `chrono::NaiveDate`     |
| `TIMESTAMP` | `chrono::NaiveDateTime` |
| `VARCHAR`   | `String`                |
| `BINARY`    | `Vec<u8>`               |

### Semi-Structured Data Types

| Databend      | Rust            |
| ------------- | --------------- |
| `ARRAY[T]`    | `Vec<T>`        |
| `TUPLE[T, U]` | `(T, U)`        |
| `MAP[K, V]`   | `HashMap<K, V>` |
| `VARIANT`     | `String`        |
| `BITMAP`      | `String`        |
| `GEOMETRY`    | `String`        |

Note: `VARIANT` is a json encoded string. Example:

```sql
CREATE TABLE example (
    data VARIANT
);
INSERT INTO example VALUES ('{"a": 1, "b": "hello"}');
```

```rust
let row = conn.query_row("SELECT * FROM example limit 1;").await.unwrap();
let (data,): (String,) = row.unwrap().try_into().unwrap();
let value: serde_json::Value = serde_json::from_str(&data).unwrap();
println!("{:?}", value);
```
