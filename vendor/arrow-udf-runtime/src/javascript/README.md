# JavaScript UDF for Apache Arrow

## Usage

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-runtime = "0.7"
```

Create a `Runtime` and define your JS functions in string form.
Note that the function must be exported and its name must match the one you pass to `add_function`.

```rust,ignore
use arrow_udf_runtime::javascript::{FunctionOptions, Runtime};

let mut runtime = Runtime::new().await?;
runtime
    .add_function(
        "gcd",
        arrow_schema::DataType::Int32,
        r#"
        export function gcd(a, b) {
            while (b != 0) {
                let t = b;
                b = a % b;
                a = t;
            }
            return a;
        }
        "#,
        FunctionOptions::default().return_null_on_null_input(),
    )
    .await?;
```

You can then call the JS function on a `RecordBatch`:

```rust,ignore
let input: RecordBatch = ...;
let output: RecordBatch = runtime.call("gcd", &input).await?;
```

If you print the input and output batch, it will be like this:

```text
 input     output
+----+----+-----+
| a  | b  | gcd |
+----+----+-----+
| 15 | 25 | 5   |
|    | 1  |     |
+----+----+-----+
```

For set-returning functions (or so-called table functions), define the function as a generator:

```rust,ignore
use arrow_udf_runtime::javascript::{FunctionOptions, Runtime};

let mut runtime = Runtime::new().await?;
runtime
    .add_function(
        "range",
        arrow_schema::DataType::Int32,
        r#"
        export function* range(n) {
            for (let i = 0; i < n; i++) {
                yield i;
            }
        }
        "#,
        FunctionOptions::default().return_null_on_null_input(),
    )
    .await?;
```

You can then call the table function via `call_table_function`:

```rust,ignore
let chunk_size = 1024;
let input: RecordBatch = ...;
let outputs = runtime.call_table_function("range", &input, chunk_size)?;
while let Some(output) = outputs.next().await {
    // do something with the output
}
```

If you print the output batch, it will be like this:

```text
+-----+-------+
| row | range |
+-----+-------+
| 0   | 0     |
| 2   | 0     |
| 2   | 1     |
| 2   | 2     |
+-----+-------+
```

The JS code will be run in an embedded QuickJS interpreter.

See the [example](examples/js.rs) for more details.

## Type Mapping

The following table shows the type mapping between Arrow and JavaScript:

| Arrow Type            | JS Type        |
| --------------------- | -------------- |
| Null                  | null           |
| Boolean               | boolean        |
| Int8                  | number         |
| Int16                 | number         |
| Int32                 | number         |
| Int64                 | number         |
| UInt8                 | number         |
| UInt16                | number         |
| UInt32                | number         |
| UInt64                | number         |
| Float32               | number         |
| Float64               | number         |
| String                | string         |
| LargeString           | string         |
| Date32                | Date           |
| Timestamp             | Date           |
| Decimal128            | BigDecimal     |
| Decimal256            | BigDecimal     |
| Binary                | Uint8Array     |
| LargeBinary           | Uint8Array     |
| List(Int8)            | Int8Array      |
| List(Int16)           | Int16Array     |
| List(Int32)           | Int32Array     |
| List(Int64)           | BigInt64Array  |
| List(UInt8)           | Uint8Array     |
| List(UInt16)          | Uint16Array    |
| List(UInt32)          | Uint32Array    |
| List(UInt64)          | BigUint64Array |
| List(Float32)         | Float32Array   |
| List(Float64)         | Float64Array   |
| List(others)          | Array          |
| Struct                | object         |

This crate also supports the following [Arrow extension types](https://arrow.apache.org/docs/format/Columnar.html#extension-types):

| Extension Type | Physical Type               | `ARROW:extension:name` | JS Type       |
| -------------- | --------------------------- | ---------------------- | ------------- |
| JSON           | String, Binary, LargeBinary | `arrowudf.json`        | any (parsed by `JSON.parse(string)`) |
| Decimal        | String                      | `arrowudf.decimal`     | BigDecimal    |

## Async Functions and Fetch API

An async function is a JavaScript function that returns a promise. If the function involves IO operations, it's usually more efficient to use async functions.

We provide a [Fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API) to allow making HTTP requests from JavaScript UDFs in async way. To use it, you need to enable it in the `Runtime`:

```rust,ignore
runtime.enable_fetch();
```

To enable async functions, you need to set `async_mode()` when adding the function. Then you can use the async `fetch()` function in your JavaScript code.

```rust,ignore
runtime
    .add_function(
        "echo",
        DataType::Utf8View,
        r#"
export async function my_fetch_udf(id) {
    const response = await fetch("https://api.example.com/" + id);
    const data = await response.json();
    return data.value;
}
"#,
        FunctionOptions::default().return_null_on_null_input().async_mode(),
    )
    .await
    .unwrap();
```

See the [README](src/fetch/README.md) of the `fetch` module for more details.

## Batched Function

When a function is batched, it will be called once for all rows in the input `RecordBatch`. The input arguments will be an array of values.


```rust,ignore
runtime
    .add_function(
        "echo",
        DataType::Utf8View,
        r#"
export function echo(vals) {
    return vals.map(v => v + "!")
}
"#,
        FunctionOptions::default().return_null_on_null_input().batched(),
    )
    .await
    .unwrap();
```

Currently, table functions can not be batched.
