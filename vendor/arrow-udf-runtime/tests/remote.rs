// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(feature = "remote-runtime")]

use std::{collections::HashSet, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_runtime::remote::Client;
use expect_test::{expect, Expect};
use futures_util::StreamExt;

const SERVER_ADDR: &str = "http://localhost:8815";

#[tokio::test]
async fn test_gcd() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();

    // build `RecordBatch` to send
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from_iter(vec![1, 6, 10]);
    let arg1 = Int32Array::from_iter(vec![3, 4, 15]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = client.call("gcd", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+
        | gcd |
        +-----+
        | 1   |
        | 2   |
        | 5   |
        +-----+"#]],
    );
}

#[tokio::test]
async fn test_decimal_add() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();

    let schema = Schema::new(vec![decimal_field("a"), decimal_field("b")]);
    let arg0 = StringArray::from(vec!["0.0001"]);
    let arg1 = StringArray::from(vec!["0.0002"]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = client.call("decimal_add", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-------------+
        | decimal_add |
        +-------------+
        | 0.0003      |
        +-------------+"#]],
    );
}

#[tokio::test]
async fn test_json_array_access() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();

    let schema = Schema::new(vec![
        json_field("array"),
        Field::new("i", DataType::Int32, true),
    ]);
    let arg0 = StringArray::from(vec![r#"[1, null, ""]"#]);
    let arg1 = Int32Array::from(vec![0]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = client.call("json_array_access", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-------------------+
        | json_array_access |
        +-------------------+
        | 1                 |
        +-------------------+"#]],
    );
}

#[tokio::test]
async fn test_range() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = client.call_table_function("range", &input).await.unwrap();

    let output = outputs.next().await.unwrap().unwrap();
    assert_eq!(output.schema().field(0).data_type(), &DataType::Int32);

    check(
        &[output],
        expect![[r#"
        +-----+-------+
        | row | range |
        +-----+-------+
        | 0   | 0     |
        | 2   | 0     |
        | 2   | 1     |
        | 2   | 2     |
        +-----+-------+"#]],
    );
}

#[tokio::test]
async fn test_get_protocol_version() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();
    assert_eq!(client.protocol_version(), 2);
}

#[tokio::test]
async fn test_get() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();

    let function = client.get("gcd").await.unwrap();

    assert_eq!(function.name, "gcd");
    assert_eq!(function.args.field(0).data_type(), &DataType::Int32);
    assert_eq!(function.args.field(1).data_type(), &DataType::Int32);
    assert_eq!(function.returns.field(0).data_type(), &DataType::Int32);
}

#[tokio::test]
async fn test_list_function() {
    let client = Client::connect(SERVER_ADDR).await.unwrap();

    let functions = client.list().await.unwrap();
    let names = functions
        .into_iter()
        .map(|f| f.name)
        .collect::<HashSet<_>>();

    assert!(names.contains("gcd"));
    assert!(names.contains("range"));
    assert!(names.contains("decimal_add"));
    assert!(names.contains("json_array_access"));
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}

/// Returns a field with JSON type.
fn json_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with decimal type.
fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
}
