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

#![cfg(feature = "wasm-runtime")]

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Fields, Schema};
use arrow_udf_runtime::wasm::Runtime;
use expect_test::{expect, Expect};

const BINARY_PATH: &str = "../target/wasm32-wasip1/release/arrow_udf_example.wasm";

static RUNTIME: std::sync::LazyLock<Runtime> = std::sync::LazyLock::new(|| {
    Runtime::new(&std::fs::read(BINARY_PATH).expect("failed to load wasm binary"))
        .expect("failed to create wasm runtime")
});

#[test]
fn test_oom() {
    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let oom = RUNTIME
        .find_function("oom", Vec::<Field>::new(), DataType::Null)
        .unwrap();
    let output = RUNTIME.call(&oom, &input);
    // panic message should be contained in the error message
    assert!(output
        .unwrap_err()
        .to_string()
        .contains("capacity overflow"));
}

#[test]
#[ignore = "FIXME: sleep should not be allowed"]
fn test_sleep() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let sleep = RUNTIME
        .find_function("sleep", vec![DataType::Int32], DataType::Int32)
        .unwrap();
    let output = RUNTIME.call(&sleep, &input);
    output.unwrap_err();
}

#[test]
fn test_gcd() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(15), Some(5), None])),
            Arc::new(Int32Array::from(vec![25, 0, 1])),
        ],
    )
    .unwrap();

    let gcd = RUNTIME
        .find_function(
            "gcd",
            vec![DataType::Int32, DataType::Int32],
            DataType::Int32,
        )
        .unwrap();
    let output = RUNTIME.call(&gcd, &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+
        | gcd |
        +-----+
        | 5   |
        | 5   |
        |     |
        +-----+"#]],
    );
}

#[test]
fn test_division_by_zero() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(15), Some(5), None])),
            Arc::new(Int32Array::from(vec![25, 0, 1])),
        ],
    )
    .unwrap();

    let div = RUNTIME
        .find_function(
            "div",
            vec![DataType::Int32, DataType::Int32],
            DataType::Int32,
        )
        .unwrap();
    let output = RUNTIME.call(&div, &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+------------------+
        | div | error            |
        +-----+------------------+
        | 0   |                  |
        |     | division by zero |
        |     |                  |
        +-----+------------------+"#]],
    );
}

#[test]
fn test_length() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["rising", "wave"]))],
    )
    .unwrap();

    let length = RUNTIME
        .find_function("length", vec![DataType::Utf8], DataType::Int32)
        .unwrap();
    let output = RUNTIME.call(&length, &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------+
        | length |
        +--------+
        | 6      |
        | 4      |
        +--------+"#]],
    );
}

#[test]
fn test_key_value() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["rising=wave", "???"]))],
    )
    .unwrap();

    let key_value = RUNTIME
        .find_function(
            "key_value",
            vec![DataType::Utf8],
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ])),
        )
        .unwrap();
    let output = RUNTIME.call(&key_value, &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------------------+
        | key_value                  |
        +----------------------------+
        | {key: rising, value: wave} |
        |                            |
        +----------------------------+"#]],
    );
}

#[test]
fn test_range() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
    )
    .unwrap();

    let range = RUNTIME
        .find_table_function("range", vec![DataType::Int32], DataType::Int32)
        .unwrap();
    let mut iter = RUNTIME.call_table_function(&range, &input).unwrap();
    let output = iter.next().unwrap().unwrap();
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

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}
