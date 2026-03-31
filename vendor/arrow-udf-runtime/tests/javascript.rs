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

#![cfg(feature = "javascript-runtime")]

use std::{sync::Arc, time::Duration};

use arrow_array::builder::{MapBuilder, StringBuilder};
use arrow_array::Array;
use arrow_array::{
    types::*, ArrayRef, BinaryArray, Date32Array, Decimal128Array, Decimal256Array, Int32Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, RecordBatch, StringArray,
    StringViewArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::i256;
use arrow_cast::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_schema::{DataType, Field, Fields, Schema};
use arrow_udf_runtime::javascript::{AggregateOptions, FunctionOptions, Runtime};
use expect_test::{expect, Expect};
use rquickjs::prelude::Async;
use rquickjs::{async_with, Function};

#[tokio::test]
async fn test_gcd() {
    let mut runtime = Runtime::new().await.unwrap();

    let js_code = r#"
        export function gcd(a, b) {
            while (b != 0) {
                let t = b;
                b = a % b;
                a = t;
            }
            return a;
        }
    "#;
    runtime
        .add_function(
            "gcd",
            DataType::Int32,
            js_code,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(25), None]);
    let arg1 = Int32Array::from(vec![Some(15), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("gcd", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+
        | gcd |
        +-----+
        | 5   |
        |     |
        +-----+"#]],
    );
}

#[tokio::test]
async fn test_to_string() {
    let mut runtime = Runtime::new().await.unwrap();

    let js_code = r#"
        export function to_string(a) {
            if (a == null) {
                return "null";
            }
            return a.toString();
        }
    "#;
    runtime
        .add_function(
            "to_string",
            DataType::Utf8,
            js_code,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(5), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_string", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-----------+
        | to_string |
        +-----------+
        | 5         |
        | null      |
        +-----------+"#]],
    );
}

#[tokio::test]
async fn test_concat() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "concat",
            DataType::Binary,
            r#"
            export function concat(a, b) {
                return a.concat(b);
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Binary, true),
        Field::new("b", DataType::Binary, true),
    ]);
    let arg0 = BinaryArray::from(vec![&b"hello"[..]]);
    let arg1 = BinaryArray::from(vec![&b"world"[..]]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("concat", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------------+
        | concat               |
        +----------------------+
        | 68656c6c6f776f726c64 |
        +----------------------+"#]],
    );
}

#[tokio::test]
async fn test_json_array_access() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "json_array_access",
            json_field("json"),
            r#"
            export function json_array_access(array, i) {
                return array[i];
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        json_field("array"),
        Field::new("i", DataType::Int32, true),
    ]);
    let arg0 = StringArray::from(vec![r#"[1, null, ""]"#]);
    let arg1 = Int32Array::from(vec![0]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("json_array_access", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
            +------+
            | json |
            +------+
            | 1    |
            +------+"#]],
    );
}

#[tokio::test]
async fn test_json_stringify() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "json_stringify",
            DataType::Utf8,
            r#"
            export function json_stringify(object) {
                return JSON.stringify(object);
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![json_field("json")]);
    let arg0 = StringArray::from(vec![r#"[1, null, ""]"#]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("json_stringify", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | json_stringify |
        +----------------+
        | [1,null,""]    |
        +----------------+"#]],
    );
}

#[tokio::test]
async fn test_binary_json_stringify() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "add_element",
            binary_json_field("object"),
            r#"
            export function add_element(object) {
                object.push(10);
                return object;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![binary_json_field("json")]);
    let arg0 = BinaryArray::from(vec![(r#"[1, null, ""]"#).as_bytes()]);
    let input = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("add_element", &input).await.unwrap();
    let row = output
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(std::str::from_utf8(row).unwrap(), r#"[1,null,"",10]"#);
}

#[tokio::test]
async fn test_large_binary_json_stringify() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "add_element",
            large_binary_json_field("object"),
            r#"
            export function add_element(object) {
                object.push(10);
                return object;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![large_binary_json_field("json")]);
    let arg0 = LargeBinaryArray::from(vec![(r#"[1, null, ""]"#).as_bytes()]);
    let input = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("add_element", &input).await.unwrap();
    let row = output
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(std::str::from_utf8(row).unwrap(), r#"[1,null,"",10]"#);
}

#[tokio::test]
async fn test_large_string_as_string() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "string_length",
            DataType::LargeUtf8,
            r#"
            export function string_length(s) {
                return "string length is " + s.length;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]);
    let arg0 = LargeStringArray::from(vec![r#"hello"#]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("string_length", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +--------------------+
        | string_length      |
        +--------------------+
        | string length is 5 |
        +--------------------+"#]],
    );
}

#[tokio::test]
async fn test_decimal128() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "decimal128_add",
            DataType::Decimal128(19, 2),
            r#"
            export function decimal128_add(a, b) {
                return a + b + BigDecimal('0.000001');
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Decimal128(19, 2), true),
        Field::new("b", DataType::Decimal128(19, 2), true),
    ]);
    let arg0 = Decimal128Array::from(vec![Some(100), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let arg1 = Decimal128Array::from(vec![Some(201), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal128_add", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | decimal128_add |
        +----------------+
        | 3.01           |
        |                |
        +----------------+"#]],
    );
}

#[tokio::test]
async fn test_decimal256() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "decimal256_add",
            DataType::Decimal256(19, 2),
            r#"
            export function decimal256_add(a, b) {
                return a + b + BigDecimal('0.000001');
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Decimal256(19, 2), true),
        Field::new("b", DataType::Decimal256(19, 2), true),
    ]);
    let arg0 = Decimal256Array::from(vec![Some(i256::from(100)), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let arg1 = Decimal256Array::from(vec![Some(i256::from(201)), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal256_add", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | decimal256_add |
        +----------------+
        | 3.01           |
        |                |
        +----------------+"#]],
    );
}

#[tokio::test]
async fn test_decimal_add() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "decimal_add",
            decimal_field("add"),
            r#"
            export function decimal_add(a, b) {
                return a + b;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![decimal_field("a"), decimal_field("b")]);
    let arg0 = StringArray::from(vec!["0.0001"]);
    let arg1 = StringArray::from(vec!["0.0002"]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal_add", &input).await.unwrap();
    assert_eq!(output.schema().field(0), &decimal_field("add"));
    check(
        &[output],
        expect![[r#"
            +--------+
            | add    |
            +--------+
            | 0.0003 |
            +--------+"#]],
    );
}

#[tokio::test]
async fn test_timestamp_second_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
        true,
    )]);
    let arg0 = TimestampSecondArray::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------+
        | timestamp_array     |
        +---------------------+
        | 1970-01-01T00:00:01 |
        |                     |
        | 1970-01-01T00:00:03 |
        +---------------------+"#]],
    );
}

#[tokio::test]
async fn test_timestamp_millisecond_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        true,
    )]);
    let arg0 = TimestampMillisecondArray::from(vec![Some(1000), None, Some(3000)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------+
        | timestamp_array     |
        +---------------------+
        | 1970-01-01T00:00:01 |
        |                     |
        | 1970-01-01T00:00:03 |
        +---------------------+"#]],
    );
}

#[tokio::test]
async fn test_timestamp_microsecond_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        true,
    )]);
    let arg0 = TimestampMicrosecondArray::from(vec![Some(1000000), None, Some(3000000)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------+
        | timestamp_array     |
        +---------------------+
        | 1970-01-01T00:00:01 |
        |                     |
        | 1970-01-01T00:00:03 |
        +---------------------+"#]],
    );
}

#[tokio::test]
async fn test_timestamp_nanosecond_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        true,
    )]);
    let arg0 = TimestampNanosecondArray::from(vec![Some(1000000), None, Some(3000000)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-------------------------+
        | timestamp_array         |
        +-------------------------+
        | 1970-01-01T00:00:00.001 |
        |                         |
        | 1970-01-01T00:00:00.003 |
        +-------------------------+"#]],
    );
}

#[tokio::test]
async fn test_date32_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "date_array",
            DataType::Date32,
            r#"
            export function date_array(a) {
                return a;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Date32, true)]);
    let arg0 = Date32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("date_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +------------+
        | date_array |
        +------------+
        | 1970-01-02 |
        |            |
        | 1970-01-04 |
        +------------+"#]],
    );
}

#[tokio::test]
async fn test_typed_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "object_type",
            DataType::Utf8,
            r#"
            export function object_type(a) {
                return Object.prototype.toString.call(a);
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    /// Generate a record batch with a single column of type `List<T>`.
    fn array_input<T: ArrowPrimitiveType>() -> RecordBatch {
        let schema = Schema::new(vec![Field::new(
            "x",
            DataType::new_list(T::DATA_TYPE, true),
            true,
        )]);
        let arg0 =
            ListArray::from_iter_primitive::<T, _, _>(vec![Some(vec![Some(Default::default())])]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap()
    }

    let cases = [
        // (input, JS object type)
        (array_input::<Int8Type>(), "Int8Array"),
        (array_input::<Int16Type>(), "Int16Array"),
        (array_input::<Int32Type>(), "Int32Array"),
        (array_input::<Int64Type>(), "BigInt64Array"),
        (array_input::<UInt8Type>(), "Uint8Array"),
        (array_input::<UInt16Type>(), "Uint16Array"),
        (array_input::<UInt32Type>(), "Uint32Array"),
        (array_input::<UInt64Type>(), "BigUint64Array"),
        (array_input::<Float32Type>(), "Float32Array"),
        (array_input::<Float64Type>(), "Float64Array"),
    ];

    for (input, expected) in cases.iter() {
        let output = runtime.call("object_type", input).await.unwrap();
        let object_type = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(object_type, format!("[object {}]", expected));
    }
}

#[tokio::test]
async fn test_arg_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "from_array",
            DataType::Int32,
            r#"
            export function from_array(x) {
                if(x == null) {
                    return null;
                }
                if(x.length > 0) {
                    return x[0];
                }
                return null;
            }
            "#,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::new_list(DataType::Int32, true),
        true,
    )]);
    let arg0 = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some([Some(1), Some(2)]),
        Some([None, Some(3)]),
        Some([Some(4), Some(5)]),
        None,
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("from_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +------------+
        | from_array |
        +------------+
        | 1          |
        |            |
        | 4          |
        |            |
        +------------+"#]],
    );
}

#[tokio::test]
async fn test_return_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "to_array",
            DataType::new_list(DataType::Int32, true),
            r#"
            export function to_array(x) {
                if(x == null) {
                    return null;
                }
                return [x];
            }
            "#,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------+
        | to_array |
        +----------+
        | [1]      |
        |          |
        | [3]      |
        +----------+"#]],
    );
}

#[tokio::test]
async fn test_arg_large_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "from_large_array",
            DataType::Int32,
            r#"
            export function from_large_array(x) {
                if(x == null) {
                    return null;
                }
                if(x.length > 0) {
                    return x[0];
                }
                return null;
            }
            "#,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::new_large_list(DataType::Int32, true),
        true,
    )]);
    let arg0 = LargeListArray::from_iter_primitive::<Int32Type, _, _>([
        Some([Some(1), Some(2)]),
        Some([None, Some(3)]),
        Some([Some(4), Some(5)]),
        None,
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("from_large_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +------------------+
        | from_large_array |
        +------------------+
        | 1                |
        |                  |
        | 4                |
        |                  |
        +------------------+"#]],
    );
}

#[tokio::test]
async fn test_return_large_array() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "to_large_array",
            DataType::new_large_list(DataType::Int32, true),
            r#"
            export function to_large_array(x) {
                if(x == null) {
                    return null;
                }
                return [x, x+1, x+2];
            }
            "#,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_large_array", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | to_large_array |
        +----------------+
        | [1, 2, 3]      |
        |                |
        | [3, 4, 5]      |
        +----------------+"#]],
    );
}

#[tokio::test]
async fn test_arg_map() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "from_map",
            DataType::Utf8,
            r#"
            export function from_map(x) {
                if(x == null) {
                    return null;
                }
                if(x.hasOwnProperty('k')) {
                    return x['k'];
                }
                return null;
            }
            "#,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let mut builder =
        MapBuilder::with_capacity(None, StringBuilder::new(), StringBuilder::new(), 3);
    builder.keys().append_value("k");
    builder.values().append_value("v");
    builder.append(true).unwrap();
    builder.keys().append_value("k1");
    builder.values().append_value("v1");
    builder.keys().append_value("k2");
    builder.values().append_value("v2");
    builder.append(true).unwrap();
    builder.append(false).unwrap();
    let arg0 = builder.finish();

    let data_type = arg0.data_type().clone();
    let schema = Schema::new(vec![Field::new("x", data_type, true)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("from_map", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------+
        | from_map |
        +----------+
        | v        |
        |          |
        |          |
        +----------+"#]],
    );
}

#[tokio::test]
async fn test_return_map() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "to_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, true),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            r#"
            export function to_map(x, y) {
                if(x == null || y == null) {
                    return null;
                }
                return {k1:x,k2:y};
            }
            "#,
            FunctionOptions::default(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Utf8, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    let arg0 = StringArray::from(vec![Some("ab"), None, Some("c")]);
    let arg1 = StringArray::from(vec![Some("xy"), None, Some("z")]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("to_map", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +------------------+
        | to_map           |
        +------------------+
        | {k1: ab, k2: xy} |
        |                  |
        | {k1: c, k2: z}   |
        +------------------+"#]],
    );
}

#[tokio::test]
async fn test_key_value() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "key_value",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            r#"
            export function key_value(s) {
                const [key, value] = s.split("=", 2);
                return {key, value};
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a=b"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("key_value", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +--------------------+
        | key_value          |
        +--------------------+
        | {key: a, value: b} |
        +--------------------+"#]],
    );
}

#[tokio::test]
async fn test_struct_to_json() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "to_json",
            json_field("to_json"),
            r#"
            export function to_json(object) {
                return object;
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    )]);
    let arg0 = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("b"), None])),
        ),
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_json", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------------+
        | to_json                   |
        +---------------------------+
        | {"key":"a","value":"b"}   |
        | {"key":null,"value":null} |
        +---------------------------+"#]],
    );
}

#[tokio::test]
async fn test_range() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            r#"
            export function* range(n) {
                for (let i = 0; i < n; i++) {
                    yield i;
                }
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range", &input, 2).unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.next().await.unwrap().unwrap();
    let o2 = outputs.next().await.unwrap().unwrap();
    assert_eq!(o1.num_rows(), 2);
    assert_eq!(o2.num_rows(), 2);
    assert!(outputs.next().await.unwrap().is_none());

    check(
        &[o1, o2],
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
async fn test_weighted_avg() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime
        .add_aggregate(
            "weighted_avg",
            DataType::Struct(
                vec![
                    Field::new("sum", DataType::Int32, false),
                    Field::new("weight", DataType::Int32, false),
                ]
                .into(),
            ),
            DataType::Float32,
            r#"
            export function create_state() {
                return {sum: 0, weight: 0};
            }
            export function accumulate(state, value, weight) {
                state.sum += value * weight;
                state.weight += weight;
                return state;
            }
            export function retract(state, value, weight) {
                state.sum -= value * weight;
                state.weight -= weight;
                return state;
            }
            export function merge(state1, state2) {
                state1.sum += state2.sum;
                state1.weight += state2.weight;
                return state1;
            }
            export function finish(state) {
                return state.sum / state.weight;
            }
"#,
            AggregateOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("weight", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    let arg1 = Int32Array::from(vec![Some(2), None, Some(4), Some(6)]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let state = runtime.create_state("weighted_avg").await.unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +---------------------+
            | array               |
            +---------------------+
            | {sum: 0, weight: 0} |
            +---------------------+"#]],
    );

    let state = runtime
        .accumulate("weighted_avg", &state, &input)
        .await
        .unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-----------------------+
            | array                 |
            +-----------------------+
            | {sum: 44, weight: 12} |
            +-----------------------+"#]],
    );

    let states = arrow_select::concat::concat(&[&state, &state]).unwrap();
    let state = runtime.merge("weighted_avg", &states).await.unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-----------------------+
            | array                 |
            +-----------------------+
            | {sum: 88, weight: 24} |
            +-----------------------+"#]],
    );

    let output = runtime.finish("weighted_avg", &state).await.unwrap();
    check_array(
        &[output],
        expect![[r#"
            +-----------+
            | array     |
            +-----------+
            | 3.6666667 |
            +-----------+"#]],
    );
}

#[tokio::test]
async fn test_timeout() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime.set_timeout(Some(Duration::from_millis(1))).await;

    let js_code = r#"
        export function square(x) {
            let sum = 0;
            for (let i = 0; i < x; i++) {
                sum += x;
            }
            return sum;
        }
    "#;
    runtime
        .add_function(
            "square",
            DataType::Int32,
            js_code,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![100]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("square", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +--------+
        | square |
        +--------+
        | 10000  |
        +--------+"#]],
    );

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![i32::MAX]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let err = runtime.call("square", &input).await.unwrap_err();
    assert!(format!("{err:?}").contains("interrupted"))
}

#[tokio::test]
async fn test_memory_limit() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime.set_memory_limit(Some(1 << 20)).await; // 1MB

    let js_code = r#"
        export function alloc(x) {
            new Array(x).fill(0);
            return x;
        }
    "#;
    runtime
        .add_function(
            "alloc",
            DataType::Int32,
            js_code,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![100]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("alloc", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-------+
        | alloc |
        +-------+
        | 100   |
        +-------+"#]],
    );

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![1 << 20]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let err = runtime.call("alloc", &input).await.unwrap_err();
    assert!(format!("{err:?}").contains("out of memory"))
}

#[tokio::test]
async fn test_view_array() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime
        .add_function(
            "echo",
            DataType::Utf8View,
            r#"
export function echo(x) {
    return x + "!"
}
"#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8View, true)]);
    let arg0 = StringViewArray::from(vec!["hello", "world"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("echo", &input).await.unwrap();

    check(
        &[output],
        expect![[r#"
        +--------+
        | echo   |
        +--------+
        | hello! |
        | world! |
        +--------+"#]],
    );
}

#[tokio::test]
async fn test_batched_return_null_on_null_input() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime
        .add_function(
            "echo",
            DataType::Utf8View,
            r#"
export function echo(vals) {
    return vals.map(v => v + "!")
}
"#,
            FunctionOptions::default()
                .return_null_on_null_input()
                .batched(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8View, true)]);
    let arg0 = StringViewArray::from(vec![Some("hello"), None, Some("world")]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("echo", &input).await.unwrap();

    check(
        &[output],
        expect![[r#"
        +--------+
        | echo   |
        +--------+
        | hello! |
        |        |
        | world! |
        +--------+"#]],
    );
}

#[tokio::test]
async fn test_batched_called_on_null_input() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime
        .add_function(
            "echo",
            DataType::Utf8View,
            r#"
export function echo(vals) {
    return vals.map(v => v + "!")
}
"#,
            FunctionOptions::default().batched(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8View, true)]);
    let arg0 = StringViewArray::from(vec![Some("hello"), None, Some("world")]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("echo", &input).await.unwrap();

    check(
        &[output],
        expect![[r#"
        +--------+
        | echo   |
        +--------+
        | hello! |
        | null!  |
        | world! |
        +--------+"#]],
    );
}

#[tokio::test]
async fn test_async_echo() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime
        .add_function(
            "echo",
            DataType::Utf8View,
            r#"
export async function echo(x) {
    return x + "!"
}
"#,
            FunctionOptions::default()
                .return_null_on_null_input()
                .async_mode(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8View, true)]);
    let arg0 = StringViewArray::from(vec!["hello", "world"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("echo", &input).await.unwrap();

    check(
        &[output],
        expect![[r#"
        +--------+
        | echo   |
        +--------+
        | hello! |
        | world! |
        +--------+"#]],
    );
}

#[tokio::test]
async fn test_async_range() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            r#"
            export async function* range(n) {
                for (let i = 0; i < n; i++) {
                    yield i;
                }
            }
            "#,
            FunctionOptions::default()
                .return_null_on_null_input()
                .async_mode(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range", &input, 2).unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.next().await.unwrap().unwrap();
    let o2 = outputs.next().await.unwrap().unwrap();
    assert_eq!(o1.num_rows(), 2);
    assert_eq!(o2.num_rows(), 2);
    assert!(outputs.next().await.unwrap().is_none());

    check(
        &[o1, o2],
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

async fn delay_strlen(msg: String) -> usize {
    use tokio::time::*;
    sleep(Duration::from_millis(100)).await;
    msg.len()
}

#[tokio::test]
async fn test_async_rust_fn() {
    let mut runtime = Runtime::new().await.unwrap();

    async_with!(runtime.context() => |ctx| {
        let global = ctx.globals();
        global.set(
            "native_delay_strlen",
            Function::new(ctx.clone(), Async(delay_strlen))
                .unwrap()
                .with_name("native_delay_strlen")
                .unwrap(),
        )
        .unwrap();
    })
    .await;

    runtime
        .add_function(
            "delayStrlen",
            DataType::Int32,
            r#"
export async function delayStrlen(s) {
    return await native_delay_strlen(s);
}
"#,
            FunctionOptions::default()
                .return_null_on_null_input()
                .async_mode(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("s", DataType::Utf8View, true)]);
    let arg0 = StringViewArray::from(vec!["hello", "world"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("delayStrlen", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +-------------+
        | delayStrlen |
        +-------------+
        | 5           |
        | 5           |
        +-------------+"#]],
    );
}

/// assert Runtime is Send and Sync
#[tokio::test]
async fn test_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Runtime>();
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check_array(actual: &[ArrayRef], expect: Expect) {
    expect.assert_eq(&pretty_format_columns("array", actual).unwrap().to_string());
}

/// Returns a field with JSON type.
fn json_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with JSON type.
fn binary_json_field(name: &str) -> Field {
    Field::new(name, DataType::Binary, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with JSON type.
fn large_binary_json_field(name: &str) -> Field {
    Field::new(name, DataType::LargeBinary, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with decimal type.
fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
}
