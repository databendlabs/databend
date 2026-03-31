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

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_runtime::javascript::{FunctionOptions, Runtime};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut runtime = Runtime::new().await.unwrap();

    runtime
        .add_function(
            "gcd",
            DataType::Int32,
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
        .await
        .unwrap();

    runtime
        .add_function(
            "fib",
            DataType::Int32,
            r#"
            export function fib(x) {
                if (x <= 1)
                    return x;
                return fib(x - 1) + fib(x - 2);
            }
            "#,
            FunctionOptions::default().return_null_on_null_input(),
        )
        .await
        .unwrap();

    println!("call gcd");
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(15), None])),
            Arc::new(Int32Array::from(vec![25, 2])),
        ],
    )
    .unwrap();

    let output = runtime.call("gcd", &input).await.unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("call fib");
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![10]))],
    )
    .unwrap();

    let output = runtime.call("fib", &input).await.unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
}
