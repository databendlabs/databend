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

#![cfg(feature = "python-runtime")]

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_runtime::python::Runtime;
use arrow_udf_runtime::CallMode;

fn main() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "gcd",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#,
        )
        .unwrap();

    runtime
        .add_function(
            "range1",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def range1(n: int):
    for i in range(n):
        yield i
"#,
        )
        .unwrap();

    println!("\ncall gcd");

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

    let output = runtime.call("gcd", &input).unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("\ncall range");

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range1", &input, 2).unwrap();
    let o1 = outputs.next().unwrap().unwrap();
    let o2 = outputs.next().unwrap().unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(&[o1, o2]).unwrap();
}
