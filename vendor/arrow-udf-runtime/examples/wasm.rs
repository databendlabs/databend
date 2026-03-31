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

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Fields, Schema};
use arrow_udf_runtime::wasm::Runtime;

fn main() {
    let filename = std::env::args().nth(1).expect("no filename");
    let runtime = Runtime::new(&std::fs::read(filename).unwrap()).unwrap();

    println!("{runtime:#?}");

    println!("\ncall gcd");

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

    let gcd = runtime
        .find_function(
            "gcd",
            vec![DataType::Int32, DataType::Int32],
            DataType::Int32,
        )
        .unwrap();
    let output = runtime.call(&gcd, &input).unwrap();
    print(&input, &output);

    println!("\ncall range");

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
    )
    .unwrap();

    let range = runtime
        .find_table_function("range", vec![DataType::Int32], DataType::Int32)
        .unwrap();
    let iter = runtime.call_table_function(&range, &input).unwrap();
    for output in iter {
        let output = output.unwrap();
        arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
    }
}

/// Concatenate two record batches and print them.
fn print(input: &RecordBatch, output: &RecordBatch) {
    let schema = Arc::new(Schema::new(
        (input.schema().fields().into_iter())
            .chain(output.schema().fields())
            .cloned()
            .collect::<Fields>(),
    ));
    let columns = (input.columns().iter())
        .chain(output.columns())
        .cloned()
        .collect();
    let merged = RecordBatch::try_new(schema, columns).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&merged)).unwrap();
}
