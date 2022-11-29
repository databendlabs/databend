// Copyright 2022 Datafuse Labs.
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

use std::io::Write;

use common_datavalues::ChunkRowIndex;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::Chunk;
use common_expression::ChunkEntry;
use common_expression::Column;
use common_expression::Value;
use goldenfile::Mint;

use crate::common::new_chunk;

#[test]
pub fn test_pass() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("kernel-pass.txt").unwrap();

    for filter in vec![
        Column::from_data(vec![true, false, false, false, true]),
        Column::from_data_with_validity(vec![true, true, false, true, true], vec![
            false, true, true, false, false,
        ]),
        Column::from_data(vec!["a", "b", "", "", "c"]),
        Column::from_data(vec![0, 1, 2, 3, 0]),
    ] {
        run_filter(
            &mut file,
            filter,
            &new_chunk(&[
                (
                    DataType::Number(NumberDataType::Int32),
                    Column::from_data(vec![0i32, 1, 2, 3, -4]),
                ),
                (
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                    Column::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                        false, true, false, false, false,
                    ]),
                ),
                (DataType::Null, Column::Null { len: 5 }),
                (
                    DataType::Nullable(Box::new(DataType::String)),
                    Column::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                        false, true, true, false, false,
                    ]),
                ),
            ]),
        );
    }

    run_concat(&mut file, &[
        new_chunk(&[
            (
                DataType::Number(NumberDataType::Int32),
                Column::from_data(vec![0i32, 1, 2, 3, -4]),
            ),
            (
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                Column::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                    false, true, false, false, false,
                ]),
            ),
            (DataType::Null, Column::Null { len: 5 }),
            (DataType::EmptyArray, Column::EmptyArray { len: 5 }),
            (
                DataType::Nullable(Box::new(DataType::String)),
                Column::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                    false, true, true, false, false,
                ]),
            ),
        ]),
        new_chunk(&[
            (
                DataType::Number(NumberDataType::Int32),
                Column::from_data(vec![5i32, 6]),
            ),
            (
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                Column::from_data_with_validity(vec![15u8, 16], vec![false, true]),
            ),
            (DataType::Null, Column::Null { len: 2 }),
            (DataType::EmptyArray, Column::EmptyArray { len: 2 }),
            (
                DataType::Nullable(Box::new(DataType::String)),
                Column::from_data_with_validity(vec!["x", "y"], vec![false, true]),
            ),
        ]),
    ]);

    run_take(
        &mut file,
        &[0, 3, 1],
        &new_chunk(&[
            (
                DataType::Number(NumberDataType::Int32),
                Column::from_data(vec![0i32, 1, 2, 3, -4]),
            ),
            (
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                Column::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                    false, true, false, false, false,
                ]),
            ),
            (DataType::Null, Column::Null { len: 5 }),
            (
                DataType::Nullable(Box::new(DataType::String)),
                Column::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                    false, true, true, false, false,
                ]),
            ),
        ]),
    );

    {
        let mut chunks = Vec::with_capacity(3);
        let indices = vec![
            (0, 0, 1),
            (1, 0, 1),
            (2, 0, 1),
            (0, 1, 1),
            (1, 1, 1),
            (2, 1, 1),
            (0, 2, 1),
            (1, 2, 1),
            (2, 2, 1),
            (0, 3, 1),
            (1, 3, 1),
            (2, 3, 1),
            // repeat 3
            (0, 0, 3),
        ];
        for i in 0..3 {
            let mut columns = Vec::with_capacity(3);
            columns.push(ChunkEntry {
                id: i,
                data_type: DataType::Number(NumberDataType::UInt8),
                value: Value::Column(Column::from_data(vec![(i + 10) as u8; 4])),
            });
            columns.push(ChunkEntry {
                id: i,
                data_type: DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                value: Value::Column(Column::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            });
            chunks.push(Chunk::new(columns, 4))
        }

        run_take_chunk(&mut file, &indices, &chunks);
    }

    run_scatter(
        &mut file,
        &new_chunk(&[
            (
                DataType::Number(NumberDataType::Int32),
                Column::from_data(vec![0i32, 1, 2, 3, -4]),
            ),
            (
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                Column::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                    false, true, false, false, false,
                ]),
            ),
            (DataType::Null, Column::Null { len: 5 }),
            (
                DataType::Nullable(Box::new(DataType::String)),
                Column::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                    false, true, true, false, false,
                ]),
            ),
        ]),
        &[0, 0, 1, 2, 1],
        3,
    );
}

fn run_filter(file: &mut impl Write, predicate: Column, chunk: &Chunk) {
    let predicate = Value::Column(predicate);
    let result = chunk.clone().filter(&predicate);

    match result {
        Ok(result_chunk) => {
            writeln!(file, "Filter:         {predicate}").unwrap();
            writeln!(file, "Source:\n{chunk}").unwrap();
            writeln!(file, "Result:\n{result_chunk}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_concat(file: &mut impl Write, chunks: &[Chunk]) {
    let result = Chunk::concat(chunks);

    match result {
        Ok(result_chunk) => {
            for (i, c) in chunks.iter().enumerate() {
                writeln!(file, "Concat-Column {}:", i).unwrap();
                writeln!(file, "{:?}", c).unwrap();
            }
            writeln!(file, "Result:\n{result_chunk}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_take(file: &mut impl Write, indices: &[u32], chunk: &Chunk) {
    let result = Chunk::take(chunk.clone(), indices);

    match result {
        Ok(result_chunk) => {
            writeln!(file, "Take:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{chunk}").unwrap();
            writeln!(file, "Result:\n{result_chunk}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_take_chunk(file: &mut impl Write, indices: &[ChunkRowIndex], chunks: &[Chunk]) {
    let result = Chunk::take_chunks(chunks, indices);
    writeln!(file, "Take Chunk indices:         {indices:?}").unwrap();
    for (i, chunk) in chunks.iter().enumerate() {
        writeln!(file, "Chunk{i}:\n{chunk}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_scatter(file: &mut impl Write, chunk: &Chunk, indices: &[u32], scatter_size: usize) {
    let result = Chunk::scatter(chunk, indices, scatter_size);

    match result {
        Ok(result_chunk) => {
            writeln!(file, "Scatter:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{chunk}").unwrap();

            for (i, c) in result_chunk.iter().enumerate() {
                writeln!(file, "Result-{i}:\n{c}").unwrap();
            }
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}
