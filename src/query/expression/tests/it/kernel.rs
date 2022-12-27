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

use common_arrow::arrow::compute::merge_sort::MergeSlice;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::BlockEntry;
use common_expression::BlockRowIndex;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Value;
use goldenfile::Mint;

use crate::common::new_block;

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
            &new_block(&[
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
        new_block(&[
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
        new_block(&[
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
        &new_block(&[
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
        let mut blocks = Vec::with_capacity(3);
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
            columns.push(BlockEntry {
                data_type: DataType::Number(NumberDataType::UInt8),
                value: Value::Column(Column::from_data(vec![(i + 10) as u8; 4])),
            });
            columns.push(BlockEntry {
                data_type: DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                value: Value::Column(Column::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            });
            blocks.push(DataBlock::new(columns, 4))
        }

        run_take_block(&mut file, &indices, &blocks);
    }

    {
        let mut blocks = Vec::with_capacity(3);
        let indices = vec![
            (0, 0, 2),
            (1, 0, 3),
            (2, 0, 1),
            (2, 1, 1),
            (0, 2, 1),
            (2, 2, 1),
            (0, 3, 1),
            (2, 3, 1),
        ];
        for i in 0..3 {
            let mut columns = Vec::with_capacity(3);
            columns.push(BlockEntry {
                data_type: DataType::Number(NumberDataType::UInt8),
                value: Value::Column(Column::from_data(vec![(i + 10) as u8; 4])),
            });
            columns.push(BlockEntry {
                data_type: DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                value: Value::Column(Column::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            });
            blocks.push(DataBlock::new(columns, 4))
        }

        run_take_block_by_slices_with_limit(&mut file, &indices, &blocks, None);
        run_take_block_by_slices_with_limit(&mut file, &indices, &blocks, Some(4));
    }

    run_take_by_slice_limit(
        &mut file,
        &new_block(&[
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
        (2, 3),
        None,
    );

    run_take_by_slice_limit(
        &mut file,
        &new_block(&[
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
        (2, 3),
        Some(2),
    );

    run_scatter(
        &mut file,
        &new_block(&[
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

fn run_filter(file: &mut impl Write, predicate: Column, block: &DataBlock) {
    let predicate = Value::Column(predicate);
    let result = block.clone().filter(&predicate);

    match result {
        Ok(result_block) => {
            writeln!(file, "Filter:         {predicate}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_concat(file: &mut impl Write, blocks: &[DataBlock]) {
    let result = DataBlock::concat(blocks);

    match result {
        Ok(result_block) => {
            for (i, c) in blocks.iter().enumerate() {
                writeln!(file, "Concat-Column {}:", i).unwrap();
                writeln!(file, "{:?}", c).unwrap();
            }
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_take(file: &mut impl Write, indices: &[u32], block: &DataBlock) {
    let result = DataBlock::take(block, indices);

    match result {
        Ok(result_block) => {
            writeln!(file, "Take:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_take_block(file: &mut impl Write, indices: &[BlockRowIndex], blocks: &[DataBlock]) {
    let result = DataBlock::take_blocks(blocks, indices);
    writeln!(file, "Take Block indices:         {indices:?}").unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_take_block_by_slices_with_limit(
    file: &mut impl Write,
    slices: &[MergeSlice],
    blocks: &[DataBlock],
    limit: Option<usize>,
) {
    let result = DataBlock::take_by_slices_limit_from_blocks(blocks, slices, limit);
    writeln!(
        file,
        "Take Block by slices (limit: {limit:?}):       {slices:?}"
    )
    .unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_take_by_slice_limit(
    file: &mut impl Write,
    block: &DataBlock,
    slice: (usize, usize),
    limit: Option<usize>,
) {
    let result = DataBlock::take_by_slice_limit(block, slice, limit);
    writeln!(file, "Take Block by slice (limit: {limit:?}): {slice:?}").unwrap();
    writeln!(file, "Block:\n{block}").unwrap();
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_scatter(file: &mut impl Write, block: &DataBlock, indices: &[u32], scatter_size: usize) {
    let result = DataBlock::scatter(block, indices, scatter_size);

    match result {
        Ok(result_block) => {
            writeln!(file, "Scatter:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();

            for (i, c) in result_block.iter().enumerate() {
                writeln!(file, "Result-{i}:\n{c}").unwrap();
            }
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}
