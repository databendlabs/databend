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

use databend_common_arrow::arrow::compute::merge_sort::MergeSlice;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockRowIndex;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;

pub fn new_block(columns: &[Column]) -> DataBlock {
    let len = columns.first().map_or(1, |c| c.len());
    let columns = columns
        .iter()
        .map(|col| BlockEntry::new(col.data_type(), Value::Column(col.clone())))
        .collect();

    DataBlock::new(columns, len)
}

pub fn run_filter(file: &mut impl Write, predicate: Vec<bool>, block: &DataBlock) {
    let result = block.clone().filter_with_bitmap(&predicate.clone().into());

    match result {
        Ok(result_block) => {
            writeln!(file, "Filter:         {predicate:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

pub fn run_concat(file: &mut impl Write, blocks: &[DataBlock]) {
    let result = DataBlock::concat(blocks);

    match result {
        Ok(result_block) => {
            for (i, c) in blocks.iter().enumerate() {
                writeln!(file, "Concat-Column {i}:").unwrap();
                writeln!(file, "{c:?}").unwrap();
            }
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

pub fn run_take_block(file: &mut impl Write, indices: &[BlockRowIndex], blocks: &[DataBlock]) {
    let result = DataBlock::take_blocks(blocks, indices, 0);
    writeln!(file, "Take Block indices:         {indices:?}").unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

pub fn run_take_block_by_slices_with_limit(
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

pub fn run_take_by_slice_limit(
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

pub fn run_scatter(file: &mut impl Write, block: &DataBlock, indices: &[u32], scatter_size: usize) {
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

pub fn run_take(file: &mut impl Write, indices: &[u32], block: &DataBlock) {
    let result = DataBlock::take(block, indices, &mut None);

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
